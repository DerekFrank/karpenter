/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package disruption

import (
	"context"

	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
)

// Terminate-First Disruption (RFC kubernetes-sigs/karpenter#3203). A voluntary disruption normally replace-firsts:
// launch the replacement, then remove the original. A fleet with no room to grow — a reserved/ODCR pool whose
// reservation is full, or a static NodePool at its replica count — can't stage a replacement first, so it must
// terminate first and let reactive provisioning refill the freed slot.
//
// This is a shared primitive, not a per-method bolt-on: any voluntary disruption method decides it the same way, from
// two scheduling simulations of the candidate-gone fleet. Callers gate it on the TerminateFirst feature flag.
//
// The decision is made by simulating twice (see shouldTerminateFirst):
//
//  1. REPLACE-FIRST feasibility, in the usual fallback ReservedOfferingMode. In fallback mode a full reservation
//     (Available=true, ReservationCapacity=0) does not "satisfy" a pod: instead of creating a reserved-only NodeClaim
//     against the full reservation, the reserved NodePool fails with a plain (non-ReservedOffering) error so the
//     scheduler falls through to any lower-weight fallback (e.g. an on-demand NodePool) or a different reservation that
//     still has capacity. (A ReservedOfferingError, i.e. strict mode, would instead poison cross-NodePool fallthrough
//     and defer the pod — which is why pass 1 must be fallback, not strict.) If every reschedulable pod places, we
//     replace-first as usual: a drifted reserved node prefers moving its pods onto an on-demand NodePool over
//     terminating first.
//
//  2. TERMINATE-FIRST feasibility. Only if pass 1 can't place every pod AND the candidate itself holds a reservation:
//     simulate again in strict mode with the candidate's own reservation slot credited back (CreditReservationCapacity),
//     modeling the slot the candidate will free on termination. If every pod then places, terminate-first — deleting
//     the candidate is exactly what unblocks the reschedule. Strict mode ensures that if more pods need the reservation
//     than the freed slot(s) can hold, the surplus fails and we do NOT terminate-first. If the candidate's reservation
//     is unavailable for a reason other than being full (expiring, ICE'd, incompatible), crediting a slot back does not
//     make it schedulable, so pass 2 also fails and we correctly stay blocked — freeing the slot wouldn't help.
//
// A launch *failure* never selects terminate-first: this reads structural capacity from the simulations, never launch
// outcomes, so a failed launch keeps replacing-first under the existing per-NodePool launch backoff.
//
// This relies on the cloud provider modeling a full-but-otherwise-healthy reservation as an offering with
// Available=true and ReservationCapacity=0 (capacity and usability are independent axes): "out of capacity, nothing
// else wrong". See the karpenter-provider-aws offering model.

// shouldTerminateFirst runs the two-pass simulation for a voluntary disruption of a single candidate. It returns:
//   - (true, pass-2 results, nil)  — terminate-first: delete the candidate and let reactive provisioning refill.
//   - (false, pass-1 results, nil) — NOT terminate-first: the caller replace-firsts with these results if every pod
//     scheduled, otherwise treats them as a Blocked event (the pods can't be rescheduled at all).
//
// See the package doc above for the full rationale. The returned error is non-nil only on a simulation error (e.g. the
// candidate began deleting), which the caller should treat as retry.
func shouldTerminateFirst(
	ctx context.Context,
	kubeClient client.Client,
	cluster *state.Cluster,
	provisioner *provisioning.Provisioner,
	clk clock.Clock,
	recorder events.Recorder,
	candidate *Candidate,
) (bool, pscheduling.Results, error) {
	// Pass 1: replace-first feasibility, in the usual fallback mode. In fallback mode a full reservation doesn't satisfy
	// a pod, so the scheduler falls through to a fallback NodePool / another reservation with capacity.
	results, err := SimulateScheduling(ctx, kubeClient, cluster, provisioner, clk, recorder, nil, candidate)
	if err != nil {
		return false, results, err
	}
	if results.AllNonPendingPodsScheduled() {
		return false, results, nil // replace-first
	}

	// Pass 1 couldn't place every pod without reusing the candidate's own reservation. Only a reserved candidate can
	// terminate-first — its freed slot is the headroom that unblocks the reschedule. Otherwise return the pass-1
	// results so the caller emits a Blocked event.
	reservationID := candidate.Labels()[cloudprovider.ReservationIDLabel]
	if candidate.capacityType != v1.CapacityTypeReserved || reservationID == "" {
		return false, results, nil
	}

	// Pass 2: terminate-first feasibility. Credit the candidate's reservation slot back and require every pod to place
	// under strict mode (surplus pods that wouldn't fit the freed slot must fail rather than fall back).
	tfResults, err := SimulateScheduling(ctx, kubeClient, cluster, provisioner, clk, recorder,
		[]pscheduling.Options{pscheduling.DisableReservedCapacityFallback, pscheduling.CreditReservationCapacity(reservationID, 1)}, candidate)
	if err != nil {
		return false, results, err
	}
	if tfResults.AllNonPendingPodsScheduled() {
		return true, tfResults, nil
	}

	// Neither replace-first nor terminate-first works. Return the pass-1 results so the Blocked event reports the
	// replace-first pod errors (the reschedule we attempted first).
	return false, results, nil
}
