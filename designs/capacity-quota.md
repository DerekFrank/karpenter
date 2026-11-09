# CapacityQuota: Granular Resource Limits

Support the SIG Autoscaling `CapacityQuota` API, so that resource limits can apply to any set of nodes selected by labels, not only to a NodePool.

## Motivation

Karpenter limits capacity with `NodePool.spec.limits`. A limit applies to the nodes of one NodePool, and there is no way to limit:

- the whole cluster ([AWS best practices](https://docs.aws.amazon.com/eks/latest/best-practices/karpenter.html) recommend limits per NodePool for this reason),
- a set of nodes that spans NodePools, such as the nodes of one team, one machine family, one zone or one OS,
- a scarce resource such as GPUs across all NodePools that can launch them.

SIG Autoscaling designed one API for this for both node autoscalers: [`CapacityQuota`](https://github.com/kubernetes/autoscaler/blob/master/cluster-autoscaler/proposals/granular-resource-limits.md) (`autoscaling.x-k8s.io/v1beta1`). Cluster Autoscaler implements it. The proposal's GA criteria wait for a Karpenter implementation, so that users get the same limits on either autoscaler. This RFC proposes that implementation ([#2698](https://github.com/kubernetes-sigs/karpenter/issues/2698)).

NodePool limits also have a gap of their own today, which this design closes for both kinds of limit: they're exact within one scheduling round and optimistic between deciders. A NodeClaim that hasn't launched counts nothing against its NodePool, so a provisioning round and a disruption command, or two provisioning rounds, can each plan up to the limit and together exceed it.

### Use Cases

1. **Cluster-wide cost cap.** An administrator caps the cluster at 512 vCPU across all NodePools, and individual NodePools can still have their own limits.
2. **Team quotas across NodePools.** Team A runs a spot NodePool and an on-demand NodePool. Their nodes carry `team: a`, and a quota of 64 vCPU applies to both NodePools together.
3. **Scarce or expensive hardware.** No more than 16 `nvidia.com/gpu` across every NodePool that can launch GPU instances.
4. **Zone or IP space protection.** No more than 64 nodes in `us-east-1a`, regardless of NodePool.

### Non-Goals

- Changing what `NodePool.spec.limits` mean. They keep their meaning, and become exact across deciders.
- Minimum limits. The upstream proposal dropped them in favour of the CapacityBuffer API.
- Limits that hold during disruption. As upstream decided, quotas are soft in the same way NodePool limits are: a node being disrupted stops counting once it is marked, so its replacement can launch before it goes.
- Defining the `CapacityQuota` API. Karpenter consumes the upstream CRD as is.

## Proposal

### Proposed Spec

Karpenter reads the upstream object. No Karpenter API changes are needed:

```yaml
apiVersion: autoscaling.x-k8s.io/v1beta1
kind: CapacityQuota
metadata:
  name: team-a
spec:
  selector:            # optional; empty or missing selects every node
    matchLabels:
      team: a
  limits:
    resources:
      cpu: "64"
      memory: 256Gi
      nodes: "20"
```

A launch is allowed only if it fits every limit that applies to it: its NodePool's `spec.limits` and every CapacityQuota whose selector may match the node it would create.

### How It Works

Karpenter keeps three kinds of state (see `pkg/state/AGENTS.md`). What counts against a limit is a derived field, kept in a store and updated on every change. A decision to launch a node is intent: it takes a while to come true, and other decisions must count it meanwhile. Karpenter keeps it as a **claim** in the same store as the counts, so that admission and the handoff to the launched node happen under one lock.

**What counts.** A quota counts, for every node its selector matches, the node's capacity and one `nodes`:

- a NodeClaim, from the moment it exists, as one `nodes` against every quota it may match (its instance type and zone may still be open), and, once it launches, as its capacity against the quotas that select its labels;
- a Node Karpenter doesn't manage, as its capacity against the quotas that select it.

A NodeClaim stops counting once it's deleting or its `DisruptionReason` condition is true, as for NodePool limits.

**Which quotas a NodeClaim may match.** Before launch, Karpenter knows the NodeClaim's requirements: its NodePool's template, its pods' requirements, and each instance type option's requirements. A quota may match a NodeClaim if its selector is compatible with the requirements of any option. A well known label the requirements leave open, such as the zone, may take any value, so a quota on it may match. A custom label the requirements don't define won't be on the node, so a quota on it doesn't.

**Admission.** Before Karpenter creates a NodeClaim, it reserves the NodeClaim's place:

1. against its NodePool's limits, holding the largest capacity among its instance type options and one `nodes`;
2. against every quota it may match, holding the same, one claim per quota, reserved one at a time in sorted quota name order;
3. if a quota refuses, it releases every claim it already holds, the NodePool's and the quotas', and the NodeClaim isn't created. Its pods wait for the next round.

Each claim is released when the NodeClaim launches and counts its real capacity, when the NodeClaim is deleted first, when its creator drops it, or after two minutes.

**Planning.** The scheduler starts each quota's remaining capacity at its limits less what claims hold, and spends from it as it does for NodePool limits: each existing node the quota selects spends its capacity, and each new NodeClaim spends the largest capacity among the options that may match the quota. It drops the instance types that would exceed a quota they may match, given the pod's requirements, so a NodeClaim's options, and so what it holds, fit every quota. The claim is the admission; the plan only avoids refusals.

**Disruption.** A command reserves its replacements before it taints its candidates, so a replacement that doesn't fit fails the command with nothing to unwind. The candidates still count until they're marked, and stop counting before the replacements launch, so they are credited: a replacement admitted in place of its candidate fits.

**Static capacity.** Static NodeClaims count against quotas and reserve against them like any other. Their NodePool's node limit stays with the static node count reservations.

### Consistency

- **No quota is exceeded by Karpenter's own decisions.** Admission is atomic within each store, and every NodeClaim counts from the moment its claim is admitted until it stops counting.
- **Holds are pessimistic until launch.** A NodeClaim whose options range from 2 to 8 CPU holds 8 against every quota it may match, until it launches. Near a limit, that can delay a second NodeClaim until the first launches.
- **Each limit admits under its own key**, and a NodeClaim spans several: its NodePool's, and one per quota it may match. Karpenter reserves them one at a time, the NodePool first, then the quotas in sorted name order, and releases everything it holds when one refuses. A decider can be refused by a claim that is about to be released, and retries next round. The fixed order keeps two deciders from each holding what the other needs.
- **Quota changes recount.** Each quota is counted by its own Derives from the NodeClaim and Node stores, with its selector captured when they're built. A change to a quota's spec builds new Derives, which count every NodeClaim and Node before the quota is enforced, then removes the old ones, so a quota is never enforced against a partial count. The claims held against the old spec are released. Karpenter admits nothing until it has synced the quotas once.
- **Nodes outside Karpenter.** Nodes Karpenter doesn't manage count from the moment cluster state sees them. A node another autoscaler is about to create doesn't count until it exists, so quotas shared with another autoscaler stay soft.

### User Experience

**A quota blocks provisioning.** The pod gets the usual `FailedScheduling` event, naming the quota:

```
all available instance types exceed limits for capacityquota (CapacityQuotas=team-a, NodePool=default)
```

**A quota restricts provisioning to some instance types.** The scheduler drops the instance types that wouldn't fit, and the NodeClaim launches with the rest. A quota on a zone or capacity type applies to every NodeClaim that may launch there, so it blocks NodeClaims whose zone is open, while pods that require another zone still schedule. Narrowing a NodeClaim's offerings to the ones that fit, instead of dropping the instance type, is future work.

### Detailed Design

| Component | Where | Responsibility |
|---|---|---|
| API types | `pkg/apis/autoscaling/v1beta1/capacityquota.go` | A copy of the upstream types, as for CapacityBuffer, because the upstream module needs newer `k8s.io` modules. |
| Quota store | `pkg/cluster/capacityquota.go` | `CapacityQuotaState`: a CapacityQuota object store following the informer, and a derived store keyed by quota name with what counts against each quota, fed by each quota's Derives from the NodeClaim and Node stores, and the claims held against it. A quota whose spec changes is rebuilt. |
| Admission | `pkg/controllers/provisioning/limits.go` | `Provisioner.ReserveLimits`: reserves against the NodePool, then each quota in name order, and releases what it holds on a refusal. |
| Planning | `pkg/controllers/provisioning/scheduling/capacityquota.go` | Drops instance types that would exceed a quota, and spends from each quota as NodeClaims are planned. |

**Feature gate.** `CapacityQuota`, alpha and off by default. With it off, quotas aren't read or enforced, and NodePool limits behave as above.

**RBAC.** `get`, `list` and `watch` on `capacityquotas.autoscaling.x-k8s.io`.

### Edge Cases

- **Labels unknown at launch.** A quota selecting a label that only appears after registration counts the node once it has the label, but can't stop the launch that first crosses it. Select on NodePool template labels or well known labels.
- **Overlapping quotas.** A NodeClaim may be selected by several quotas; it must fit every one.
- **Quota lower than current usage.** Nothing is deleted; nothing the quota may select launches until usage drops below the limit.
- **Invalid selector.** Karpenter logs an error and doesn't enforce the quota.
- **Empty selector.** It selects every node, so it acts as a cluster-wide limit (Use Case 1).

## Alternatives Considered

### One key for every quota

Keep every quota's fields under one key, so a NodeClaim is admitted against all of them under one lock and there is nothing to release. Rejected: every NodeClaim change in the cluster would take that one lock and copy every quota's fields, and claims would need a multi-key value. Reserving each key in a fixed order and releasing on a refusal keeps the limits exact at the cost of an occasional extra round.

### Enforce quotas only after the cloud provider picks an instance

Admit a NodeClaim, then fail it if the launched instance breaks a quota. Simpler, but it wastes launches and leaves pods pending longer.

### Map quotas onto NodePool limits

Translate each quota into limits on the NodePools it could select. This fails for quotas that cross NodePools, where the capacity has to be shared rather than split.

## Graduation Criteria

- **Alpha:** feature gate off by default; quotas enforced during provisioning, disruption and static capacity.
- **Beta:** narrowing offerings instead of dropping instance types; `status.used`; metrics and events on the quota; e2e tests for each use case; agreed ownership of `status.used` with Cluster Autoscaler.
- **GA:** follows the upstream API reaching v1.

## Open Questions

1. Who owns `status.used` when both autoscalers run in a cluster?
2. Should Karpenter install the `CapacityQuota` CRD in its Helm chart, or require users to install it from SIG Autoscaling?
3. Should the claims against a quota whose spec changes carry over to the new spec, rather than being released? Until then a NodeClaim in flight across a quota change counts only as a node against the new spec until it launches.
