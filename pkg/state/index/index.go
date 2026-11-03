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

package index

import (
	"context"
	"errors"
	"fmt"

	"github.com/samber/lo"
	"go.uber.org/multierr"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

// Setup registers every index with the indexer. Indexes on Karpenter CRDs fail open when the CRD isn't installed, so
// controllers that don't rely on those CRDs keep working.
func Setup(ctx context.Context, indexer client.FieldIndexer) {
	lo.Must0(PodNodeName(ctx, indexer), "failed to setup pod indexer")
	lo.Must0(NodeProviderID(ctx, indexer), "failed to setup node provider id indexer")
	lo.Must0(VolumeAttachmentNodeName(ctx, indexer), "failed to setup volumeattachment indexer")

	handleCRDIndexerError := func(err error, msg string) {
		noKindMatchError := &meta.NoKindMatchError{}
		if errors.As(err, &noKindMatchError) {
			log.FromContext(ctx).Error(err, msg)
		} else if err != nil {
			panic(fmt.Sprintf("%s, %s", err, msg))
		}
	}
	handleCRDIndexerError(NodeClaimProviderID(ctx, indexer), "failed to setup nodeclaim provider id indexer")
	handleCRDIndexerError(NodeClaimNodeClassRef(ctx, indexer), "failed to setup nodeclaim nodeclassref indexer")
	handleCRDIndexerError(NodePoolNodeClassRef(ctx, indexer), "failed to setup nodepool nodeclassref indexer")
}

// PodNodeName indexes Pods by spec.nodeName.
func PodNodeName(ctx context.Context, indexer client.FieldIndexer) error {
	return indexer.IndexField(ctx, &corev1.Pod{}, "spec.nodeName", func(o client.Object) []string {
		return []string{o.(*corev1.Pod).Spec.NodeName}
	})
}

// NodeProviderID indexes Nodes by spec.providerID.
func NodeProviderID(ctx context.Context, indexer client.FieldIndexer) error {
	return indexer.IndexField(ctx, &corev1.Node{}, "spec.providerID", func(o client.Object) []string {
		return []string{o.(*corev1.Node).Spec.ProviderID}
	})
}

// VolumeAttachmentNodeName indexes VolumeAttachments by spec.nodeName.
func VolumeAttachmentNodeName(ctx context.Context, indexer client.FieldIndexer) error {
	return indexer.IndexField(ctx, &storagev1.VolumeAttachment{}, "spec.nodeName", func(o client.Object) []string {
		return []string{o.(*storagev1.VolumeAttachment).Spec.NodeName}
	})
}

// NodeClaimProviderID indexes NodeClaims by status.providerID.
func NodeClaimProviderID(ctx context.Context, indexer client.FieldIndexer) error {
	return indexer.IndexField(ctx, &v1.NodeClaim{}, "status.providerID", func(o client.Object) []string {
		return []string{o.(*v1.NodeClaim).Status.ProviderID}
	})
}

// NodeClaimNodeClassRef indexes NodeClaims by spec.nodeClassRef.group, spec.nodeClassRef.kind and
// spec.nodeClassRef.name.
func NodeClaimNodeClassRef(ctx context.Context, indexer client.FieldIndexer) error {
	return multierr.Combine(
		indexer.IndexField(ctx, &v1.NodeClaim{}, "spec.nodeClassRef.group", func(o client.Object) []string {
			return []string{o.(*v1.NodeClaim).Spec.NodeClassRef.Group}
		}),
		indexer.IndexField(ctx, &v1.NodeClaim{}, "spec.nodeClassRef.kind", func(o client.Object) []string {
			return []string{o.(*v1.NodeClaim).Spec.NodeClassRef.Kind}
		}),
		indexer.IndexField(ctx, &v1.NodeClaim{}, "spec.nodeClassRef.name", func(o client.Object) []string {
			return []string{o.(*v1.NodeClaim).Spec.NodeClassRef.Name}
		}),
	)
}

// NodePoolNodeClassRef indexes NodePools by spec.template.spec.nodeClassRef.group, spec.template.spec.nodeClassRef.kind
// and spec.template.spec.nodeClassRef.name.
func NodePoolNodeClassRef(ctx context.Context, indexer client.FieldIndexer) error {
	return multierr.Combine(
		indexer.IndexField(ctx, &v1.NodePool{}, "spec.template.spec.nodeClassRef.group", func(o client.Object) []string {
			return []string{o.(*v1.NodePool).Spec.Template.Spec.NodeClassRef.Group}
		}),
		indexer.IndexField(ctx, &v1.NodePool{}, "spec.template.spec.nodeClassRef.kind", func(o client.Object) []string {
			return []string{o.(*v1.NodePool).Spec.Template.Spec.NodeClassRef.Kind}
		}),
		indexer.IndexField(ctx, &v1.NodePool{}, "spec.template.spec.nodeClassRef.name", func(o client.Object) []string {
			return []string{o.(*v1.NodePool).Spec.Template.Spec.NodeClassRef.Name}
		}),
	)
}
