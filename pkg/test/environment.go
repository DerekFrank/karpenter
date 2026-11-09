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

package test

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/awslabs/operatorpkg/option"
	"github.com/samber/lo"
	"go.uber.org/multierr"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	autoscalingv1beta1 "sigs.k8s.io/karpenter/pkg/apis/autoscaling/v1beta1"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/state/overlay"
	"sigs.k8s.io/karpenter/pkg/utils/env"
)

type Environment struct {
	envtest.Environment

	// Client is the suite's client, the third party: it reads through the cache and the overlay, writes to the API
	// server directly, and waits until the cache and every store following the overlay have seen each write.
	Client client.Client
	// APIServer reads the API server without the cache. The overlay records only the writes of the kinds cluster state
	// follows, so a controller's write to a kind it doesn't, such as a NodeOverlay, a node class or a
	// PersistentVolumeClaim, reaches a suite's cached reads only through the informer, which lags the write: read one
	// the controller under test wrote back from APIServer.
	APIServer client.Client
	// Overlay is the controller under test's client, as the manager's is in production: its reads include its own
	// writes as soon as they return, and every store following it sees each write before the write returns.
	Overlay             *overlay.Client
	Clock               *clocktesting.FakeClock
	KubernetesInterface kubernetes.Interface
	Version             *version.Version
	Done                chan struct{}
	Cancel              context.CancelFunc
}

type EnvironmentOptions struct {
	crds          []*apiextensionsv1.CustomResourceDefinition
	fieldIndexers []func(cache.Cache) error
	configOptions []func(*rest.Config)
}

// WithCRDs registers the specified CRDs to the apiserver for use in testing
func WithCRDs(crds ...*apiextensionsv1.CustomResourceDefinition) option.Function[EnvironmentOptions] {
	return func(o *EnvironmentOptions) {
		o.crds = append(o.crds, crds...)
	}
}

// WithFieldIndexers expects a function that indexes fields against the cache such as cache.IndexField(...). Each index
// is registered with the overlay too, so a List with a field selector on it includes the overlay's writes.
func WithFieldIndexers(fieldIndexers ...func(cache.Cache) error) option.Function[EnvironmentOptions] {
	return func(o *EnvironmentOptions) {
		o.fieldIndexers = append(o.fieldIndexers, fieldIndexers...)
	}
}

// WithConfigOptions allows customization of the rest.Config before client creation
func WithConfigOptions(options ...func(*rest.Config)) option.Function[EnvironmentOptions] {
	return func(o *EnvironmentOptions) {
		o.configOptions = append(o.configOptions, options...)
	}
}

func NodeProviderIDFieldIndexer(ctx context.Context) func(cache.Cache) error {
	return func(c cache.Cache) error {
		return c.IndexField(ctx, &corev1.Node{}, "spec.providerID", func(obj client.Object) []string {
			return []string{obj.(*corev1.Node).Spec.ProviderID}
		})
	}
}

func NodeClaimProviderIDFieldIndexer(ctx context.Context) func(cache.Cache) error {
	return func(c cache.Cache) error {
		return c.IndexField(ctx, &v1.NodeClaim{}, "status.providerID", func(obj client.Object) []string {
			return []string{obj.(*v1.NodeClaim).Status.ProviderID}
		})
	}
}

func NodeClaimNodeClassRefFieldIndexer(ctx context.Context) func(cache.Cache) error {
	return func(c cache.Cache) error {
		var err error
		err = multierr.Append(err, c.IndexField(ctx, &v1.NodeClaim{}, "spec.nodeClassRef.group", func(obj client.Object) []string {
			return []string{obj.(*v1.NodeClaim).Spec.NodeClassRef.Group}
		}))
		err = multierr.Append(err, c.IndexField(ctx, &v1.NodeClaim{}, "spec.nodeClassRef.kind", func(obj client.Object) []string {
			return []string{obj.(*v1.NodeClaim).Spec.NodeClassRef.Kind}
		}))
		err = multierr.Append(err, c.IndexField(ctx, &v1.NodeClaim{}, "spec.nodeClassRef.name", func(obj client.Object) []string {
			return []string{obj.(*v1.NodeClaim).Spec.NodeClassRef.Name}
		}))
		return err
	}
}

func NodePoolNodeClassRefFieldIndexer(ctx context.Context) func(cache.Cache) error {
	return func(c cache.Cache) error {
		var err error
		err = multierr.Append(err, c.IndexField(ctx, &v1.NodePool{}, "spec.template.spec.nodeClassRef.group", func(obj client.Object) []string {
			return []string{obj.(*v1.NodePool).Spec.Template.Spec.NodeClassRef.Group}
		}))
		err = multierr.Append(err, c.IndexField(ctx, &v1.NodePool{}, "spec.template.spec.nodeClassRef.kind", func(obj client.Object) []string {
			return []string{obj.(*v1.NodePool).Spec.Template.Spec.NodeClassRef.Kind}
		}))
		err = multierr.Append(err, c.IndexField(ctx, &v1.NodePool{}, "spec.template.spec.nodeClassRef.name", func(obj client.Object) []string {
			return []string{obj.(*v1.NodePool).Spec.Template.Spec.NodeClassRef.Name}
		}))
		return err
	}
}

func VolumeAttachmentFieldIndexer(ctx context.Context) func(cache.Cache) error {
	return func(c cache.Cache) error {
		return c.IndexField(ctx, &storagev1.VolumeAttachment{}, "spec.nodeName", func(obj client.Object) []string {
			return []string{obj.(*storagev1.VolumeAttachment).Spec.NodeName}
		})
	}
}

func NewEnvironment(options ...option.Function[EnvironmentOptions]) *Environment {
	opts := option.Resolve(options...)
	ctx, cancel := context.WithCancel(context.Background())

	version := version.MustParseSemantic(strings.ReplaceAll(env.WithDefaultString("K8S_VERSION", "1.36.x"), ".x", ".0"))
	environment := envtest.Environment{Scheme: scheme.Scheme, CRDs: opts.crds}
	if version.Minor() >= 21 && version.Minor() < 32 {
		// PodAffinityNamespaceSelector is used for label selectors in pod affinities.  If the feature-gate is turned off,
		// the api-server just clears out the label selector so we never see it.  If we turn it on, the label selectors
		// are passed to us and we handle them. This feature is alpha in apiextensionsv1.21, beta in apiextensionsv1.22 and will be GA in 1.24. See
		// https://github.com/kubernetes/enhancements/issues/2249 for more info.
		environment.ControlPlane.GetAPIServer().Configure().Set("feature-gates", "PodAffinityNamespaceSelector=true")
	}
	//MinDomains got promoted to stable in 1.32
	if version.Minor() >= 24 && version.Minor() < 32 {
		// MinDomainsInPodTopologySpread enforces a minimum number of eligible node domains for pod scheduling
		// See https://kubernetes.io/docs/concepts/scheduling-eviction/topology-spread-constraints/#spread-constraint-definition
		// Ref: https://github.com/aws/karpenter-core/pull/330
		environment.ControlPlane.GetAPIServer().Configure().Set("feature-gates", "MinDomainsInPodTopologySpread=true")
	}

	_ = lo.Must(environment.Start())

	// Apply any config overrides
	for _, option := range opts.configOptions {
		option(environment.Config)
	}

	// Every suite reads through the informer cache and the overlay, as the manager does, and cluster state follows each
	// kind's newest version through the overlay.
	c := lo.Must(cache.New(environment.Config, cache.Options{Scheme: scheme.Scheme}))
	cached := lo.Must(client.New(environment.Config, client.Options{Scheme: scheme.Scheme, Cache: &client.CacheOptions{Reader: c}}))
	overlaid := lo.Must(overlay.New(ctx, cached, c, overlaidKinds(environment.Config)...))
	indexer := overlaid.FieldIndexer(c)
	for _, index := range opts.fieldIndexers {
		lo.Must0(index(indexingCache{Cache: c, indexer: indexer}))
	}
	lo.Must0(indexer.IndexField(ctx, &corev1.Pod{}, "spec.nodeName", func(o client.Object) []string {
		return []string{o.(*corev1.Pod).Spec.NodeName}
	}))
	go func() {
		lo.Must0(c.Start(ctx))
	}()
	if !c.WaitForCacheSync(ctx) {
		log.Fatalf("cache failed to sync")
	}
	apiServer := lo.Must(client.New(environment.Config, client.Options{Scheme: scheme.Scheme}))
	return &Environment{
		Environment:         environment,
		Client:              NewSyncingClient(overlaid, cached, apiServer),
		APIServer:           apiServer,
		Overlay:             overlaid,
		Clock:               clocktesting.NewFakeClock(time.Now()),
		KubernetesInterface: kubernetes.NewForConfigOrDie(environment.Config),
		Version:             version,
		Done:                make(chan struct{}),
		Cancel:              cancel,
	}
}

// overlaidKinds are the kinds cluster state follows (cluster.Kinds), which the overlay overlays, among those the API
// server serves.
func overlaidKinds(config *rest.Config) []client.Object {
	mapper := lo.Must(apiutil.NewDynamicRESTMapper(config, lo.Must(rest.HTTPClientFor(config))))
	return lo.Filter([]client.Object{&v1.NodeClaim{}, &v1.NodePool{}, &corev1.Node{}, &corev1.Pod{}, &storagev1.CSINode{}, &autoscalingv1beta1.CapacityQuota{}},
		func(obj client.Object, _ int) bool {
			gvk := lo.Must(apiutil.GVKForObject(obj, scheme.Scheme))
			_, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
			return err == nil
		})
}

// indexingCache registers a suite's field indexes with the cache and the overlay.
type indexingCache struct {
	cache.Cache
	indexer client.FieldIndexer
}

func (c indexingCache) IndexField(ctx context.Context, obj client.Object, field string, extractValue client.IndexerFunc) error {
	return c.indexer.IndexField(ctx, obj, field, extractValue)
}

func (e *Environment) Stop() error {
	close(e.Done)
	e.Cancel()
	return e.Environment.Stop()
}
