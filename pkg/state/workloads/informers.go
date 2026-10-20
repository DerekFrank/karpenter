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

package workloads

import (
	"context"
	"fmt"

	"github.com/awslabs/operatorpkg/serrors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// Informers registers the informers for the kinds the enabled features read before the manager starts. They sync with
// the rest of the cache before any controller runs, so reads never start an informer on the scheduling path, and
// missing RBAC fails at startup.
type Informers struct {
	objects []client.Object
}

func NewInformers(objects ...client.Object) *Informers {
	return &Informers{objects: objects}
}

func (i *Informers) Name() string {
	return "state.workloads"
}

func (i *Informers) Register(ctx context.Context, m manager.Manager) error {
	for _, obj := range i.objects {
		if _, err := m.GetCache().GetInformer(ctx, obj); err != nil {
			return serrors.Wrap(fmt.Errorf("registering informer, %w", err), "kind", fmt.Sprintf("%T", obj))
		}
	}
	return nil
}
