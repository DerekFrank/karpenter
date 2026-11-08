# State

Karpenter keeps three kinds of state about the objects in a cluster. Each kind
has its own source of truth and its own rules.

| | Facts | Derived fields | Intent |
|---|---|---|---|
| What | the API objects, and joins of objects that are one thing (a Node and its NodeClaim) | values computed from other objects: a NodePool's NodeClaim usage, the pod requests on a Node | what Karpenter has decided and only Karpenter records: nominations, deletion marks |
| Source of truth | the API server | the API server, through the objects they're computed from | Karpenter's memory |
| Consistency | an object, or a join, reads as one version | each field may lag the object and the other fields, and converges | per decision |

This file covers facts and derived fields. Intent lives in `state.Cluster` in
`pkg/controllers/state`.

## Layout

- **`pkg/state`** holds the primitives, which know no object kinds: `Store`,
  `Contribute` (`Contributor`), `Derive`, `Accessor` (`NewAccessor`), `View`,
  and the sources `Store.Source`, `Wake` and `After`.
- **`pkg/state/overlay`** holds `overlay.Client`, the read-after-write client:
  its reads include its own writes before the informer delivers them.
- **`pkg/cluster`** holds the cluster's state built from those primitives: one
  file per kind, each with its accessor, such as `cluster.NodePools` in
  `nodepool.go`, and the stores of its derived fields, such as
  `cluster.NodePoolUsage`.
- **`cluster.State`** holds every accessor and the stores behind them.
  `cluster.New` builds it once, in `pkg/controllers/controllers.go`. Each
  controller's constructor takes the accessors it reads, as
  `nodepoolcounter.NewController` takes `s.NodePools` and `s.NodePoolUsage`.
- **Imports point one way:** controllers import `pkg/cluster`, which imports
  `pkg/state`. depguard (`.golangci.yaml`, rules `state` and `cluster`) enforces
  it.

## Reading state

Read an object that has a state accessor through the accessor. Write it through
the client.

```go
nodePool, err := c.nodePools.Get(ctx, req.NamespacedName) // c.nodePools is a *cluster.NodePools
nodePool.Object.Status.Resources = lo.Assign(BaseResources, nodePool.Derived.Usage)
c.kubeClient.Status().Patch(ctx, nodePool.Object, client.MergeFrom(stored))
```

- **A state accessor** is a `state.Accessor` for one kind, such as
  `cluster.NodePools`. `Get` and `List` return a `state.View{Object, Derived}`,
  such as `cluster.NodePool`.
- **`Object` is the API object**, read through the client, so it is
  read-after-write with the overlay. Write, watch and record events for it as
  for any object.
- **`Derived` is in-memory only**, read from its stores lock-free.
- **Derived values are shared and immutable.** Copy one to change it.
- **The client type enforces the rule.** A controller that reads through
  accessors takes a client of `client.Writer` and `client.StatusClient`, as the
  `writer` interface in `pkg/controllers/nodepool/counter` does, so a read
  through the client is a compile error.

## Reacting to derived fields

A store is a controller-runtime source whose requests are its keys, and an
accessor is the source of the stores behind it. A controller that watches one
reconciles an object whenever the object's derived fields change:

```go
controllerruntime.NewControllerManagedBy(m).
	For(&v1.NodePool{}, ...).      // the object's own events
	WatchesRawSource(nodePools).   // its derived fields' changes
	Complete(c)
```

- **Watch a store whose keys and fields are exactly what the controller
  reconciles on.** A store notifies only when a key's fields change, so its
  fields are its filter and its keys are its requests. When no store fits,
  `state.Derive` one: keyed by the object the controller reconciles, holding
  only the fields it acts on, such as the number of pods on each deleting node
  rather than every pod's usage.
- **`store.Source()` is the store as a source**, for a store keyed by name or
  `types.NamespacedName`. Register it with `NewAccessor`, or watch it on its
  own.
- **`Start` enqueues every key the store holds, then each one that changes.**
  The workqueue holds a key once, so a burst of changes reconciles the object
  once, with the fields as they are when it runs.
- **`WaitForSync` holds the controller's workers until the store is synced**:
  every Contributor that feeds it has applied its first List, and every store
  it is derived from is synced. That is bounded, as controller-runtime's
  `CacheSyncTimeout` requires.
- **`state.Wake(sources...)`** folds every change into one empty request, for a
  controller that reconciles the whole cluster at once, in place of a
  singleton's poll. **`state.After(delay, sources...)`** batches each request's
  changes for `delay`, for a watcher such as metrics.
- **The source wakes the controller; the reconcile reads the fields** through
  the accessor.

## Keeping derived fields

Every mutation updates the derived fields, so a read is a lookup whose cost is
independent of the number of objects behind the field.

- **One `state.Store[K, F]` per set of derived fields**, made with
  `state.NewStore` and keyed by the object the fields describe, such as the
  NodePool's name. `F` lists the fields.
- **One `Contributor` per kind that feeds them**, made with `state.Contribute`.
  It names the key each object contributes to (`target`), what it contributes
  (`contribution`), and how a change folds into the fields
  (`apply(fields, prev, next)`).
  - Run it as a manager runnable (`Start`), so it follows the informer and the
    `overlay.Client`'s writes, as the nodepool counter's `Register` adds
    `usage.NodeClaims`.
  - `Observe` feeds an object seen elsewhere, and `Forget` removes one known
    only by name.
  - `Recompute` refreshes a contribution that reads other objects.
  - `Resync` makes a listed set the contributors to one key, healing missed
    events.
  - `CompareWith` sets the equality used to skip unchanged contributions.
- **`state.Derive` feeds a store from another store's fields**, such as Node
  capacity summed onto its NodePool.
- **`Store.Update` sets fields Karpenter decides**, under the store's lock.
  `Store.Subscribe` calls a func with each changed key until the func it returns
  is called.
- **The accessor's `derive` reads the stores**, so readers get the fields.
- **Each aggregate lives in a `Store`**, fed by a `Contributor` or `Derive`.
  Look up objects by a value of one object, such as pods by `spec.nodeName`,
  with a field index registered in `setupIndexers` in `pkg/operator`.

## Joins

A join is several objects that are one thing, such as a Node and its NodeClaim
joined on providerID. Its parts live in one store under one key, with a
`Contributor` per kind contributing the object itself. One store has one lock,
so a join reads as one version.

Derived fields of a join, such as the pods' usage of a Node, live in their own
stores. Each store has its own lock, so fields from different stores, and a
join and its fields, may lag each other. A reader that combines them tolerates
the lag: a pod that moves between nodes can briefly count on both or neither.

## Adding a derived field

- Add the field to its store's fields struct and to the accessor's `Derived`
  struct, such as `cluster.NodePoolDerived`. Comment what it holds, which kind
  feeds it, and when an object stops counting.
- `target` is a pure function of the object, since a contribution moves only
  when the object changes. Make `contribution` pure too where possible. A
  contribution that reads other objects, such as a pod's PVCs, is current as of
  the object's last change or `Recompute`; say so on the field.
- `apply` returns new fields and leaves the ones it gets intact. Copy any map or
  slice before changing it. `apply` runs under the store's lock, so it is
  non-blocking and free of I/O.
- `target`, `contribution` and `apply` run on every event. Read the condition or
  field directly, as `NewNodePoolUsage` reads `Status.Conditions`.

## Testing

- Drive a `Contributor` with `Observe` and `Forget`, then assert on the store's
  `Get`, as `store_test.go` does.
- Cover moving between keys (`TestContributorMovesContributionsBetweenKeys`), an
  older version arriving after a newer one
  (`TestContributorKeepsTheNewestVersionAcrossWritesAndTheInformer`), and the
  object going away
  (`TestStoreDropsAKeyOnceNothingContributesAndItsFieldsAreZero`).
- Check a field against a recount of its objects after many concurrent changes,
  as `TestContributorConcurrentChangesConverge` does.
- Read through the accessor over a fake client, as `accessor_test.go` does.
- Start a source on a workqueue and assert on what it queues, as
  `TestAccessorEnqueuesObjectsWhoseDerivedFieldsChange` and `source_test.go` do.
  Test a derived store's narrowing as
  `TestADerivedStoreNotifiesOnlyWhenItsNarrowerFieldsChange` does.
