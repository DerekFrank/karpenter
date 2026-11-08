# State

Karpenter keeps three kinds of state about the objects in a cluster. Each kind
has its own source of truth and its own rules.

| | Facts | Derived fields | Intent |
|---|---|---|---|
| What | the API objects, and joins of objects that are one thing | values computed from other objects: a NodePool's NodeClaim usage, the pod requests on a Node | what Karpenter has decided to do and is still doing: a node reserved against a limit for a NodeClaim it is creating |
| Source of truth | the API server | the API server, through the objects they're computed from | Karpenter's memory, until the objects it decided on exist |
| Consistency | an object, or a join, reads as one version | each field may lag the object and the other fields, and converges | each decision is atomic with the fields it decides on, and counts until its object does |

A decision that completes with one write, such as marking a node for
disruption, is a fact: write it to the object, and the overlay serves it as
soon as the write returns. A decision that takes a while to come true, and that
other decisions count meanwhile, is intent, kept as a claim. See
[Claims](#claims).

## Layout

- **`pkg/state`** holds the primitives, which know no object kinds: `Store`
  (`NewStore`, `NewObjectStore`, `NewJoinStore`, `Join`), `Derive`
  (`Derivation`, `Contribution`, `Equaler`), `Accessor`, `JoinAccessor`,
  `View`, `Claims`, and the sources `Store.Source`, `Requires`, `Wake` and
  `After`.
- **`pkg/state/statetest`** checks a `Contribution`'s laws in tests.
- **`pkg/state/overlay`** holds `overlay.Client`, the read-after-write client:
  its reads include its own writes before the informer delivers them.
- **`pkg/cluster`** holds the cluster's state built from those primitives: one
  file per kind, each with its accessor, such as `cluster.NodePools` in
  `nodepool.go`, and the stores of its derived fields, such as
  `cluster.NodePoolUsage`.
- **`cluster.State`** holds every accessor and the stores behind them.
  `cluster.New` builds it once, where the operator is wired (`kwok/main.go`),
  and passes it to `controllers.NewControllers`. Each controller's constructor
  takes the accessors it reads, as `nodepoolcounter.NewController` takes
  `s.NodePools` and `s.NodePoolUsage`. See [Startup](#startup).
- **`cluster.NodeState`** holds the stores behind the nodes Karpenter
  schedules onto and disrupts, NodeClaim first: each NodeClaim with the intent
  kept about its node and the Node its owner reference links, by name
  (`nodeclaim.go`); each Node, by name, and the providerID link from a
  NodeClaim to its Node before registration (`node.go`); and the pods bound to
  each Node. Its NodeClaim, Node and pod stores follow the informers. A
  `cluster.StateNode` is a view built from them at read time.
- **`pkg/controllers/state`** holds `state.Cluster`, a façade over
  `cluster.NodeState`, with the wait on unlaunched NodeClaims, the pod
  scheduling telemetry and the consolidation state. Its informer controllers
  keep only what takes I/O to read: CSINode drivers, PVC drivers and cost.
- **Imports point one way:** controllers import `pkg/cluster`, which imports
  `pkg/state`. depguard (`.golangci.yaml`, rules `state` and `cluster`) enforces
  it. `hack/storelint`, run by `make verify`, checks where stores are built
  and started: see [Startup](#startup).

## Stores

A `state.Store[K, F]` holds fields `F` per key `K`. Reads are lock-free
lookups (`Get`, `List`); every write takes the store's one lock. There are
three kinds, and each is the source of the next:

| Store | Made with | Holds | Written by |
|---|---|---|---|
| object store | `NewObjectStore[S](kubeClient, informers)` | the objects of kind `S`, by namespaced name | the informer and the overlay's writes, or `Observe`/`Forget`/`Resync` |
| join store | `NewJoinStore(kubeClient, informers, Join(...)...)` | objects of several kinds that are one thing, under one key | the same, one `Join` part per kind |
| derived store | `NewStore[K, F]()` | fields computed from another store, held by claims, or decided by Karpenter | `Derive`, `Claims`, `Store.Update` |

- **An object or join store orders each object's versions** by UID and
  resourceVersion: an older version arriving after a newer one is dropped, a
  delete leaves a tombstone until the informer's delete, and a newer object
  under a name another UID holds replaces it.
- **With a kubeClient, an object or join store is a manager runnable.**
  `Start` lists the kind (the list kind is the GVK plus `List`, from the
  scheme), then follows the informer and the `overlay.Client`'s writes. The
  overlay calls its write handlers before a write returns, so the store holds a
  write as soon as it returns.
- **A store following an informer holds the informer's own objects.** A copy
  from the first List or a write holds an object's version only until the
  informer delivers its own object for it, so the store adds no copy to the
  informer's cache. A caller that feeds a store passes the cache's objects
  (`client.UnsafeDisableDeepCopy`), never a deep copy.
- **Without a kubeClient, a caller feeds it.** `Observe(obj)` applies a version
  seen elsewhere, `Forget(obj)` removes the object of `obj`'s kind and name,
  and `state.Resync(store, objs, scope)` makes `objs` the objects among those
  named in `scope`, which heals missed events. `scope` comes from an index,
  such as `Derivation.Sources(key)`, never from a scan.
- **`Store.Synced`** is true once every kind of an object or join store has
  applied its first List, and once every `Derive` into a derived store has
  applied its source. A store fed by a caller is synced from the start.
- **`Store.Subscribe(fn)`** calls `fn` with each `Change{Key, Old, New,
  Existed, Exists}` after the lock is released, and returns a func that stops
  it. `Existed` and `Exists` tell a key added or removed with zero fields apart
  from no change.
- **`Store.Clear`** empties a store in place and notifies each removal, so
  subscribers stay subscribed. Reset state with `Clear`, never by replacing a
  store.
- **`Store.Touch(key)`** re-notifies a key without changing it, so the derives
  from it recompute.

## Joins

A join is several objects that are one thing under one key. `Join(key, set)`
is one kind's part: `key` is the join
key of an object, and `set` stores the object in the fields, or a nil object
once it leaves the key. One store has one lock, so a join reads as one version.

- **Intent about an object lives in its entry**, set with `Store.Update`, such
  as a node's deletion mark kept with its NodeClaim, in a single-part join
  store. `set` drops the intent once no part is left
  (`NodeClaimFields.orNothing` in `pkg/cluster`), so a gone NodeClaim leaves
  nothing behind.
- **Objects of different kinds that are one thing only some of the time**,
  such as a Node and its NodeClaim, live in their own stores. Once one names
  the other, a `Derive` puts it on the other's entry: a store can be fed by
  both its join part and a `Derive`, as the NodeClaim store holds the Node
  whose owner reference names it. The join part's `set` and the
  contribution's `Delete` each clear only what they set. Before then, a
  derived index links them at read time, as `NodeState.linkedNode` links a
  NodeClaim to the Node with its providerID until the Node registers.
- **`set` is pure and cheap.** It runs under the store's lock on every event.

## Derived fields

`Derive(from, to, key, value)` keeps `to`'s fields up to date from `from`'s:
each key of `from` contributes `value(k, fields)` to the key `key(k, fields)`
returns in `to`. The contribution is a type `C` that implements
`state.Contribution[F, C]`:

- `Update(fields, prev)` returns the fields with the receiver contributing in
  place of `prev`, the zero `C` when the source is new to the key.
- `Delete(fields)` returns the fields without the receiver, which is leaving
  the key. It runs once per stay.

**Derive dispatches every change.** No caller chooses:

- a source new to a key gets `next.Update(fields, zero)`;
- a changed contribution to the same key gets `next.Update(fields, prev)`;
- an equal contribution gets nothing: no write and no notification.
  Contributions compare with `Equal` when `C` implements `state.Equaler[C]`, and
  with `reflect.DeepEqual` otherwise;
- a source that moves gets `prev.Delete(fields)` at the old key, then
  `next.Update(fields, zero)` at the new one;
- a source that is gone, or whose key is zero, gets `prev.Delete(fields)` at its
  old key.

**The zero key means a source contributes to nothing**, such as a NodeClaim
being deleted, which no longer counts against its NodePool.

`Derive` returns a `Derivation`. It applies `from`'s existing keys before it
returns, so a `Derive` made after its source filled still sees everything.
`Sources(key)` returns the source keys contributing to `key`, the index
`Resync` scopes by. `Stop` unsubscribes.

### Writing a contribution

- **Give each contribution a named type**, such as `nodeClaimUsage` or
  `podContribution`, with `Update` and `Delete` methods.
- **`Delete` undoes `Update` from zero, and `Update(fields, prev)` equals
  `prev.Delete` followed by `Update(fields, zero)`.** Check both with
  `statetest.ExpectContribution`, or `ExpectContributionWith` for fields that
  `equality.Semantic` can't compare.
- **Write `Update` to replace `prev` in one pass**, copying each map once:
  merge the new value into a copy, then subtract `prev` from that copy.
- **Return new fields and leave the ones you get intact.** Copy a map or slice
  before changing it. `Update` and `Delete` run under the derived store's lock,
  so they never block and never do I/O.
- **Implement `Equal`** for a contribution holding maps or quantities, so an
  unchanged contribution costs a comparison and nothing else.
- **Keep fields small, and never a collection that grows with the cluster.**
  Fields are immutable, so a change copies them. A set of every node in a
  NodePool would be copied on every node event, which makes the NodePool's
  fields quadratic to keep. Count the members in the fields, and read the
  members themselves from the `Derivation`'s `Sources(key)`, which indexes them
  in place, as `NodeState.NodePoolNodes` and `JoinAccessor.IndexField` do.
- **A contribution that keeps a sample instead of counting**, such as a
  DaemonSet's newest pod (`newestPod`), has a `Delete` that doesn't undo
  `Update`. Say so on the type, and leave it out of the law check.

### Keeping values pure

`key` and `value` run on whatever goroutine changed the source, with no request
behind them.

- **Read only the source's fields.** A value that depends on another object
  belongs in that object's own store, combined at read time. A pod's volumes
  are the PVC keys it mounts (from the pod) and each PVC's driver (from the
  PVC's store); `NodeState.withNode` groups the node's PVCs by driver when it builds a
  StateNode.
- **Capture configuration at construction**, such as the options in the
  context `NewNodeState` takes, which decide a pod's disruption cost.
- **Data Karpenter reads with I/O**, such as a Node's CSINode drivers, goes in a
  derived store keyed by its object, set with `Store.Update` by the caller that
  read it.

## Reading state

Read an object that has a state accessor through the accessor. Write it through
the client.

```go
np, err := nodePools.Get(ctx, client.ObjectKey{Name: name}) // nodePools is a state.Accessor
np.Object.Spec.Limits                                     // the NodePool, through the client
np.Derived.Usage                                          // fields Karpenter keeps about it
kubeClient.Status().Patch(ctx, np.Object, client.MergeFrom(stored))
```

- **An object-backed accessor**, `state.NewAccessor`, reads `Get` and `List`
  through the client, so selectors, namespaces and field indexes work, and the
  object is a copy unless the caller passes `client.UnsafeDisableDeepCopy`.
- **A join-backed accessor**, `state.NewJoinAccessor`, reads from its join
  store. It returns a copy of the object unless the caller passes
  `client.UnsafeDisableDeepCopy`, and filters `List` by label selector.
- **A join-backed accessor's field indexes** work as a cache's do:
  `IndexField(field, index)` derives a store of keys by `index`'s value, and
  `List(client.MatchingFields{field: value})` reads only that value's keys. A
  field selector on an unindexed field is an error, as it is for the client.
- **`Get` and `List` return a `state.View{Object, Derived}`.** `Derived` is
  read from its stores lock-free, and is shared and immutable: copy one to
  change it.
- **Combine stores at read time**, as `NodeState` combines a NodeClaim, its linked Node, its pods'
  usage, its CSI drivers and its PVCs' drivers. Each store is from its own
  moment, so a reader tolerates the lag: a pod that moves between nodes can
  briefly count on both or neither.
- **The client type enforces the rule.** A controller that reads through
  accessors takes a client of `client.Writer` and `client.StatusClient`, so a
  read through the client is a compile error.
- **Write from derived fields once they're synced.** Check `Store.Synced`
  first.

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
  `state.Derive` one in the controller's `Register`: keyed by the object the
  controller reconciles, holding only the fields it acts on, such as the number
  of pods on each deleting node rather than every pod's usage. It moves to
  `pkg/cluster` once a second controller reads it.
- **`store.Source()` is the store as a source**, for a store keyed by name or
  `types.NamespacedName`. Pass it to `NewAccessor`, or watch it on its own.
- **`Start` enqueues every key the store holds, then each one that changes.**
  The workqueue holds a key once, so a burst of changes reconciles the object
  once, with the fields as they are when it runs.
- **`WaitForSync` holds the controller's workers until the store is synced.**
  That is bounded, as controller-runtime's `CacheSyncTimeout` requires.
- **`state.Requires(stores...)`** holds the workers until the stores are synced
  and enqueues nothing, for a controller that reads stores it doesn't watch.
- **`state.Wake(sources...)`** folds every change into one empty request, for a
  controller that reconciles the whole cluster at once, in place of a
  singleton's poll. **`state.After(delay, sources...)`** batches each request's
  changes for `delay`, for a watcher such as metrics.
- **The source wakes the controller; the reconcile reads the fields** through
  the accessor.

## Startup

```go
s := cluster.New(ctx, clk, kubeClient, mgr.GetCache(), cloudProvider)
lo.Must0(s.Register(mgr))
op.WithControllers(ctx, controllers.NewControllers(ctx, mgr, ..., s, ...)...).Start(ctx)
```

1. **`cluster.New` builds the shared stores** and does nothing else.
2. **`s.Register(mgr)` starts them**: it adds every shared object and join
   store as a manager runnable, exactly once. A controller never adds a shared
   store; it adds only the stores it builds itself.
3. **Each controller's `Register` builds its local stores**: its local
   `Derive`s, held in typed fields of the controller, and the sources and
   `Requires` it reads. There is no registry and no lookup by name.
4. **`mgr.Start` syncs the cache, then starts the stores and controllers
   together.** `WaitForSync` orders them, not runnable groups: each store lists
   and marks itself synced, and each controller's workers wait on its sources
   and `Requires`.

- **Declare a controller's stores with `state.Requires` in `Register`**, never
  with a `Synced` check in `Reconcile`. A store's sync is one-way and bounded by
  its first List, so gating the start is enough.
- **Wait for unbounded conditions in `Reconcile`, and wake on them.** "No
  NodeClaim is launching" may never hold, so it isn't in `Requires`: the
  provisioner and disruption check `NodeClaimsLaunched`, and disruption wakes
  on `cluster.NodeState.UnlaunchedNodeClaimStore`.
- **`hack/storelint` enforces where stores are built and started.**
  `state.NewStore`, `NewObjectStore`, `NewJoinStore`, `Join` and `Derive` are
  called only in a `Register(context.Context, manager.Manager) error` method,
  in `pkg/cluster`, in `pkg/state` or in a test; and only `cluster.State.Register`
  adds a shared store, one reached through a `pkg/cluster` value, to a manager.
- **A suite's cluster state has no informers.** It reconciles the informer
  stand-ins in `pkg/test/expectations` (`NodeClaimInformer`, `NodeInformer`,
  `PodInformer`), which show cluster state an object as the informer would.

## Claims

A claim holds a value against a key for an object a caller is about to create,
such as a node against a static NodePool's node limit, so a decision against a
limit counts what is already on its way. Claims live in the derived store they
count with, so admission and the handoff to the counted object happen under one
lock and are atomic.

```go
reservations = r.cluster.NodePoolState.ReserveNodeCount(np.Name, nodeLimit, 1) // admits atomically against counted + held
nodeClaim.Reservation = reservations[0]                                         // the provisioner binds or releases it
```

- **`state.NewClaims(derivation, counts, ttl, clock)`** makes claims against
  the keys a `Derivation` feeds. A held value is a `state.Contribution` to the
  fields: `Update` from zero holds it, and `Delete` releases it. `counts`
  returns whether a source's contribution counts what its claim held, such as
  launched capacity for a claim of the largest instance type it might launch.
- **`Claims.Reserve(key, n, value, admit)`** grants up to `n` claims, one at a
  time while `admit` accepts the fields with the claim held. Hold the most the
  object could count, so the limit holds while the object is on its way.
- **Every claim ends exactly once.** `Bind` it to its source key once created;
  the `Derivation` releases it when it counts that source, at once if it
  already has, or when the source goes away first. `Release` it when the caller
  drops the create. A claim uncounted within its TTL (`ReservationTTL` for
  NodePools) expires.
- **Each object counts at least once.** An object may count twice for a moment,
  while it counts before its claim is bound; that only refuses a later
  admission, so it is safe.
- **A claim travels with its work**, such as `scheduling.NodeClaim.Reservation`
  on the NodeClaim a command creates. Every path that drops the work releases
  it, as the provisioner and the disruption queue do.
- **Claims live in memory.** Clearing their derived store ends them.

## Adding state

1. Find the source: an object kind (object store), several kinds that are one
   thing (join store), or another store's fields (derived store).
2. Key the derived store by the object the fields describe, and list the fields
   in `F`, each commented with what it holds, which kind feeds it, and when a
   source stops counting.
3. Write the contribution type, its `Update`, `Delete` and, for maps, `Equal`.
4. `Derive` it, and check the laws with `statetest`.
5. Read it through an accessor, or combine it at read time where the reader
   builds its view.

## Testing

- Drive an object store with `Observe` and `Forget`, then assert on the derived
  store's `Get`, as `store_test.go` does.
- Cover moving between keys (`TestDeriveMovesContributionsBetweenKeys`), an
  older version arriving after a newer one
  (`TestObjectStoreKeepsTheNewestVersionAcrossWritesAndTheInformer`), the
  dispatch (`TestDeriveDispatchesUpdateAndDelete`) and the source going away
  (`TestStoreDropsAKeyOnceNothingContributesAndItsFieldsAreZero`).
- Check a field against a recount of its sources after many concurrent changes,
  as `TestDeriveConcurrentChangesConverge` does.
- Check each contribution's laws with `statetest.ExpectContribution`.
- Read through the accessor over a fake client, as `accessor_test.go` does.
- Start a source on a workqueue and assert on what it queues, as
  `TestAccessorEnqueuesObjectsWhoseDerivedFieldsChange` and `source_test.go` do.
  Test a derived store's narrowing as
  `TestADerivedStoreNotifiesOnlyWhenItsNarrowerFieldsChange` does.
- Test claims as `claims_test.go` does: race reservers that create and delete
  objects against a recount of what objects count and live claims hold, and
  check that what objects count stays within the limit
  (`TestClaimsNeverExceedTheLimitOrCountAnObjectZeroTimes`).
