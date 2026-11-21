# State

Karpenter keeps the cluster in stores. A store is a map from a key to an
entry, and an entry describes one object:

```go
type Entry[O, D, I any] struct {
    Object  O // what the entry is about
    Derived D // fields computed from other stores
    Intent  I // what Karpenter decided about the object
}
```

| | Object | Derived | Intent |
|---|---|---|---|
| What | an API object, or a Go type Karpenter or the provider defines | values computed from other stores: a NodePool's NodeClaim usage, the pod requests on a Node | what Karpenter decided and is still doing: a node marked for deletion, a nomination |
| Written by | the informer and the overlay; a feed; construction from the key | `Derive`s and `Claims` | the intent's handle, held by its one writer |
| Consistency | an entry reads as one version | each field may lag the object and other stores, and converges | each write is atomic with the entry it's on |

A decision that completes with one write, such as a NodeClaim's
`karpenter.sh/disrupted` taint, is a fact: it's written to the object, and
the overlay serves it as soon as the write returns. A decision that takes a
while to come true, and that other decisions count meanwhile, is a claim held
in `Derived`. See [Claims](#claims).

## Layout

- **`pkg/state`** holds the primitives, which know no object kinds: `Entry`,
  `Store` and `Reader` (`NewObjectStore`, `NewFedStore`, `NewSyntheticStore`), `Derive` and `DeriveObject` (`Derivation`,
  `Contribution`, `Equaler`), `NewIndex` (`Index`), `DependsOn`, `Intent` and
  `Intended`, `Claims`, and the sources `Reader.Source`, `Reader.SourceOn`,
  `Requires`, `Wake` and `WakeCounting`.
- **`pkg/state/statetest`** checks a `Contribution`'s laws in tests, and holds
  `Objects`, a source a test fills as an informer would.
- **`pkg/state/overlay`** holds `overlay.Client`, the read-after-write client:
  its reads include its own writes before the informer delivers them.
- **`pkg/cluster`** holds the cluster's state built from those primitives, one
  file and one type per object or synthetic object: `nodeclaim.go`
  (`NodeClaims`, the managed nodes, with `NodeClaimsByProviderID` and
  `NodeClaimsByNodePool`, the `Deletion`, `Nomination` and `BufferPods`
  handles, and `Registered`, `InFlight`, `Deleting`, `Nominated` and
  `HasBufferPods`), `node.go` (`Nodes` with `Unmanaged`, `CSINodes`),
  `nodeview.go` (`NodeKey` and `NodeView`), `nodeusage.go` (the pod usage on a
  Node's entry), `pod.go` (`Pods`, `PodsByNode`, `PodsByDaemonSet` and
  `PodsBySimulatedNodeClaim`, `DaemonSetPod`, `PodsOn`, and the
  `PodScheduling` handle), `pvc.go` (PVCs and their drivers), `cluster.go`
  (the synthetic `Cluster{}` and its counts), `nodepool.go` (`NodePoolState`),
  `capacityquota.go` and `cost.go` (`ClusterCost`). `state.go` composes them.
  `pkg/cluster/prediction` and `pkg/cluster/virtualpods` hold the prediction
  store and the CapacityBuffer virtual pods.
- **`cluster.State`** holds every store's Reader, the indexes on them and the
  intent handles. `cluster.New` builds it once, where the operator is wired
  (`kwok/main.go`), and passes it to `controllers.NewControllers`, which gives
  each controller the Readers it reads and the handles it writes: the
  disruption queue gets `Deletion`, and the provisioner gets `Nomination`,
  `BufferPods` and `PodScheduling`. `State.Close` stops it once the manager
  stops. See [Startup](#startup).
- **Imports point one way:** controllers import `pkg/cluster`, which imports
  `pkg/state`. depguard (`.golangci.yaml`) enforces it. `hack/storelint`, run
  by `make verify`, enforces the rules in [Lint](#lint).

## Stores

There are two kinds of store. They differ only in where the object comes from:

| Store | Made with | Object | Written by |
|---|---|---|---|
| object store | `NewObjectStore[K, S, D, I](from, opts...)`, with `WithKey(func(types.NamespacedName) K)` (such as `state.ByName`) and `WithFilter(func(S) bool)` | the newest version of each object of kind `S` that `from` holds, under its namespaced name or `key(name)` | `from`: the overlay client's `Source` for the kind, or `statetest.Objects` in a `pkg/state` primitive spec, a benchmark or the scheduling properties harness |
| synthetic store | `NewFedStore[K, O, D, I]()` | a Go type, reported by a feed | `Feed` and `FeedAll` |
| | `NewSyntheticStore(build)` | `build(key)`, while the entry has a contribution | `Derive`s and `Claims` |

- **An entry exists while it has an object.** An object store's entry drops
  when the informer deletes the object, or a new UID replaces it under the
  same name. A fed store's entry drops when the feed stops reporting the
  object. A key-built entry exists while a `Derive` or a claim contributes to
  it.
- **Intent drops with its entry.** A new object under an old name starts with
  no intent.
- **`Derived` waits for its object.** Contributions to a key whose entry
  doesn't exist, such as the pods bound to a Node the store hasn't seen yet,
  are kept, and the entry has them once its object arrives. `Get` returns
  them with `false`; `List` and `Subscribe` see only entries that exist.
- **A synthetic object is a plain Go type**, such as `cluster.Cluster` or a
  `TopologyGroup`, and never a `client.Object`, so a client write of one
  doesn't compile and it never reaches the API server.
- **A store's entries never change once stored.** A reader takes no lock and
  can keep what `Get` and `List` return. Every write takes the store's one
  lock, and notifies once the lock is released.
- **A `Derive` commits only from its source's current entry.** It computes a
  contribution from a source entry without a lock, and commits it only if that
  entry is still the source store's current one, by pointer, recomputing
  otherwise: a removed source has no current entry, so a recompute from before
  the removal is refused and retried.
- **An object store applies the newest version of an object on each
  notification.** On every key its source notifies, from an informer event, a
  write or a delete, it reads that object's newest version and commits it, or
  drops the entry once it's gone, then reads again and repeats until what it
  read is what it committed, so a read from before a removal can't commit after
  it. `overlay.Source` returns the newer of the cache's object and the client's
  own write, and hides an object the client deleted until the informer's store
  drops it. A newer resourceVersion wins, within one UID and across UIDs under
  one name. A removed UID is kept in the store's `gone` set while the source
  still holds it, so a stale read of it loses.
- **Every kind a store follows is overlaid**, so notifications come only from
  the overlay, after it has updated. `cluster.Kinds` lists them, and the
  operator overlays them. An object store holds the informer's own objects, or
  the overlay's copy of a write until the informer delivers its own.
- **A store holds every object of its kind.** A `Derive`'s zero key does the
  filtering, as cost's Derives do for NodeClaims without the labels that price
  them. `WithFilter` is for a kind whose readers would all have to
  filter, or whose objects cost memory to hold, measured first: the NodeClaim
  store holds only the NodeClaims of the cloud provider's node classes.
- **An object store is a manager runnable.** `Start` waits for its source to
  sync, then applies every key the source holds. A store whose source has
  synced when it's built applies them then.
- **A feed upserts one entry with `Feed(key, obj)`, or replaces a group with
  `FeedAll(in, objects)`.** `Feed` with the zero object drops the entry.
  `FeedAll` makes `objects` the store's objects among the keys `in` accepts,
  such as one NodeClass's instance types, so a reader sees the old set or the
  new one.
- **`Reader`** is a store's read-only handle: `Get`, `List`, `Subscribe`,
  `Synced`, `Source` and `SourceOn`. `pkg/cluster` exports Readers. See
  [Reading](#reading).
- **`Synced`** is true once an object store's source has synced and it has
  applied every key the source held then, and every `Derive` into a store has
  applied its source. A fed or key-built store is synced from the start.
- **`Subscribe(fn)`** calls `fn` with each `Change{Key, Old, New, Existed,
  Exists}` after the lock is released, and returns a func that stops it.
- **`Close`** stops what keeps a store current or holds a timer on it: an
  object store stops following its source, and each `Derive` into the store,
  with the `Claims` against it, each `WithExpiry` intent's timer and each
  `DependsOn` that touches it stop. The entries stay as they were. It's
  idempotent. `cluster.State.Close` closes every store `cluster.New` built.
- **`DependsOn` is the only way to touch a key**, so the `Derive`s from its
  store recompute it: `touch` is internal to `pkg/state`. See
  [Keeping values pure](#keeping-values-pure).

### Index

- **`state.NewIndex(store, values)`** returns an `*Index` of a store's
  entries by the values `values(k, entry)` returns, of any comparable type,
  such as the pods by each PVC they mount:
  `mountsPVC := state.NewIndex(pods, func(k, e) []types.NamespacedName {...})`.
  `values` runs under the store's lock on every change of an entry, so it's
  pure and cheap, like a contribution; a value it returns twice indexes the
  key once.
- **Only entries that exist are indexed.** An entry that stops existing leaves
  every value, and `Derived` waiting for its object is not indexed. `NewIndex`
  indexes the entries a store already holds before it returns.
- **`idx.Lookup(v)`** returns the keys indexed under `v`, in no particular
  order, without a lock: each value's keys are replaced, never changed, on a
  write.
- **Matching a field is the index's `List`.** `idx.List(v, opts...)` returns
  the entries indexed under `v`, as `Lookup` then `Get`, where the client
  would take `client.MatchingFields`: `cluster.PodsByNode.List(name)` is the
  pods bound to the Node `name`. It takes the options a Reader's `List` takes,
  and copies each object as it does. An entry that changed since the `Lookup`
  may no longer return `v`, so a reader re-checks it.
- **An index replaces a `Derive` used only for its `Sources`.** `pkg/cluster`
  exports its indexes on `cluster.State` as `<Store>By<Thing>`:
  `NodeClaimsByProviderID` (the providerID is never a key),
  `NodeClaimsByNodePool` (`NodePoolState.LaunchedNodes` and
  `UnlaunchedNodeClaims`), `PodsByNode` (`cluster.PodsOn`), `PodsByDaemonSet`
  (`cluster.DaemonSetPod` picks a DaemonSet's newest pod from it) and
  `PodsBySimulatedNodeClaim`. `Nodes.Unmanaged` reads the Node store's index of
  unmanaged Nodes.
- **Index by a value that holds few keys**, such as a PVC or a NodeClaim. A
  write that moves a key into or out of a value copies that value's keys, so
  indexing many keys under one value, such as pods by namespace, makes every
  create and delete under it cost the size of the value.

## Writers

Each part of an entry has one kind of writer:

- **`Object`**: the informer and the overlay, for object stores; the feed, or
  construction from the key, for synthetic stores.
- **`Derived`**: `Derive`s and `Claims`, and nothing else. A `Derive`'s target
  is written only by its `Derive`s and `Claims`.
- **`Intent`**: the intent's handle. Its holder is the only writer.

Data Karpenter reads with I/O lives in its own store, keyed by its own
object, and drops with that object: a CSINode in the CSINode object store, a
PVC's resolved driver in the `PVCs` fed store. A `Derive` carries it to the
stores that use it.

## Derive

`Derive(from, to, key, value)` keeps `to`'s `Derived` fields up to date from
`from`'s entries: each entry of `from` contributes `value(k, entry)` to the
key `key(k, entry)` returns in `to`. The contribution is a type `C` that
implements `state.Contribution[D, C]`:

- `Update(fields, prev)` returns the fields with the receiver contributing in
  place of `prev`, the zero `C` when the source is new to the key.
- `Delete(fields)` returns the fields without the receiver, which is leaving
  the key. It runs once per stay.

**Derive dispatches every change:**

- a source new to a key gets `next.Update(fields, zero)`;
- a changed contribution to the same key gets `next.Update(fields, prev)`;
- an equal contribution gets nothing: no write and no notification.
  Contributions compare with `Equal` when `C` implements `state.Equaler[C]`, and
  with `reflect.DeepEqual` otherwise;
- a source that moves gets `prev.Delete(fields)` at the old key, then
  `next.Update(fields, zero)` at the new one;
- a source that is gone, or whose key is zero, gets `prev.Delete(fields)` at its
  old key.

**The zero key contributes to nothing**, such as a NodeClaim being deleted,
which stops counting against its NodePool. A singleton synthetic object
keys by a non-zero value, as `cluster.TheCluster` does.

`Derive` applies `from`'s entries before it returns, so a `Derive` made after
its source filled sees everything. `Sources(key)` returns the source keys
contributing to `key`. `Stop` unsubscribes. `Remove` takes back everything the
`Derivation` contributed and leaves nothing behind in its target, for a
`Derive` whose configuration is replaced.

**`DeriveObject(from, to, key, value)`** is a `Derive` whose contribution reads
only the source's object, such as a Node's NodePool label: it recomputes only
when the object changes, so a change of the entry's `Derived`, such as a pod
binding to the Node, costs a comparison.

### Writing a contribution

- **Each contribution is a named type**, such as `podContribution` or
  `linkedNode`, with `Update` and `Delete` methods.
- **Every contribution satisfies the laws:** `Delete` undoes `Update` from
  zero, and `Update(fields, prev)` equals `prev.Delete` followed by
  `Update(fields, zero)`. `statetest.ExpectContribution` checks both, or
  `ExpectContributionWith` for fields that `equality.Semantic` can't compare.
- **A sample is chosen at read time from an index**, such as a DaemonSet's
  newest pod (`cluster.DaemonSetPod` over `PodsByDaemonSet`), rather than kept
  in a field, which would have to choose it again whenever it leaves.
- **`Update` replaces `prev` in one pass**, copying each map once: it merges
  the new value into a copy, then subtracts `prev` from that copy.
- **A contribution returns new fields and leaves the ones it gets intact.** It
  copies a map or slice before changing it. `Update` and `Delete` run under
  the target's lock, so they never block and never do I/O.
- **A contribution holding maps or quantities implements `Equal`**, so an
  unchanged contribution costs a comparison and nothing else.
- **Fields are small, and none is a collection that grows with the cluster.**
  A map or slice field carries a `// bounded: <what bounds it>` comment, such
  as "pods per node". To relate many objects to one key, index them by it with
  `NewIndex`, as `NodeClaimsByNodePool` does for `NodePoolState.LaunchedNodes`.
- **A join is a `Derive` onto an entry.** The Node a NodeClaim's owner
  reference links, with its usage and repair policy matches, is the NodeClaim
  entry's `Derived`, contributed from the Node store (`linkedNode`).
- **Every derived field has a reader.** A field with no reader is deleted with
  its `Derive`. A `Derive` computes a field a reader reads; waking a controller
  on part of an entry is `SourceOn`'s job.

### Keeping values pure

`key` and `value` run on whatever goroutine changed the source, with no
request behind them.

- **They read the source entry**, and configuration captured at construction,
  such as the options in the context `cluster.New` takes, which decide a pod's
  disruption cost.
- **A value that depends on another store's entries** reads them, and
  `DependsOn` keeps it current. A pod's volume usage reads its PVCs' drivers
  from `PVCs` (`pvc.go`). A value that reads what another store contributes to
  its own entry needs nothing more: a NodeClaim's contribution to its
  NodePool's disruption counts reads its own entry's `Derived.Node`, so the
  Node's change reaches it as a change of the NodeClaim's entry.
- **`DependsOn(target, dep, idx)`** keeps it current over an `Index`: `idx`
  indexes the source store `target`'s entries by the keys of `dep` they read,
  such as `DependsOn(pods, pvcs, mountsPVC)`, and on each change of `dep`'s
  entry under a key, including one that appears or drops, `DependsOn` touches
  the keys of `target` that `idx.Lookup(key)` returns, so the `Derive`s from
  `target` recompute them. An `idx` of another store panics. The touch stores
  the entry in a new slot, so a recompute that read `dep` before it changed is
  refused at commit, and the one the touch triggers holds. It returns a func
  that stops it.

## Intent

- **Intent is a field group on the object's entry**, read with the object, and
  each field is a `state.Intended[V]{Value, Until}`.
- **Each intent has a typed handle**, `state.NewIntent(store, field)`, with
  `Set`, `Clear`, `Get`, and `Update`, which computes the value from the one it
  replaces under the store's lock. `pkg/cluster`
  wraps each in a type its one writer holds: `Deletion` (`Mark`, `Unmark`) for
  the disruption queue, `Nomination` (`Nominate`) and `BufferPods`
  (`Replace`) for the provisioner, `PodScheduling` (`Ack`, `MarkDecisions`,
  `MapToNodeClaims`, on the pods' entries) for the provisioner and the pod
  controller that acks the pods it triggers it for, and `Registrations`
  (`Record`) for the NodeClaim lifecycle controller, whose recorded launch
  outcomes the registration health controller reads to set
  `NodeRegistrationHealthy`. A controller without the handle can't write the
  intent.
- **`Set` on an entry that doesn't exist does nothing**, since intent about an
  object the store doesn't hold would outlive it.
- **`WithExpiry(clock, ttl)`** makes a value lapse `ttl` after it's set, or at
  the time `SetUntil` gives. A lapse is an event: the handle clears the value
  from a timer on the clock at the earliest lapse, and the entry notifies. The
  timer stops once nothing can lapse, and for good once the store closes. A
  suite steps its fake clock to lapse intent. `Intended.At` reads a value as of a time.

## Claims

A claim holds a value in an entry's `Derived` fields for an object a caller is
about to create, such as a node against a static NodePool's node limit or a
NodeClaim's largest capacity against a NodePool's resource limits, so a
decision against a limit counts what is already on its way. Admission and the
handoff to the counted object happen under the store's one lock.

```go
reservations = r.cluster.NodePoolState.ReserveNodeCount(np.Name, nodeLimit, 1) // admits atomically against counted + held
nodeClaim.Reservation = reservations[0]                                         // the provisioner binds or releases it
```

- **`state.NewClaims(derivation, counts, ttl, clock)`** makes claims against
  the keys a `Derivation` feeds. A held value is a `state.Contribution` to the
  fields: `Update` from zero holds it, and `Delete` releases it. `counts`
  returns whether a source's contribution counts what its claim held. Several
  `Claims` can hold against one `Derivation`, each settled by its own
  `counts`.
- **`Claims.Reserve(key, n, value, admit)`** grants up to `n` claims, one at a
  time while `admit` accepts the fields with the claim held. `admit` checks
  every condition the decision depends on. `n` is the most the caller wants,
  never a shortfall computed from a read outside `Reserve`.
- **`Claims.ReserveReplacing(key, n, value, replacing, admit)`** admits as if
  the sources in `replacing` had stopped contributing, under the same lock, for
  objects that replace them, such as a disruption's replacements for its
  candidates.
- **Every claim ends exactly once.** `Bind` it to its source key once created;
  the `Derivation` releases it when it counts that source, at once if it
  already has, or when the source stops contributing or goes away first.
  `Release` it when the caller drops the create. Otherwise it expires.
- **Expiry is an event.** A claim not bound, or bound to a source the
  `Derivation` hasn't seen contributing, expires at its TTL (`ReservationTTL`
  for NodePools and CapacityQuotas): the `Claims` arm one timer at the
  earliest expiry among their live claims, expire the claims due when it
  fires, and notify the key. A suite steps its fake clock to expire claims.
  `Claims.OnExpire` counts expiries
  (`karpenter_cluster_state_claims_expired_total`).
- **Cluster state's timers wait on the clock it was given.** A spec that waits
  for the code under test to block on a shared fake clock gives that code a
  `test.NewClockView(env.Clock)` and waits on the view's `HasWaiters`, which
  counts only the code's own waiters, not cluster state's lapse and expiry
  timers on the same clock.
- **Each object counts at least once.** An object may count twice for a moment,
  while it counts before its claim is bound; that only refuses a later
  admission.
- **A claim travels with its work**, such as `scheduling.NodeClaim.Reservation`
  on the NodeClaim a command creates. Every path that drops the work releases
  it.
- **Fields an `admit` reads share an entry.** Fields share a store only when a
  consumer, a claim's `admit` above all, needs them consistent.
- **All-or-nothing across keys** reserves each key in sorted key order and
  releases everything on a refusal, as `Provisioner.ReserveLimits` does for a
  NodePool and its CapacityQuotas.
- **A `Derive` whose configuration changes is rebuilt** by a controller driven
  by the configuration's store source. Each CapacityQuota has its own
  `Derive`s, with its selector captured when they're built; a spec change
  builds new ones and `Remove`s the old, which releases the claims against
  them. The enforced quota is the CapacityQuota's intent.

## Reading

```go
np, ok := s.NodePoolState.Get(name)  // an Entry; the NodePool is a deep copy
np.Object.Spec.Limits
np.Derived.Usage                     // fields derived about it, shared
kubeClient.Status().Patch(ctx, np.Object, client.MergeFrom(stored))

for _, nc := range s.NodeClaims.List(client.UnsafeDisableDeepCopy) { // the store's own objects, read-only
    cluster.ViewOfNodeClaim(nc).Allocatable()
}
```

- **A `Reader` returns entries.** `Get(key, opts...)` returns the `Entry` and
  whether it exists; `List(opts...)` returns every entry that exists, as an
  `iter.Seq2` of key and entry.
- **`Get` and `List` deep-copy the object**, so a caller can change and write
  it: with the object's `DeepCopy`, as every API object has; a synthetic object
  without one is a value or never changed by its readers, and is returned as
  is. `client.UnsafeDisableDeepCopy` returns the store's own object instead,
  for a hot read that only reads, such as a metrics scrape, a scheduling
  snapshot or a disruption candidate list. `Derived` and `Intent` are never
  copied: they're shared and never change.
- **Options are controller-runtime's.** A `List` takes exactly
  `client.UnsafeDisableDeepCopy` and a label selector (`client.MatchingLabels`,
  `client.MatchingLabelsSelector`), a scan, on a store of `metav1.Object`s; a
  `Get` takes only `client.UnsafeDisableDeepCopy`. Any other option panics,
  naming it, including `client.MatchingFields`: matching a field is an
  [Index](#index)'s `List`.
- **A read takes one lookup per store per object.** A managed node is one
  NodeClaim entry, which carries its Node. A value aggregated across objects
  is a derived field.
- **Readers tolerate lag between stores.** An object and the fields derived
  from other stores may be from different moments, and converge.
- **An object read without a copy is read-only.** It's the informer's own
  object, as are a NodeClaim entry's `Derived.Node` and a `NodeView`'s
  `NodeClaim()` and `Node()`. Writing one, even through the client, writes the
  cache: `Create`, `Update`, `Patch`, `Status().Patch` and a `Delete` that
  leaves the object in storage decode the server's response into the object
  they're passed. storelint reports a client write of one. Write a
  `DeepCopy`, and read conditions with
  `StatusConditions(status.WithObservedOnly())`.
- **Read an object that has a store through its `Reader`, and write it
  through the client.** A controller that reads through Readers takes a
  client of `client.Writer` and `client.StatusClient`.

## Reacting

A controller wakes on a store's source, narrowed to the fields it acts on:

```go
controllerruntime.NewControllerManagedBy(m).
	Named("node.termination").
	WatchesRawSource(nodes.SourceOn(func(_ string, e cluster.NodeEntry) any {
		if e.Object.DeletionTimestamp.IsZero() {
			return nil
		}
		return e.Derived.Usage.Pods
	})).
	Complete(c)
```

- **`SourceOn(on)`** enqueues every key the store holds, then each key that
  appears, disappears, or whose `on(Old)` and `on(New)` differ, compared
  semantically. `on` is pure and cheap; it runs twice on every change.
  `Source()` is `SourceOn` of the whole entry.
- **A controller reconciles the key of the store it wakes on.** When it acts
  on several objects that belong together, it wakes on the store whose entry
  carries them, as node hydration wakes on NodeClaims.
- **A store keyed by name or `types.NamespacedName` names its requests.** One
  keyed by anything else, such as `cluster.Cluster`, is a source only under
  `Wake`.
- **`state.Requires(stores...)`** holds a controller's workers until the stores
  it reads are synced, and enqueues nothing.
- **`state.Wake(sources...)`** folds every change into one empty request, for
  a controller that reconciles the whole cluster at once.
  **`state.WakeCounting(count, sources...)`** calls `count` for each change as
  it enqueues it, for a controller that skips work while nothing it wakes on
  changed, as disruption's consolidation does.
- **Metrics are built at scrape time, not reconciled.** A metrics controller
  registers a `metrics.Collector` whose collect reads its stores with
  `client.UnsafeDisableDeepCopy` on each scrape, and never polls; a duration
  is observed by a subscriber on the change that ends it.
- **`RequeueAfter` is for wall-clock deadlines only,** such as a TTL or a
  timeout, marked `//storelint:deadline <reason>`. A controller reacting to
  state wakes through a source. A controller that must poll something no
  store holds marks it `//storelint:poll <what it polls>`.
- **A subscriber is cheap and non-blocking.** It updates a counter, enqueues
  or `Touch`es. Work a change triggers runs in a controller that watches a
  source.
- **A controller's local `Derive` narrows the derived store closest to what it
  needs**, such as node usage for pod counts per node, rather than re-folding
  an object store.

## Startup

```go
s := cluster.New(ctx, clk, mgr.GetClient(), cloudProvider) // the overlay client, which overlays cluster.Kinds
defer s.Close()
lo.Must0(s.Register(mgr))
op.WithControllers(ctx, controllers.NewControllers(ctx, mgr, ..., s, ...)...).Start(ctx)
```

1. **`cluster.New` constructs the shared stores,** their `Derive`s and their
   intent handles.
2. **`State.Register(mgr)` starts every shared store, once**, as a manager
   runnable.
3. **A controller's `Register`** builds the `Derive`s only that controller
   reads, held in typed fields of the controller, and declares its sources and
   `Requires`. A `Derive` moves to `pkg/cluster` when a second reader appears.
4. **`WaitForSync` orders startup.** Each object store applies its source once synced and marks itself
   synced, and each controller's workers wait on its sources and `Requires`.
   No constructor outside `pkg/state` and `pkg/cluster` registers a `Derive`
   or a subscriber.
5. **`State.Close` runs once the manager's `Start` returns.** Every store
   `cluster.New` built stops following the overlay, and every lapse and expiry
   timer stops.

- **A controller declares the stores it reads with `state.Requires` in
  `Register`.** A store's sync is one-way and bounded by its first List, so
  gating the start is enough.
- **An unbounded condition is waited on in `Reconcile`, and woken on.** "No
  NodeClaim is launching" may never hold, so the provisioner and disruption
  check `NodeClaimsLaunched`, and disruption wakes on
  `NodeClaims.UnlaunchedSource`.
- **Every kind cluster state follows reads through the overlay.** The
  operator overlays `cluster.Kinds`, and `cluster.New` reads each kind's
  newest versions through the overlay client it's given.
- **A suite's cluster state follows the envtest cache and the overlay**, as
  under the manager. See [Testing](#testing).

## Placement

- **A derived field lives with whoever defines its meaning.**
- **State two or more controllers read lives in `pkg/cluster`.** State one
  controller reads lives with that controller.
- **Policy lives with its owner:** repair matching in `node/health`,
  disruptability and budgets in `disruption`. `pkg/cluster` holds state, and
  its node functions do no I/O and carry no policy.
- **`pkg/state` holds the kind-free primitives,** and `pkg/cluster` holds one
  file per object or synthetic object.

## The node layout

```go
type NodeClaimEntry = state.Entry[*v1.NodeClaim, NodeClaimDerived, NodeClaimIntent]
type NodeClaimDerived struct {
    Node                *corev1.Node // the Node whose owner reference names this NodeClaim; nil while in flight
    Usage               NodeUsage    // that Node's entry's usage
    RepairPolicyMatches []repair.Match
}
type NodeEntry = state.Entry[*corev1.Node, NodeDerived, struct{}]
type NodeDerived struct {
    Usage               NodeUsage
    RepairPolicyMatches []repair.Match
}
```

- **A node's identity is its `NodeKey{NodeClaim, Node}`.** Exactly one field
  is set: a managed node keys by its NodeClaim's name from the moment the
  NodeClaim is created, before it has a providerID or a Node, until its entry
  drops; an unmanaged node keys by its Node's name. The providerID is never a
  key: `NodeClaimsByProviderID` indexes it.
- **NodeClaims are the managed nodes, launched or in flight:** an object store
  keyed by name. A NodeClaim's `Derived` is the Node its owner reference
  links, that Node's entry's usage and its repair policy matches, until repair
  derives its own; its `Intent` is the deletion mark, the nomination and the
  buffer pods. Disruption, static provisioning, limits and nominations read
  no other node store; a node's pods they read from `PodsByNode`.
- **Nodes are every Node:** an object store keyed by name. A Node's `Derived`
  holds the usage of the pods bound to it, from the pod store keyed by
  `spec.nodeName`, its CSINode's drivers (`NodeUsage.CSIDrivers`) and its
  repair policy matches. It has no NodeClaim. It's the source of each NodeClaim
  entry's `Derived`, and only the scheduler, the metrics and the controllers
  keyed by Node, such as termination and node health, read it.
- **Two `Derive`s join them:** pods → the Node entry's `Usage`, keyed by
  `spec.nodeName`; the Node entry → its NodeClaim entry's `Derived`, keyed by
  the Node's owner reference, once cluster state tracks the Node. A pod event
  writes the Node entry, then its NodeClaim entry, and the `Derive`s from the
  NodeClaim store recompute on the NodeClaim's change, comparing an equal
  contribution away.
- **From a Node to its NodeClaim, follow the owner reference:**
  `nodes.Get(pod.Spec.NodeName)`, `cluster.OwnerNodeClaim(node)`, then
  `nodeClaims.Get`. No field or index holds the link.
- **`NodeView` is how a reader sees a node's facts:** a concrete struct of
  three pointers (NodeClaim, Node, usage), passed by value, from
  `cluster.ViewOfNodeClaim(entry)` or `cluster.ViewOfNode(entry)`, since Go
  defines no methods on an alias of another package's instantiated generic
  type. Its methods are `Key`, `NodeName` (the Node's name, `""` while in
  flight, which pods' `spec.nodeName` compare against; not an identity),
  `Managed`, `Labels`, `Annotations`, `Taints`, `Capacity`, `Allocatable`,
  `Available`, `PodRequests`, `PodLimits`, `DaemonSetRequests`,
  `DaemonSetLimits`, `DisruptionCost`, `HostPortUsage` and `VolumeUsage` (the
  view's own copies, which a scheduler adds simulated pods to),
  `Initialized`, `RebootInProgress`, `ProviderID`, `HostName`, `Name` (for
  logs) and `NodeClaim()`, `Node()` and `Usage()` (the store's, read-only).
  Labels and taints come from the NodeClaim until the Node registers; capacity
  and allocatable from the NodeClaim, in place of the Node's zero quantities,
  until it initializes. Capacity quotas read labels through the same helper.
- **What only a managed node has are functions of its `NodeClaimEntry`:**
  `Launched`, `Registered`, `InFlight` (`Derived.Node == nil`), `Deleting`
  (the deletion mark, a deletion timestamp or a terminating instance),
  `Nominated(e, now)` and `HasBufferPods`. They do no I/O and carry no policy.
  Anything that addresses a node, such as a nomination, the deletion mark or a
  disruption candidate, uses the managed node's entry: Karpenter holds no
  intent on unmanaged nodes.
- **An unmanaged Node has no owning NodeClaim and no `karpenter.sh/nodepool`
  label**, and is read from the Node store through `Nodes.Unmanaged(opts...)`,
  a typed index of the Node store.
- **A new Node is registering until its owner reference is set.** It carries
  its `karpenter.sh/nodepool` label from kubelet registration, a provider
  contract, so it's neither unmanaged nor a second node: its NodeClaim's entry,
  in flight, already represents it, and the machine counts once. Pods bound
  meanwhile count on the Node's entry and join the NodeClaim's entry once the
  owner reference lands.
- **The scheduler's existing capacity** is each launched NodeClaim's node that
  isn't `Deleting`, and each unmanaged Node that isn't being deleted, read
  without a copy (`Provisioner.ExistingNodes`); the deleting ones' pods
  reschedule.
- **The cluster-wide counts are the synthetic `Cluster{}` entry**, derived
  from the NodeClaim store.

## Lint

`hack/storelint` reports:

- a write of a `Derive` or `Claims` target (`Store.Feed`, `FeedAll`) outside
  `pkg/state`;
- a `Subscribe`, `Derive`, `DeriveObject`, `NewIndex`, `DependsOn` or store
  constructor in a `New*` function outside `pkg/state` and `pkg/cluster`;
- a store built anywhere but `pkg/state` itself, `pkg/cluster`, a
  `Register(context.Context, manager.Manager) error` method or a test;
- a shared store added to a manager anywhere but `cluster.State.Register`;
- a `RequeueAfter` in a controller that reads stores, unless the line carries
  `//storelint:deadline <reason>` or `//storelint:poll <what it polls>`;
- a `Contribution` type without a `statetest.ExpectContribution` test in its
  package;
- a map or slice field of a store's fields without a `// bounded:` comment;
- a client write of, or `StatusConditions()` on, an object a store holds: a
  field of a `pkg/cluster` value, such as a NodeClaim entry's `Derived.Node`;
  the `Object` of an entry a read returned with `client.UnsafeDisableDeepCopy`,
  a `pkg/cluster` function returned, or a function was passed; a `NodeView`'s
  `NodeClaim()` or `Node()`; or a variable holding one;
- a `client.MatchingFields` passed to a store's or an index's `List`, which
  panics on it;
- outside `pkg/cluster`, a node fact `NodeView` owns read straight from a node
  entry: any field of a NodeClaim entry's `Derived.Node`, and the `Labels`,
  `Spec.Taints`, `Status.Capacity` or `Status.Allocatable` of a
  `NodeClaimEntry`'s or `NodeEntry`'s `Object`. A NodeClaim's own spec and
  status, such as its `NodeClassRef` or conditions, are read from the entry's
  `Object`.

## Adding state

1. Find the source: an API object (an object store), a Go type a feed reports
   or one built from the key (a synthetic store), or another store's entries
   (a `Derive` onto an entry).
2. Put the fields on the entry of the object they describe, and comment each
   with what it holds, which store feeds it, and when a source stops counting.
3. Write the contribution type, its `Update`, `Delete` and, for maps, `Equal`.
4. `Derive` it, and check the laws with `statetest`.
5. Read it through its store's `Reader`, or a node's through its `NodeView`.

## Testing

Tests are Ginkgo specs, one `RunSpecs` suite per package.

- **A suite runs against the envtest API server, its cache and the overlay**,
  as the manager does: `test.NewEnvironment` builds them.
- **Each spec builds its own cluster state**, in `BeforeEach`, with
  `cluster.New(ctx, env.Clock, env.Overlay, cloudProvider)` and
  `DeferCleanup(s.Close)`, and builds there the controllers under test that
  take it. A new State reads what the API server holds, so no spec sees
  another's intent, claims or derived fields.
- **The controller under test gets `env.Overlay`**, as it gets the manager's
  client in production: its writes reach the stores before they return.
- **The suite writes through `env.Client`**, as a third party: it writes to the
  API server directly, and each write returns once the cache and every store
  following the overlay hold it, or its removal. `ExpectApplied`,
  `ExpectDeleted` and every other write through it wait the same way. A spec
  reconciles only the controller under test, and asserts on what the stores
  show. A suite has no other sync points; a spec that needs one is missing a
  helper.
- **`env.APIServer`** reads back a controller's write to a kind the overlay
  doesn't record.
- **`pkg/state`'s primitive specs and benchmarks drive stores through
  `statetest.Objects`**, filled with `Put` and `Remove`: `cluster.WithSources`
  for cluster state. `pkg/cluster` and controller specs write through
  `env.Client`, except where envtest can't produce the state or makes the run
  count impractical:
  - the scheduling properties harness, which runs thousands of generated cases
    per run;
  - "should report how long cluster state has been unsynced while its stores
    are" in metrics/node, and "Node stores should be at least as strongly
    synced as the List check once its stores sync with no unlaunched
    NodeClaims" in `pkg/cluster`, and "doesn't back off when the
    CapacityQuotas haven't synced" in disruption's queue, which need sources
    that never sync.
- Drive an object store through its source, then assert on the target's `Get`,
  as `store_test.go` does.
- Cover moving between keys ("Derive should move contributions between keys"),
  an older version arriving after a newer one ("ObjectStore should keep the
  newer version when an older read commits late"), the dispatch ("Derive
  should dispatch Update and Delete"), `Derived` waiting for its object
  ("ObjectStore should hold Derived for a key until its object arrives"), and
  a join ("Join should put a Node's name on its NodeClaim's entry").
- Check a field against a recount of its sources after many concurrent changes,
  as "Derive should converge under concurrent changes" and "NodePool should
  keep fields that match a recount of their definitions" do.
- Check each contribution's laws with `statetest.ExpectContribution(GinkgoT(), ...)`.
- Check a removed `Derive` leaves nothing behind ("Derive should leave nothing
  behind in its target once removed"), and a closed store follows nothing and
  waits on no clock ("Close should stop following the source, the Derives into
  the store and its timers, and leave its entries"; "Close should leave no
  subscribers on the overlay and no timers on the clock" in `pkg/cluster`).
- Test intent as "Intent should set, clear and drop with its entry" and
  "Intent should lapse WithExpiry once its clock steps past the TTL, and
  notify the lapse" do, and a feed as "FedStore should replace a set of
  objects in one write" does.
- Test reads as "Reader should deep-copy the object unless
  UnsafeDisableDeepCopy" does.
- Start a source on a workqueue and assert on what it queues, as "Store Source
  should enqueue only keys that appear, disappear, or whose narrowed fields
  change, with SourceOn" does, and as each controller's Wakes spec does for
  its `SourceOn` (disruption's `wakes_test.go`, termination's and hydration's
  "Wakes" specs).
- Test claims as `claims_test.go` does: race reservers that create and delete
  objects against a recount of what objects count and live claims hold, and
  check that what objects count stays within the limit ("Claims should never
  exceed the limit or count an object zero times"). Collect a goroutine's
  failures and assert on them after it joins; a Gomega failure inside a
  goroutine holding a lock can hang the spec.
- Drive a Node's registration window as "Registering window" in
  `pkg/cluster` does.
