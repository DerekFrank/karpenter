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
  `Store` and `Reader` (`NewObjectStore`, `NewObjectStoreBy`, `NewFedStore`,
  `NewSyntheticStore`), `Derive` and `DeriveObject` (`Derivation`,
  `Contribution`, `Equaler`), `Intent` and `Intended`, `Claims`, `Accessor`,
  `StoreAccessor`, `View`, and the sources `Reader.Source`, `Reader.SourceOn`,
  `Requires`, `Wake`, `WakeCounting` and `After`.
- **`pkg/state/statetest`** checks a `Contribution`'s laws in tests.
- **`pkg/state/overlay`** holds `overlay.Client`, the read-after-write client:
  its reads include its own writes before the informer delivers them.
- **`pkg/cluster`** holds the cluster's state built from those primitives, one
  file and one type per object or synthetic object: `nodeclaim.go`
  (`NodeClaims`, and the `Deletion`, `Nomination` and `BufferPods` handles),
  `node.go` (`Nodes`, `CSINodes`), `nodeusage.go` (the pod usage on a Node's
  entry), `pod.go` (`Pods`), `pvc.go` (`PVCs`), `daemonset.go`
  (`DaemonSets`), `cluster.go` (the synthetic `Cluster{}` and its counts),
  `nodepool.go` (`NodePoolState` and the `NodePools` accessor) and
  `capacityquota.go`. `nodestate.go` is the node accessor, which combines a
  NodeClaim's entry with its Node's. `state.go` composes them.
- **`cluster.State`** holds every accessor, the stores behind them and the
  intent handles. `cluster.New` builds it once, where the operator is wired
  (`kwok/main.go`), and passes it to `controllers.NewControllers`, which gives
  each controller the accessors it reads and the handles it writes: the
  disruption queue gets `Deletion`, and the provisioner gets `Nomination` and
  `BufferPods`. See [Startup](#startup).
- **`pkg/controllers/state`** holds `state.Cluster`, a façade over
  `cluster.NodeState`, `cluster.NodePoolState` and `cluster.PodScheduling`,
  with the wait on unlaunched NodeClaims, the pod scheduling telemetry and the
  consolidation state. Its informer controllers keep what takes I/O to read:
  PVC drivers and cost.
- **Imports point one way:** controllers import `pkg/cluster`, which imports
  `pkg/state`. depguard (`.golangci.yaml`) enforces it, and freezes the
  importers of `pkg/controllers/state` with a list that only shrinks.
  `hack/storelint`, run by `make verify`, enforces the rules in [Lint](#lint).

## Stores

There are two kinds of store. They differ only in where the object comes from:

| Store | Made with | Object | Written by |
|---|---|---|---|
| object store | `NewObjectStore[S, D, I](kubeClient, informers)`, `NewObjectStoreBy(kubeClient, informers, key)` | an API object of kind `S`, by namespaced name or the key `key` returns | the informer and the overlay's writes, or `Observe`, `Forget` and `Resync` |
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
- **An object store orders each object's versions** by UID and
  resourceVersion: an older version arriving after a newer one is dropped, a
  delete leaves a tombstone until the informer's delete, and a newer object
  under a name another UID holds replaces it.
- **With a kubeClient, an object store is a manager runnable.** `Start` lists
  the kind (the list kind is the GVK plus `List`, from the scheme), then
  follows the informer and the `overlay.Client`'s writes. The overlay calls its
  write handlers before a write returns, so the store holds a write as soon as
  it returns.
- **An object store following an informer holds the informer's own objects.**
  A copy from the first List or a write holds an object's version only until
  the informer delivers its own object for it. A caller that feeds a store
  passes the cache's objects (`client.UnsafeDisableDeepCopy`).
- **Without a kubeClient, a caller feeds an object store.** `Observe(obj)`
  applies a version seen elsewhere, `state.Forget(store, name)` removes the
  object named `name`, and `state.Resync(store, objs, scope)` makes `objs` the
  objects among those named in `scope`, which heals missed events. `scope`
  comes from an index, such as `Derivation.Sources(key)`.
- **A feed replaces a set of entries in one write.** `FeedAll(in, objects)`
  makes `objects` the store's objects among the keys `in` accepts, such as one
  NodeClass's instance types, so a reader sees the old set or the new one.
- **`Reader`** is a store's read-only handle: `Get`, `List`, `Subscribe`,
  `Synced`, `Source` and `SourceOn`. `pkg/cluster` exports Readers.
- **`Synced`** is true once an object store following an informer has applied
  its first List, and every `Derive` into a store has applied its source. A
  store fed by a caller is synced from the start.
- **`Subscribe(fn)`** calls `fn` with each `Change{Key, Old, New, Existed,
  Exists}` after the lock is released, and returns a func that stops it.
- **`Clear`** empties a store in place and notifies each removal, so
  subscribers stay subscribed.
- **`Touch(key)`** re-notifies a key without changing it, so the `Derive`s
  from it recompute.

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
which no longer counts against its NodePool. A singleton synthetic object
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
- **A sample is chosen at read time from the Derivation's Sources**, such as a
  DaemonSet's newest pod (`DaemonSets.GetDaemonSetPod`), so the contribution
  is a count that keeps the laws.
- **`Update` replaces `prev` in one pass**, copying each map once: it merges
  the new value into a copy, then subtracts `prev` from that copy.
- **A contribution returns new fields and leaves the ones it gets intact.** It
  copies a map or slice before changing it. `Update` and `Delete` run under
  the target's lock, so they never block and never do I/O.
- **A contribution holding maps or quantities implements `Equal`**, so an
  unchanged contribution costs a comparison and nothing else.
- **Fields are small, and none is a collection that grows with the cluster.**
  A map or slice field carries a `// bounded: <what bounds it>` comment, such
  as "pods per node". To relate many objects to one key, use the key's
  `Derive` sources, which are indexed in place, as
  `NodePoolState.LaunchedNodes` and `StoreAccessor.IndexField` do.
- **A join is a `Derive` onto an entry.** The Node a NodeClaim's owner
  reference links is the NodeClaim entry's `Derived.Node`, contributed from
  the Node store (`linkedNode`).
- **Every derived field has a reader.** A field with no reader is deleted with
  its `Derive`. A `Derive` computes a field a reader reads; waking a controller
  on part of an entry is `SourceOn`'s job.

### Keeping values pure

`key` and `value` run on whatever goroutine changed the source, with no
request behind them.

- **They read the source entry**, and configuration captured at construction,
  such as the options in the context `cluster.New` takes, which decide a pod's
  disruption cost.
- **A value that depends on another store's entries** is kept current with an
  index, a `Derive` keyed by the dependency whose `Sources` are the
  dependents, plus a subscriber that `Touch`es the dependents when the
  dependency changes. A pod's volume usage reads its PVCs' drivers from
  `PVCs`, which index the pods that mount PVCs by namespace and `Touch` them
  when a driver resolves (`pvc.go`). A NodeClaim's NodePool capacity reads its
  Node's entry, and a Node's change `Touch`es the NodeClaim its owner
  reference names.

## Intent

- **Intent is a field group on the object's entry**, read with the object, and
  each field is a `state.Intended[V]{Value, Until}`.
- **Each intent has a typed handle**, `state.NewIntent(store, field)`, with
  `Set`, `Clear`, `Get`, and `Update` for writers that race. `pkg/cluster`
  wraps each in a type its one writer holds: `Deletion` (`Mark`, `Unmark`) for
  the disruption queue, `Nomination` (`Nominate`) and `BufferPods`
  (`Replace`) for the provisioner, and `Registrations` (`Record`) for the
  NodeClaim lifecycle controller, whose recorded launch outcomes the
  registration health controller reads to set `NodeRegistrationHealthy`. A
  controller without the handle can't write the intent.
- **`Set` on an entry that doesn't exist does nothing**, since intent about an
  object the store doesn't hold would outlive it.
- **`WithExpiry(clock, ttl)`** makes a value lapse `ttl` after it's set, or at
  the time `SetUntil` gives. A lapse is an event: the handle clears the value
  from a timer on the clock at the earliest lapse, and the entry notifies. The
  timer stops once nothing can lapse. A suite steps its fake clock to lapse
  intent. `Intended.At` reads a value as of a time.

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
np, err := nodePools.Get(ctx, client.ObjectKey{Name: name}) // nodePools is a state.Accessor
np.Object.Spec.Limits                                     // the NodePool, through the client
np.Derived.Usage                                          // fields derived about it
kubeClient.Status().Patch(ctx, np.Object, client.MergeFrom(stored))
```

- **An accessor reads entries and returns a `state.View{Object, Derived,
  Intent}`.** The object is a deep copy, with `client.UnsafeDisableDeepCopy` as
  the opt-out. `Derived` and `Intent` are shared and immutable.
- **`state.StoreAccessor`** reads an object store, and serves `List` selectors
  and field indexes as the client does, with an `IndexField` for each index.
  **`state.Accessor`** reads objects through the client, for a kind whose
  readers need the client's reads, such as NodePools.
- **A read combines one lookup per store per object.** The node accessor reads
  a NodeClaim's entry and the entry of the Node it names. A value aggregated
  across objects is a derived field.
- **Readers tolerate lag between stores.** An object and the fields derived
  from other stores may be from different moments, and converge.
- **An object a store holds is read-only.** A store following an informer holds
  the informer's own objects, so a `StateNode`'s Node and NodeClaim, an
  entry's object and a read with `client.UnsafeDisableDeepCopy` are the
  cache's. Writing one, even through the client, writes the cache: `Create`,
  `Update`, `Patch`, `Status().Patch` and a `Delete` that leaves the object in
  storage decode the server's response into the object they're passed. Write
  a `DeepCopy`, and read conditions with
  `StatusConditions(status.WithObservedOnly())`.
- **Read an object that has an accessor through it, and write it through the
  client.** A controller that reads through accessors takes a client of
  `client.Writer` and `client.StatusClient`.

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
- **`state.After(delay, sources...)`** batches each request's changes for
  `delay`, for a watcher such as metrics.
- **`RequeueAfter` is for wall-clock deadlines only,** such as a TTL or a
  timeout. A controller reacting to derived values wakes through a source.
- **A subscriber is cheap and non-blocking.** It updates a counter, enqueues
  or `Touch`es. Work a change triggers runs in a controller that watches a
  source.
- **A controller's local `Derive` narrows the derived store closest to what it
  needs**, such as node usage for pod counts per node, rather than re-folding
  an object store.

## Startup

```go
s := cluster.New(ctx, clk, kubeClient, mgr.GetCache(), cloudProvider)
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
4. **`WaitForSync` orders startup.** Each object store lists and marks itself
   synced, and each controller's workers wait on its sources and `Requires`.
   No constructor outside `pkg/state` and `pkg/cluster` registers a `Derive`
   or a subscriber.

- **A controller declares the stores it reads with `state.Requires` in
  `Register`.** A store's sync is one-way and bounded by its first List, so
  gating the start is enough.
- **An unbounded condition is waited on in `Reconcile`, and woken on.** "No
  NodeClaim is launching" may never hold, so the provisioner and disruption
  check `NodeClaimsLaunched`, and disruption wakes on
  `NodeClaims.UnlaunchedSource`.
- **A suite's cluster state has no informers.** It reconciles the informer
  stand-ins in `pkg/test/expectations` (`NodeClaimInformer`, `NodeInformer`,
  `PodInformer`), which show cluster state an object as the informer would.

## Placement

- **A derived field lives with whoever defines its meaning.**
- **State two or more controllers read lives in `pkg/cluster`.** State one
  controller reads lives with that controller.
- **Policy lives with its owner:** repair matching in `node/health`,
  disruptability and budgets in `disruption`. `pkg/cluster` and `StateNode`
  hold state.
- **`pkg/state` holds the kind-free primitives,** and `pkg/cluster` holds one
  file per object or synthetic object.

## The node layout

- **Nodes are an object store keyed by name.** A Node's `Derived` holds the
  usage of the pods bound to it, from the pod store keyed by `spec.nodeName`,
  its CSINode's drivers, and its repair policy matches.
- **NodeClaims are an object store keyed by name.** A NodeClaim's `Derived` is
  its linked Node's name, contributed from the Node store by owner reference,
  and its `Intent` is the deletion mark, the nomination and the buffer pods.
  The node accessor combines the NodeClaim's entry with the Node's entry it
  names, so a pod event changes only the Node's entry.
- **A managed Node is read through its NodeClaim's entry.** An unmanaged Node
  has no owning NodeClaim and no `karpenter.sh/nodepool` label, and is read
  from the Node store, through the `Cluster{}` entry's unmanaged index.
- **A new Node is registering until its owner reference is set.** It carries
  its `karpenter.sh/nodepool` label from kubelet registration, a provider
  contract, so it's neither unmanaged nor a second node: its NodeClaim's entry
  already represents it. Pods bound meanwhile count on the Node's entry and
  join the NodeClaim's node once the owner reference lands. The providerID is
  no store's key.
- **The cluster-wide counts are the synthetic `Cluster{}` entry**, derived
  from the NodeClaim and Node stores.

## Lint

`hack/storelint` reports:

- a write of a `Derive` or `Claims` target (`Store.Feed`, `FeedAll`, `Observe`,
  `Clear`) outside `pkg/state`;
- a `Subscribe`, `Derive`, `DeriveObject` or store constructor in a `New*`
  function outside `pkg/state` and `pkg/cluster`;
- a store built anywhere but `pkg/state` itself, `pkg/cluster`, a
  `Register(context.Context, manager.Manager) error` method or a test;
- a shared store added to a manager anywhere but `cluster.State.Register`;
- a `RequeueAfter` in a controller that reads stores, unless the line carries
  `//storelint:deadline <reason>`;
- a `Contribution` type without a `statetest.ExpectContribution` test in its
  package;
- a map or slice field of a store's fields without a `// bounded:` comment;
- a client write of, or `StatusConditions()` on, an object a `pkg/cluster`
  value holds.

## Adding state

1. Find the source: an API object (an object store), a Go type a feed reports
   or one built from the key (a synthetic store), or another store's entries
   (a `Derive` onto an entry).
2. Put the fields on the entry of the object they describe, and comment each
   with what it holds, which store feeds it, and when a source stops counting.
3. Write the contribution type, its `Update`, `Delete` and, for maps, `Equal`.
4. `Derive` it, and check the laws with `statetest`.
5. Read it through an accessor, or through the node accessor's combined read.

## Testing

Tests are Ginkgo specs, one `RunSpecs` suite per package. A suite's setup
writes objects through the kubeClient and feeds the object stores through the
informer stand-ins; only the controller under test uses the accessors.

- Drive an object store with `Observe` and `Forget`, then assert on the
  target's `Get`, as `store_test.go` does.
- Cover moving between keys ("Derive should move contributions between keys"),
  an older version arriving after a newer one ("ObjectStore should keep the
  newest version across writes and the informer"), the dispatch ("Derive
  should dispatch Update and Delete"), `Derived` waiting for its object
  ("ObjectStore should hold Derived for a key until its object arrives"), and
  a join ("Join should put a Node's name on its NodeClaim's entry").
- Check a field against a recount of its sources after many concurrent changes,
  as "Derive should converge under concurrent changes" and "NodePool should
  keep fields that match a recount of their definitions" do.
- Check each contribution's laws with `statetest.ExpectContribution(GinkgoT(), ...)`.
- Check a removed `Derive` leaves nothing behind ("Derive should leave nothing
  behind in its target once removed").
- Test intent as "Intent should set, clear and drop with its entry" and
  "Intent should lapse WithExpiry once its clock steps past the TTL, and
  notify the lapse" do, and a feed as "FedStore should replace a set of
  objects in one write" does.
- Read through the accessors as `accessor_test.go` does.
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
  `pkg/controllers/state` does.
