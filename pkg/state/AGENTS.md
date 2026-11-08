# State

Karpenter keeps three kinds of state about the objects in a cluster. Each kind
has its own source of truth and its own rules.

| | Facts | Derived fields | Intent |
|---|---|---|---|
| What | the API objects, and joins of objects that are one thing (a Node and its NodeClaim) | values computed from other objects: a NodePool's NodeClaim usage, the pod requests on a Node | what Karpenter has decided to do and is still doing: a node reserved against a limit for a NodeClaim it is creating |
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
  `View` and `Claims`.
- **`pkg/state/statetest`** checks a `Contribution`'s laws in tests.
- **`pkg/state/overlay`** holds `overlay.Client`, the read-after-write client:
  its reads include its own writes before the informer delivers them.

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

A join is several objects that are one thing, such as a Node and its NodeClaim
joined on providerID. `Join(key, set)` is one kind's part: `key` is the join
key of an object, and `set` stores the object in the fields, or a nil object
once it leaves the key. One store has one lock, so a join reads as one version.

- **Intent about a join lives in its entry**, set with `Store.Update`, such as
  a node's deletion mark. `set` drops the intent once no part is left
  (`nodeFields.orNothing` in cluster state), so a gone node leaves nothing
  behind.
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
- **A contribution that keeps a sample instead of counting**, such as a
  DaemonSet's newest pod (`newestPod`), has a `Delete` that doesn't undo
  `Update`. Say so on the type, and leave it out of the law check.

### Keeping values pure

`key` and `value` run on whatever goroutine changed the source, with no request
behind them.

- **Read only the source's fields.** A value that depends on another object
  belongs in that object's own store, combined at read time. A pod's volumes
  are the PVC keys it mounts (from the pod) and each PVC's driver (from the
  PVC's store); `stateNode` groups the node's PVCs by driver when it builds a
  StateNode.
- **Capture configuration at construction**, such as the options in the
  context `NewCluster` takes, which decide a pod's disruption cost.
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
- **`Get` and `List` return a `state.View{Object, Derived}`.** `Derived` is
  read from its stores lock-free, and is shared and immutable: copy one to
  change it.
- **Combine stores at read time**, as `stateNode` combines a node, its pods'
  usage, its CSI drivers and its PVCs' drivers. Each store is from its own
  moment, so a reader tolerates the lag: a pod that moves between nodes can
  briefly count on both or neither.
- **The client type enforces the rule.** A controller that reads through
  accessors takes a client of `client.Writer` and `client.StatusClient`, so a
  read through the client is a compile error.
- **Write from derived fields once they're synced.** Check `Store.Synced`
  first.

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
- Test claims as `claims_test.go` does: race reservers that create and delete
  objects against a recount of what objects count and live claims hold, and
  check that what objects count stays within the limit
  (`TestClaimsNeverExceedTheLimitOrCountAnObjectZeroTimes`).
