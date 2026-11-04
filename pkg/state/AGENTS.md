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

- **`pkg/state`** holds the primitives, which know no object kinds: `Store`,
  `Contribute` (`Contributor`), `Derive`, `Accessor` (`NewAccessor`), `View`
  and `Claims`.
- **`pkg/state/overlay`** holds `overlay.Client`, the read-after-write client:
  its reads include its own writes before the informer delivers them.

## Reading state

Read an object that has a state accessor through the accessor. Write it through
the client.

```go
np, err := nodePools.Get(ctx, client.ObjectKey{Name: name}) // nodePools is a state.Accessor
np.Object.Spec.Limits                                     // the NodePool, from the cache
np.Derived.Usage                                          // fields Karpenter keeps about it
kubeClient.Status().Patch(ctx, np.Object, client.MergeFrom(stored))
```

- **A state accessor** is a `state.Accessor` for one kind. `Get` and `List`
  return a `state.View{Object, Derived}`.
- **`Object` is the API object**, read through the client, so it is
  read-after-write with the overlay. Write, watch and record events for it as
  for any object.
- **`Derived` is in-memory only**, read from its stores lock-free.
- **Derived values are shared and immutable.** Copy one to change it.
- **The client type enforces the rule.** A controller that reads through
  accessors takes a client of `client.Writer` and `client.StatusClient`, so a
  read through the client is a compile error.
- **Write from derived fields once they're synced.** Check the Contributor's
  `Synced` first.

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
    `overlay.Client`'s writes.
  - `Observe` feeds an object seen elsewhere, and `Forget` removes one known
    only by name.
  - `Recompute` refreshes a contribution that reads other objects.
  - `Resync` makes a listed set the contributors to one key, healing missed
    events.
  - `CompareWith` sets the equality used to skip unchanged contributions.
- **`state.Derive` feeds a store from another store's fields**, such as Node
  capacity summed onto its NodePool.
- **`Store.Update` sets fields Karpenter decides**, under the store's lock.
  `Store.Subscribe` notifies on each changed key.
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

## Claims

A claim holds a value against a key for an object a caller is about to create,
such as a node against a static NodePool's node limit, so a decision against a
limit counts what is already on its way. Claims live in the same store as the
derived fields they count with, so admission and the handoff to the counted
object happen under one lock and are atomic.

```go
reservations = r.cluster.NodePoolState.ReserveNodeCount(np.Name, nodeLimit, 1) // admits atomically against counted + held
nodeClaim.Reservation = reservations[0]                                         // the provisioner binds or releases it
```

- **`state.NewClaims(contributor, hold, counts, ttl, clock)`** makes claims
  against the keys a `Contributor` feeds. `hold` folds a held value into the
  fields, as `apply` does. `counts` returns whether an object's contribution
  counts what its claim held, such as launched capacity for a claim of the
  largest instance type it might launch.
- **`Claims.Reserve(key, n, value, admit)`** grants up to `n` claims, one at a
  time while `admit` accepts the fields with the claim held. Hold the most the
  object could count, so the limit holds while the object is on its way. A
  pessimistic hold costs a little headroom until the object counts; an
  optimistic one lets concurrent callers overshoot.
- **Every claim ends exactly once.** `Bind` it to the object once created; the
  `Contributor` releases it when it counts that object, at once if it already
  has, or when the object goes away first. `Release` it when the caller drops the
  create. A claim uncounted within its TTL (`state.ReservationTTL` for
  NodePools) expires, which bounds what a dropped claim or an ambiguous failure
  holds.
- **Each object counts at least once.** A claim ends when the `Contributor`
  counts its object, which can be after the create returns. An object may count
  twice for a moment, while it counts before its claim is bound; that only
  refuses a later admission, so it is safe.
- **A claim travels with its work**, such as `scheduling.NodeClaim.Reservation`
  on the NodeClaim a command creates. Every path that drops the work releases
  it, as the provisioner and the disruption queue do.
- **Claims live in memory**, and end with the process along with the work they
  are for. Reserve once `state.Cluster` has synced, as the static provisioning
  controller does, so admission counts every existing object.
- **Intent is a set of claims.** Each claim has an identity, so it ends exactly
  once.

## Adding a derived field

- Add the field to its store's fields struct and to the accessor's `Derived`
  struct. Comment what it holds, which kind feeds it, and when an object stops
  counting.
- `target` is a pure function of the object, since a contribution moves only
  when the object changes. Make `contribution` pure too where possible. A
  contribution that reads other objects, such as a pod's PVCs, is current as of
  the object's last change or `Recompute`; say so on the field.
- `apply` returns new fields and leaves the ones it gets intact. Copy any map or
  slice before changing it. `apply` runs under the store's lock, so it is
  non-blocking and free of I/O.
- `target`, `contribution` and `apply` run on every event. Read the condition or
  field directly from the object.

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
- Test claims as `claims_test.go` does: race reservers that create and delete
  objects against a recount of what objects count and live claims hold, and
  check that what objects count stays within the limit
  (`TestClaimsNeverExceedTheLimitOrCountAnObjectZeroTimes`).
