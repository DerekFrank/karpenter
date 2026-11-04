# State

Karpenter keeps three kinds of state about the objects in a cluster. Each kind
has its own source of truth, so each has its own rules.

| | Facts | Derived fields | Intent |
|---|---|---|---|
| What | the API objects, and joins of objects that are one thing (a Node and its NodeClaim) | values computed from other objects: a NodePool's NodeClaim usage, the pod requests on a Node | what Karpenter has decided and nothing else records: reservations, nominations, marks |
| Source of truth | the API server | the API server, through the objects they're computed from | Karpenter's memory |
| Consistency | an object, or a join, reads as one version | may lag the object and each other, but converges | not covered here yet |

This file covers facts and derived fields. Intent is still kept by hand in
cluster state.

## Reading state

Read an object that has a state accessor only through the accessor. Use the client
only to write it.

```go
np, err := nodePools.Get(ctx, client.ObjectKey{Name: name})
np.Object.Spec.Limits // the NodePool, from the cache
np.Derived.Usage      // fields Karpenter keeps about it
kubeClient.Status().Patch(ctx, np.Object, client.MergeFrom(stored))
```

- **A state accessor** is a `store.Reader` for one kind, such as
  `state.NodePools`. `Get` and `List` return `store.Object{Object, Derived}`:
  the API object read through the client, so it is read-after-write with the
  overlay, and the derived fields, read from their stores without a lock.
- **`Object` is the API object itself**, so write, watch, and record events for
  it as usual. `Derived` never reaches the API server.
- **Derived values are shared.** Never modify one; copy it first.
- **Make the rule hold in code.** Give a controller that reads through
  accessors a client it can only write with (`client.Writer` and
  `client.StatusClient`), so that a read through the client doesn't compile.

## Keeping derived fields

Every mutation updates the derived fields, so a read never computes. Don't
recompute a field from the cache when a reader asks for it, even lazily behind a
cache that events invalidate: a key covering many objects that change between
reads makes each read O(objects). A static NodePool scaling up reads its
NodeClaim counts once per NodeClaim it creates, so recomputing made a
20,000-NodeClaim scale-up quadratic, 50x slower than updating the counts on
each change.

- **One store per set of derived fields**, `Store[K, F]`, keyed by the object
  the fields describe, such as the NodePool's name or the Node's name. `F` lists
  the fields.
- **A `Contributor` per kind that feeds them.** It names the key each object
  contributes to (`target`), what it contributes (`contribution`), and how a
  change folds into the fields (`apply(fields, prev, next)`). Start it as a
  runnable so it follows the informer and the client's writes; `Apply` and
  `Forget` feed objects seen elsewhere.
- **`Derive` feeds fields from another store's fields**, such as Node capacity
  summed onto its NodePool.
- **Read the store in the accessor's `derive`**, so readers get the field.
- Don't add a map to cluster state, or a loop that recomputes an aggregate on
  each call. Lookups of objects by a value derived from one object are field
  indexes instead; see `pkg/state/index/AGENTS.md`.

## Joins

A join is several objects that are one thing, such as a Node and its NodeClaim,
which cluster state joins on providerID. Its parts must read together, so they
live in one store under one key, with a Contributor per kind contributing the
object itself. One store has one lock, so a joined object reads as one version.
Derived fields of a join, such as the pods' usage of a Node, stay in their own
stores and may lag the join.

Never coordinate locks across stores to make derived fields consistent with each
other or with the object. A reader that combines them must tolerate a lag, as
readers of cluster state already do: a pod that moves between nodes can briefly
count on both or neither.

## Adding a derived field

- Add the field to its store's fields struct and to the accessor's `Derived`.
  Comment what it holds, which kind feeds it, and when an object stops counting.
- Make `target` a pure function of the object, since a contribution moves only
  when the object changes. Make `contribution` one too where possible. A
  contribution that reads other objects, such as a pod's PVCs, goes stale until
  the object itself changes; say so on the field.
- Make `apply` return new fields and never modify the ones it gets. Copy any
  map or slice before changing it. `apply` runs under the store's lock, so it
  must not block or do I/O.
- Read the condition or field you need directly. Helpers that allocate, such as
  `StatusConditions()`, run on every event.

## Testing

Drive a Contributor with `Apply` and `Forget`, then assert on `Get`, as
`store_test.go` does. Cover moving between keys, an older version arriving
after a newer one, and the object going away. Check a field against a recount
of the objects it's computed from after many random changes. Read through the
accessor over a fake client, as `reader_test.go` does.
