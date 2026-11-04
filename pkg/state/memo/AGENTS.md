# Derived Values

A value derived from objects, such as the pod requests on a Node or a NodePool's
NodeClaim count, is computed from the cache: look the objects up by a field
index and fold them. The informer cache, with the read-after-write overlay in
`pkg/operator/overlay`, is the source of truth. Don't keep a second copy of it
that each event updates by hand, such as a map of per-object contributions that
adds and subtracts on every change.

When a value is read too often to compute on each read, cache it in a `Memo`
and invalidate its key on every event that can change it. The memo is a cache
and nothing more: dropping it at any time is correct, and the next read
recomputes from the cache.

## The shape

- **`compute(ctx, key)`** lists through the client Karpenter writes through,
  by a field index (`client.MatchingFields`), and folds the objects into the
  value. Reading through that client means a value computed after a write sees
  the write. Pass `client.UnsafeDisableDeepCopy` when compute only reads.
- **`Invalidate(keys...)`** after every change that can change a value, once
  the change is readable through that client:
  - from the informer path: cluster state's `Update*`/`Delete*` calls, or an
    informer handler;
  - from Karpenter's own writes, with `overlay.Client.AddWriteHandler`.

  An object that moves between keys invalidates both the old and the new key.
  When a change can't name its key, such as a delete that only has a name,
  `Reset` the memo.
- **`Track` the objects a value is computed from**, instead of invalidating by
  hand. Its `inputs` func returns the key an object counts toward and a struct
  of the object's fields the value reads. The tracker hashes that struct and
  invalidates only when the key or the hash changes. So `Observe` on every
  event is idempotent, a condition-only update recomputes nothing, and `Forget`
  knows the key a deleted object counted toward.
  - List in the inputs every field compute reads that can change while the key
    stays the same. A field left out makes the memo silently stale, and no hash
    check can catch it. Test it: change the field and expect a recompute.
  - Use exported fields of values, never whole objects or `ObjectMeta`, which
    change on every event. Track panics at construction on inputs JSON can't
    hash exactly: unexported fields, interfaces, and times, which go in as
    `UnixNano`.
  - A compute that counts objects it finds in the cache `Claim`s them, so their
    deletes invalidate, and checks that a newly claimed object still exists.
- **Readers share values.** Never modify a value `Get` returns. Copy the parts a
  caller may change before handing them out.
- **Decided values aren't derived.** Reservations, nominations and marks are
  fields under a lock next to the memo. Where one can be persisted on the
  object, such as a condition, persist it, so that it becomes derived.

## Keeping a value incremental instead

Only when reads recompute over many objects on a hot path, because the key
covers many objects and they change between most reads. Measure it first, and
test the incremental value against a full compute.

For example, `NodePoolState` keeps static NodePools' NodeClaim counts
incrementally. Static provisioning reserves after each NodeClaim it creates, so
a memo recounted the whole NodePool on every reservation. That made a scale-up
quadratic: 50x slower at 20,000 NodeClaims.

## Testing

Change objects through the client, invalidate as the event path does, and
assert that `Get` equals a fresh compute. Cover an object moving between keys,
an object going away, and a change while a value is being computed.
