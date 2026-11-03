# Field Indexes

This package registers the field indexes Karpenter reads through its cache.

Every lookup of objects by a value derived from the object is a field index.
Don't add a map to cluster state, or a List-and-filter loop, for a lookup.
Register a field index and read it from the cache. The informer updates an index
together with its store, before it calls any event handler, so an index is never
behind the cache.

## Adding an index

- Add an exported function to `index.go` with the signature
  `func(ctx context.Context, indexer client.FieldIndexer) error`. It calls
  `indexer.IndexField(ctx, &<Kind>{}, "<field>", ...)` once for each field it
  indexes.
- Call it from `Setup`. Wrap indexes on Karpenter CRDs in
  `handleCRDIndexerError`, so they fail open when the CRD isn't installed.
- If test suites need the index, add a `<Name>FieldIndexer` wrapper in
  `pkg/test/environment.go` that calls the function, so tests and the operator
  share one definition.
- Name the field after the path it indexes, such as `spec.nodeName` or
  `status.providerID`. For a value computed from the object, use a descriptive
  name in the same dotted style.
- Make the index function depend only on its argument. Don't fetch another
  object inside it. Return `nil` to leave an object out of the index. Return
  several values when the object belongs to several keys.

## Reading an index

- List with `client.MatchingFields{"<field>": value}`. If more than one package
  reads it, add a `For<Thing>(value) client.ListOption` helper to
  `pkg/utils/<kind>`, like `nodeclaim.ForProviderID`.
- Don't use a label selector to look up objects on a hot path. The cache scans
  every object of the kind, or of the namespace, for a label selector. Index the
  label value instead.
