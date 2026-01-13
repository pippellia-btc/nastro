# nastro
A collection of plug&play, yet highly configurable databases for Nostr.

> [!WARNING] 
> **Deprecated / Archive only**  
> This library is no longer maintained as I no longer believe in the idea of interchangeable databases.  
> From now on, I will only develop opinionated custom databases, such as [nostr-sqlite](https://github.com/vertex-lab/nostr-sqlite).

## Installation

```
go get github.com/pippellia-btc/nastro
```

## Simple Abstraction

All databases fullfil the `Store` interface, which exposes easy to use methods for dealing with everything Nostr.  

```golang
type Store interface {
	Save(ctx context.Context, event *nostr.Event) error

	Delete(ctx context.Context, id string) error

	Replace(ctx context.Context, event *nostr.Event) (bool, error)

	Query(ctx context.Context, filters ...nostr.Filter) ([]nostr.Event, error)

	Count(ctx context.Context, filters ...nostr.Filter) (int64, error)
}
```

## Deep Customization

`nastro` is designed to be minimal by default, but easily extended to fit your needs.  
You can fine-tune both structure and behavior through functional options:

```golang
store, err := sqlite.New("relay.sqlite",
	sqlite.WithAdditionalSchema(mySchema),    	// custom tables or indexes
	sqlite.WithQueryBuilder(myBuilder),     	// override default SQL generation
	sqlite.WithFilterPolicy(myPolicy),     		// sanitize or reject filters
)
```

This setup provides clean separation between core logic and custom rules, so you can adapt behavior without forking or modifying internals.

## Requirements for Databases

These are the new requirements for new databases to be added to `nastro`:
1. fullfil the `Store` interface.
2. expose the underlying database client (if any).
3. allow customization of `EventPolicy`, `FilterPolicy` and other parameters with functional options.
