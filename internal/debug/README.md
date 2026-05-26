# Debug Package

Utilities for debugging and tracing code execution.

## Tracer

Hierarchical logging with indentation to visualize call stacks.

```go
// Create a tracer with a prefix (shown in output)
var trace = debug.NewTracer("fetcher")

// Log function entry/exit with automatic indentation
func (f *Fetcher) FetchNext() {
    defer trace.Enter("Fetcher.FetchNext", f)()  // Note: ()() is intentional

    trace.Println("processing doc=%s", docID)
}

// Disable/enable at runtime
trace.Disable()
trace.Enable()
```

**Output:**

```shell
fetcher:      > Fetcher.FetchNext (14000af1b90) | fetcher.go:42
fetcher:          processing doc=bae-123 | fetcher.go:44
```

**Global settings:**

```go
debug.PrefixWidth = 12       // Fixed width for prefix alignment
debug.ShowSourceLocation = true  // Show file:line at end of each log
```

## ResourceTracker

Tracks resource lifecycle to detect leaks.

```go
// Create a tracker
var iterTracker = debug.NewResourceTracker("iterator", trace)

// Track when creating
func newIterator(name string) *Iterator {
    iter := &Iterator{name: name}
    iterTracker.Track(iter, name)
    return iter
}

// Untrack when closing
func (iter *Iterator) Close() error {
    iterTracker.Untrack(iter, iter.name)
    return iter.inner.Close()
}

// Assert no leaks in tests
iterTracker.AssertEmpty()  // Panics if resources remain
```

**Output:**

```shell
fetcher:      >>> NEW iterator: docFetcher (addr: 14000af1b90). Total: 1
fetcher:      <<< CLOSED iterator: docFetcher (addr: 14000af1b90). Total: 0
```

**Methods:**

- `Track(resource, description)` - Register a resource
- `Untrack(resource, description)` - Unregister a resource
- `Count()` - Get current count
- `Remaining()` - List all tracked resources
- `AssertEmpty()` - Panic if any resources remain
- `Clear()` - Remove all tracked resources
