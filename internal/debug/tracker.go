// Copyright 2024 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package debug

import (
	"fmt"
	"sync"
)

// ResourceTracker tracks resources (like iterators, connections, etc.) to help
// detect leaks and understand resource lifecycle.
type ResourceTracker struct {
	mu        sync.Mutex
	name      string
	resources map[uintptr]string // addr -> description
	count     int
	tracer    *Tracer
}

// NewResourceTracker creates a new tracker for resources of the given name.
// If tracer is nil, a new tracer with the resource name as prefix is created.
func NewResourceTracker(name string, tracer *Tracer) *ResourceTracker {
	if tracer == nil {
		tracer = NewTracer(name)
	}
	return &ResourceTracker{
		name:      name,
		resources: make(map[uintptr]string),
		tracer:    tracer,
	}
}

// Track registers a new resource with an optional description.
// Returns true if the resource was newly added, false if it was already tracked (duplicate).
func (rt *ResourceTracker) Track(resource any, description string) bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	addr := Addr(resource)
	if _, exists := rt.resources[addr]; exists {
		rt.tracer.Println("!!! DUPLICATE %s: %s (addr: %x). Total: %d", rt.name, description, addr, rt.count)
		return false
	}

	rt.count++
	rt.resources[addr] = description
	rt.tracer.Println(">>> NEW %s: %s (addr: %x). Total: %d", rt.name, description, addr, rt.count)
	return true
}

// Untrack removes a resource from tracking.
// Returns true if the resource was found and removed, false if it wasn't tracked.
func (rt *ResourceTracker) Untrack(resource any, description string) bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	addr := Addr(resource)
	if _, exists := rt.resources[addr]; !exists {
		rt.tracer.Println("!!! NOT FOUND %s: %s (addr: %x). Total: %d", rt.name, description, addr, rt.count)
		return false
	}

	rt.count--
	delete(rt.resources, addr)
	rt.tracer.Println("<<< CLOSED %s: %s (addr: %x). Total: %d", rt.name, description, addr, rt.count)

	if rt.count > 0 {
		rt.printRemaining()
	}
	return true
}

// Count returns the current number of tracked resources.
func (rt *ResourceTracker) Count() int {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.count
}

// Remaining returns a slice of descriptions for all currently tracked resources.
func (rt *ResourceTracker) Remaining() []string {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	result := make([]string, 0, len(rt.resources))
	for addr, desc := range rt.resources {
		result = append(result, fmt.Sprintf("%s (addr: %x)", desc, addr))
	}
	return result
}

// printRemaining prints all remaining resources (must be called with lock held).
func (rt *ResourceTracker) printRemaining() {
	addrs := make([]string, 0, len(rt.resources))
	for addr, desc := range rt.resources {
		addrs = append(addrs, fmt.Sprintf("%s:%x", desc, addr))
	}
	rt.tracer.Println("    REMAINING %ss: %v", rt.name, addrs)
}

// Clear removes all tracked resources.
func (rt *ResourceTracker) Clear() {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.resources = make(map[uintptr]string)
	rt.count = 0
}

// AssertEmpty panics if there are any tracked resources remaining.
// Useful in tests to ensure all resources were properly cleaned up.
func (rt *ResourceTracker) AssertEmpty() {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	if rt.count > 0 {
		remaining := make([]string, 0, len(rt.resources))
		for addr, desc := range rt.resources {
			remaining = append(remaining, fmt.Sprintf("%s (addr: %x)", desc, addr))
		}
		panic(fmt.Sprintf("ResourceTracker[%s] has %d leaked resources: %v",
			rt.name, rt.count, remaining))
	}
}
