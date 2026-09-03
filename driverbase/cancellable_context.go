// Copyright (c) 2026 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package driverbase

import (
	"context"
	"sync"
)

// CancellableContext tracks one context that may be cancelled concurrently.
// The zero value is ready for use. A CancellableContext must not be copied
// after first use.
type CancellableContext struct {
	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
}

// NewContext cancels the currently tracked context and replaces it with a new
// background context.
func (c *CancellableContext) NewContext() context.Context {
	c.mu.Lock()
	previous := c.cancel
	ctx, cancel := context.WithCancel(context.Background())
	c.ctx, c.cancel = ctx, cancel
	c.mu.Unlock()

	if previous != nil {
		previous()
	}
	return ctx
}

// FinishContext stops tracking ctx if it is still current. It does not cancel
// ctx because a returned result may continue using it.
func (c *CancellableContext) FinishContext(ctx context.Context) {
	c.mu.Lock()
	if c.ctx == ctx {
		c.ctx = nil
		c.cancel = nil
	}
	c.mu.Unlock()
}

// CancelContext cancels and clears the currently tracked context. It reports
// whether a context was active.
func (c *CancellableContext) CancelContext() bool {
	c.mu.Lock()
	cancel := c.cancel
	c.ctx = nil
	c.cancel = nil
	c.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	return cancel != nil
}
