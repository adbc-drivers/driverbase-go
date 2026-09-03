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

package driverbase_test

import (
	"context"
	"sync"
	"testing"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/stretchr/testify/require"
)

func TestCancellableContext(t *testing.T) {
	var contexts driverbase.CancellableContext

	first := contexts.NewContext()
	second := contexts.NewContext()
	require.ErrorIs(t, first.Err(), context.Canceled)
	require.NoError(t, second.Err())

	contexts.FinishContext(first)
	require.True(t, contexts.CancelContext())
	require.ErrorIs(t, second.Err(), context.Canceled)
	require.False(t, contexts.CancelContext())
}

func TestCancellableContextFinish(t *testing.T) {
	var contexts driverbase.CancellableContext

	ctx := contexts.NewContext()
	contexts.FinishContext(ctx)
	require.NoError(t, ctx.Err())
	require.False(t, contexts.CancelContext())
}

func TestCancellableContextConcurrent(t *testing.T) {
	var contexts driverbase.CancellableContext
	var group sync.WaitGroup
	for range 100 {
		group.Add(2)
		go func() {
			defer group.Done()
			ctx := contexts.NewContext()
			contexts.FinishContext(ctx)
		}()
		go func() {
			defer group.Done()
			contexts.CancelContext()
		}()
	}
	group.Wait()
	contexts.CancelContext()
}
