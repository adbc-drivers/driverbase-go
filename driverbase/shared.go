// Copyright (c) 2025 Columnar Technologies, Inc.
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
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/apache/arrow-adbc/go/adbc"
)

// Shared allows shared usage of an underlying resource, but not concurrently.
type Shared[T any] struct {
	mu     sync.RWMutex
	handle *T
	closer io.Closer
}

func NewShared[T any](handle *T, closer io.Closer) *Shared[T] {
	return &Shared[T]{
		handle: handle,
		closer: closer,
	}
}

func (sh *Shared[T]) Close() error {
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if sh.handle == nil {
		return nil
	}

	if err := sh.closer.Close(); err != nil {
		return errors.Join(adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("[driverbase] Shared[%T].Close: failed to close resource: %s", *new(T), err),
		}, err)
	}
	sh.handle = nil
	sh.closer = nil
	return nil
}

// Hold gets exclusive access to the connection until the returned handle is released.
// Use sparingly; do not create long-lived handles as it increases potential for deadlock.
func (sh *Shared[T]) Hold() (SharedHandle[T], error) {
	sh.mu.Lock()

	if sh.handle == nil {
		sh.mu.Unlock()
		return SharedHandle[T]{}, adbc.Error{
			Msg:  fmt.Sprintf("[driverbase] Shared[%T].Hold: already closed", *new(T)),
			Code: adbc.StatusInvalidState,
		}
	}

	// mutex is held as long as this exists
	h := SharedHandle[T]{shared: sh}
	return h, nil
}

func (sh *Shared[T]) Run(closure func(*T) error) error {
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if sh.handle == nil {
		return adbc.Error{
			Msg:  fmt.Sprintf("[driverbase] Shared[%T].Run: already closed", *new(T)),
			Code: adbc.StatusInvalidState,
		}
	}

	if err := closure(sh.handle); err != nil {
		return err
	}
	return nil
}

func WithShared[T, R any](sh *Shared[T], closure func(*T) (R, error)) (R, error) {
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if sh.handle == nil {
		return *new(R), adbc.Error{
			Msg:  fmt.Sprintf("[driverbase] Shared[%T].WithShared: already closed", *new(T)),
			Code: adbc.StatusInvalidState,
		}
	}

	return closure(sh.handle)
}

type SharedHandle[T any] struct {
	shared *Shared[T]
}

func (sh *SharedHandle[T]) Handle() *T {
	if sh.shared == nil {
		return nil
	}
	return sh.shared.handle
}

func (sh *SharedHandle[T]) Release() {
	if sh.shared == nil {
		return
	}
	sh.shared.mu.Unlock()
	sh.shared = nil
}
