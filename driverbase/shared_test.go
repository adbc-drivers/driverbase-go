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

package driverbase_test

import (
	"fmt"
	"testing"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/stretchr/testify/suite"
)

func TestShared(t *testing.T) {
	suite.Run(t, &SharedTest{})
}

type SharedTest struct {
	suite.Suite
}

type value struct {
	closeErr error
}

func (v *value) Close() error {
	return v.closeErr
}

func (s *SharedTest) TestNewClose() {
	v := &value{}
	conn := driverbase.NewShared(v, v)
	s.NoError(conn.Close())
	// Close is idempotent
	s.NoError(conn.Close())

	v = &value{closeErr: fmt.Errorf("close error")}
	conn = driverbase.NewShared(v, v)
	s.ErrorContains(conn.Close(), "close error")
}

func (s *SharedTest) TestAfterClose() {
	v := &value{}
	conn := driverbase.NewShared(v, v)
	s.NoError(conn.Close())

	handle, err := conn.Hold()
	s.ErrorContains(err, "Hold: already closed")

	err = conn.Run(func(v *value) error {
		s.Fail("should not run")
		return nil
	})
	s.ErrorContains(err, "Run: already closed")

	_, err = driverbase.WithShared(conn, func(v *value) (any, error) {
		s.Fail("should not run")
		return nil, nil
	})
	s.ErrorContains(err, "WithShared: already closed")

	s.Nil(handle.Handle())
}
