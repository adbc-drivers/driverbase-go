// Copyright (c) 2025 ADBC Drivers Contributors.
//
// This file has been modified from its original version, which is
// under the Apache License:
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package driverbase

import (
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc"
)

// ErrorHelper helps format errors for ADBC drivers.
type ErrorHelper struct {
	DriverName string
}

func (helper *ErrorHelper) Errorf(code adbc.Status, message string, format ...any) error {
	msg := fmt.Sprintf(message, format...)
	return adbc.Error{
		Code: code,
		Msg:  fmt.Sprintf("[%s] %s", helper.DriverName, msg),
	}
}

func (helper *ErrorHelper) InvalidArgument(message string, format ...any) error {
	return helper.Errorf(adbc.StatusInvalidArgument, message, format...)
}

func (helper *ErrorHelper) InvalidState(message string, format ...any) error {
	return helper.Errorf(adbc.StatusInvalidState, message, format...)
}

func (helper *ErrorHelper) NotImplemented(message string, format ...any) error {
	return helper.Errorf(adbc.StatusNotImplemented, message, format...)
}

func (helper *ErrorHelper) IO(message string, format ...any) error {
	return helper.Errorf(adbc.StatusIO, message, format...)
}

func (helper *ErrorHelper) NotFound(message string, format ...any) error {
	return helper.Errorf(adbc.StatusNotFound, message, format...)
}

func (helper *ErrorHelper) Unknown(message string, format ...any) error {
	return helper.Errorf(adbc.StatusUnknown, message, format...)
}

func (helper *ErrorHelper) AlreadyExists(message string, format ...any) error {
	return helper.Errorf(adbc.StatusAlreadyExists, message, format...)
}

func (helper *ErrorHelper) InvalidData(message string, format ...any) error {
	return helper.Errorf(adbc.StatusInvalidData, message, format...)
}

func (helper *ErrorHelper) Integrity(message string, format ...any) error {
	return helper.Errorf(adbc.StatusIntegrity, message, format...)
}

func (helper *ErrorHelper) Internal(message string, format ...any) error {
	return helper.Errorf(adbc.StatusInternal, message, format...)
}

func (helper *ErrorHelper) Cancelled(message string, format ...any) error {
	return helper.Errorf(adbc.StatusCancelled, message, format...)
}

func (helper *ErrorHelper) Timeout(message string, format ...any) error {
	return helper.Errorf(adbc.StatusTimeout, message, format...)
}

func (helper *ErrorHelper) Unauthenticated(message string, format ...any) error {
	return helper.Errorf(adbc.StatusUnauthenticated, message, format...)
}

func (helper *ErrorHelper) Unauthorized(message string, format ...any) error {
	return helper.Errorf(adbc.StatusUnauthorized, message, format...)
}
