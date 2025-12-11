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
	"errors"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc"
)

// ErrorInfo contains extracted information from a database driver error
type ErrorInfo struct {
	Status     adbc.Status
	SqlState   string
	VendorCode int32
	Details    []adbc.ErrorDetail
}

// ErrorInspector inspects database driver errors to extract metadata.
// Drivers can implement this interface to map database-specific errors to
// ADBC status codes and extract error details like SQLSTATE and vendor codes.
type ErrorInspector interface {
	InspectError(err error, defaultStatus adbc.Status) ErrorInfo
}

// ErrorHelper helps format errors for ADBC drivers.
type ErrorHelper struct {
	DriverName     string
	ErrorInspector ErrorInspector
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

// wrapError creates an adbc.Error by inspecting the underlying error.
func (helper *ErrorHelper) wrapError(err error, defaultStatus adbc.Status, format string, args ...any) error {
	if err == nil {
		return nil
	}

	var adbcErr adbc.Error
	if errors.As(err, &adbcErr) {
		return err
	}

	contextMsg := fmt.Sprintf(format, args...)

	// Inspect error if driver provided inspect error function that maps database-specific errors to ADBC status codes
	var info ErrorInfo
	if helper.ErrorInspector != nil {
		info = helper.ErrorInspector.InspectError(err, defaultStatus)
	}

	status := info.Status
	if status == 0 {
		status = defaultStatus
	}

	msg := fmt.Sprintf("[%s] %s: %v", helper.DriverName, contextMsg, err)

	adbcError := adbc.Error{
		Code:       status,
		Msg:        msg,
		VendorCode: info.VendorCode,
		Details:    info.Details,
	}

	// Copy SQLSTATE if available (exactly 5 characters)
	if len(info.SqlState) >= 5 {
		copy(adbcError.SqlState[:], info.SqlState[0:5])
	}

	return errors.Join(adbcError, err)
}

func (helper *ErrorHelper) WrapIO(err error, format string, args ...any) error {
	return helper.wrapError(err, adbc.StatusIO, format, args...)
}

func (helper *ErrorHelper) WrapInvalidArgument(err error, format string, args ...any) error {
	return helper.wrapError(err, adbc.StatusInvalidArgument, format, args...)
}

func (helper *ErrorHelper) WrapInvalidState(err error, format string, args ...any) error {
	return helper.wrapError(err, adbc.StatusInvalidState, format, args...)
}

func (helper *ErrorHelper) WrapInvalidData(err error, format string, args ...any) error {
	return helper.wrapError(err, adbc.StatusInvalidData, format, args...)
}

func (helper *ErrorHelper) WrapInternal(err error, format string, args ...any) error {
	return helper.wrapError(err, adbc.StatusInternal, format, args...)
}

func (helper *ErrorHelper) WrapUnauthenticated(err error, format string, args ...any) error {
	return helper.wrapError(err, adbc.StatusUnauthenticated, format, args...)
}

func (helper *ErrorHelper) WrapUnauthorized(err error, format string, args ...any) error {
	return helper.wrapError(err, adbc.StatusUnauthorized, format, args...)
}

func (helper *ErrorHelper) WrapNotFound(err error, format string, args ...any) error {
	return helper.wrapError(err, adbc.StatusNotFound, format, args...)
}

func (helper *ErrorHelper) WrapUnknown(err error, format string, args ...any) error {
	return helper.wrapError(err, adbc.StatusUnknown, format, args...)
}

func (helper *ErrorHelper) WrapAlreadyExists(err error, format string, args ...any) error {
	return helper.wrapError(err, adbc.StatusAlreadyExists, format, args...)
}

func (helper *ErrorHelper) WrapIntegrity(err error, format string, args ...any) error {
	return helper.wrapError(err, adbc.StatusIntegrity, format, args...)
}

func (helper *ErrorHelper) WrapCancelled(err error, format string, args ...any) error {
	return helper.wrapError(err, adbc.StatusCancelled, format, args...)
}

func (helper *ErrorHelper) WrapTimeout(err error, format string, args ...any) error {
	return helper.wrapError(err, adbc.StatusTimeout, format, args...)
}

func (helper *ErrorHelper) WrapNotImplemented(err error, format string, args ...any) error {
	return helper.wrapError(err, adbc.StatusNotImplemented, format, args...)
}
