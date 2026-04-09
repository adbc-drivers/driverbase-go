// Copyright (c) 2025 ADBC Drivers Contributors
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
	"context"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	StatementMessageOptionUnknown              = "Unknown statement option"
	StatementMessageOptionUnsupported          = "Unsupported statement option"
	StatementMessageTraceParentIncorrectFormat = "Incorrect or unsupported trace parent format"
)

type StatementImpl interface {
	adbc.StatementWithContext
	adbc.StatementExecuteSchema
	adbc.GetSetOptionsWithContext
	adbc.OTelTracing
	Base() *StatementImplBase
}

type StatementImplBase struct {
	ErrorHelper ErrorHelper
	Tracer      trace.Tracer

	cnxn        *ConnectionImplBase
	traceParent string
}

type Statement interface {
	adbc.StatementWithContext
	adbc.GetSetOptionsWithContext
}

type statement struct {
	StatementImpl
}

func NewStatementImplBase(cnxn *ConnectionImplBase, errorHelper ErrorHelper) StatementImplBase {
	return StatementImplBase{
		ErrorHelper: errorHelper,
		Tracer:      cnxn.Tracer,
		cnxn:        cnxn,
	}
}

func NewStatement(impl StatementImpl) adbc.StatementWithContext {
	return &statement{
		StatementImpl: impl,
	}
}

func (st *StatementImplBase) Base() *StatementImplBase {
	return st
}

func (st *StatementImplBase) Close(ctx context.Context) error {
	return nil
}

func (st *StatementImplBase) Bind(ctx context.Context, values arrow.RecordBatch) error {
	defer values.Release()
	return st.ErrorHelper.Errorf(adbc.StatusNotImplemented, "bind")
}

func (st *StatementImplBase) BindStream(ctx context.Context, stream array.RecordReader) error {
	defer stream.Release()
	return st.ErrorHelper.Errorf(adbc.StatusNotImplemented, "bind stream")
}

func (st *StatementImplBase) SetSqlQuery(ctx context.Context, query string) error {
	return st.ErrorHelper.Errorf(adbc.StatusNotImplemented, "set sql query")
}

func (st *StatementImplBase) SetSubstraitPlan(ctx context.Context, plan []byte) error {
	return st.ErrorHelper.Errorf(adbc.StatusNotImplemented, "set substrait plan")
}

func (st *StatementImplBase) GetParameterSchema(ctx context.Context) (*arrow.Schema, error) {
	return nil, st.ErrorHelper.Errorf(adbc.StatusNotImplemented, "get parameter schema")
}

func (st *StatementImplBase) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	return nil, -1, st.ErrorHelper.Errorf(adbc.StatusNotImplemented, "execute query")
}

func (st *StatementImplBase) ExecuteUpdate(ctx context.Context) (int64, error) {
	return -1, st.ErrorHelper.Errorf(adbc.StatusNotImplemented, "execute update")
}

func (st *StatementImplBase) Prepare(ctx context.Context) error {
	return st.ErrorHelper.Errorf(adbc.StatusNotImplemented, "prepare")
}

func (st *StatementImplBase) ExecutePartitions(ctx context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	return nil, adbc.Partitions{}, -1, st.ErrorHelper.Errorf(adbc.StatusNotImplemented, "execute partitions")
}

func (st *StatementImplBase) ExecuteSchema(context.Context) (*arrow.Schema, error) {
	return nil, st.ErrorHelper.Errorf(adbc.StatusNotImplemented, "execute schema")
}

func (st *StatementImplBase) SetOption(ctx context.Context, key, value string) error {
	switch strings.ToLower(key) {
	case adbc.OptionKeyTelemetryTraceParent:
		st.SetTraceParent(strings.TrimSpace(value))
		return nil
	}
	return st.ErrorHelper.Errorf(adbc.StatusNotImplemented, "%s '%s'", StatementMessageOptionUnknown, key)
}

func (st *StatementImplBase) SetOptionBytes(ctx context.Context, key string, value []byte) error {
	return st.ErrorHelper.Errorf(adbc.StatusNotImplemented, "%s '%s'", StatementMessageOptionUnknown, key)
}

func (st *StatementImplBase) SetOptionInt(ctx context.Context, key string, value int64) error {
	return st.ErrorHelper.Errorf(adbc.StatusNotImplemented, "%s '%s'", StatementMessageOptionUnknown, key)
}

func (st *StatementImplBase) SetOptionDouble(ctx context.Context, key string, value float64) error {
	return st.ErrorHelper.Errorf(adbc.StatusNotImplemented, "%s '%s'", StatementMessageOptionUnknown, key)
}

func (st *StatementImplBase) GetOption(ctx context.Context, key string) (string, error) {
	switch strings.ToLower(key) {
	case adbc.OptionKeyTelemetryTraceParent:
		return st.GetTraceParent(), nil
	}
	return "", st.ErrorHelper.Errorf(adbc.StatusNotImplemented, "%s '%s'", StatementMessageOptionUnknown, key)
}

func (st *StatementImplBase) GetOptionBytes(ctx context.Context, key string) ([]byte, error) {
	return nil, st.ErrorHelper.Errorf(adbc.StatusNotImplemented, "%s '%s'", StatementMessageOptionUnknown, key)
}

func (st *StatementImplBase) GetOptionInt(ctx context.Context, key string) (int64, error) {
	return 0, st.ErrorHelper.Errorf(adbc.StatusNotImplemented, "%s '%s'", StatementMessageOptionUnknown, key)
}

func (st *StatementImplBase) GetOptionDouble(ctx context.Context, key string) (float64, error) {
	return 0, st.ErrorHelper.Errorf(adbc.StatusNotImplemented, "%s '%s'", StatementMessageOptionUnknown, key)
}

func (st *StatementImplBase) GetTraceParent() string {
	return st.traceParent
}

func (st *StatementImplBase) SetTraceParent(traceParent string) {
	st.traceParent = traceParent
}

func (st *StatementImplBase) StartSpan(
	ctx context.Context,
	spanName string,
	opts ...trace.SpanStartOption,
) (context.Context, trace.Span) {
	ctx, _ = maybeAddTraceParent(ctx, st.cnxn, st)
	return st.Tracer.Start(ctx, spanName, opts...)
}

func (st *StatementImplBase) GetInitialSpanAttributes() []attribute.KeyValue {
	return st.cnxn.GetInitialSpanAttributes()
}

var _ StatementImpl = (*StatementImplBase)(nil)
var _ Statement = (*statement)(nil)
