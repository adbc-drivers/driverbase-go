// Copyright (c) 2025 Columnar Technologies.  All rights reserved.

module github.com/columnar-tech/drivers/driverbase

go 1.24.0

require (
	github.com/apache/arrow-adbc/go/adbc v1.5.0
	github.com/apache/arrow-go/v18 v18.2.0
	github.com/columnar-tech/drivers/testutil v0.0.0-00010101000000-000000000000
	github.com/stretchr/testify v1.10.0
	golang.org/x/sync v0.11.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/goccy/go-json v0.10.5 // indirect
	github.com/google/flatbuffers v25.2.10+incompatible // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	github.com/zeebo/xxh3 v1.0.2 // indirect
	golang.org/x/exp v0.0.0-20240909161429-701f63a606c0 // indirect
	golang.org/x/mod v0.23.0 // indirect
	golang.org/x/sys v0.31.0 // indirect
	golang.org/x/tools v0.30.0 // indirect
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
	google.golang.org/protobuf v1.36.5 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/columnar-tech/drivers/testutil => ../testutil
