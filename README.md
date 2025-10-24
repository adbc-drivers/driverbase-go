<!--
  Copyright (c) 2025 ADBC Drivers Contributors

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

          http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Go Framework for Apache Arrow ADBC Drivers

This is a framework for developing ADBC drivers in Go.  It is forked from the
framework upstream in [apache/arrow-adbc](https://github.com/apache/arrow-adbc/).
The upstream framework is marked as an internal package, and is hence not
usable from outside the repository itself.

## Installation

This is not meant to be installed directly.

Developers can add this to a project via `go get`:

```shell
go get github.com/adbc-drivers/driverbase-go/driverbase
go get github.com/adbc-drivers/driverbase-go/testutil
go get github.com/adbc-drivers/driverbase-go/validation
```

## Usage

- `driverbase` is a framework for building ADBC drivers.
- `testutil` contains common helpers for writing unit tests.
- `validation` contains basic integration tests for ADBC drivers.

Currently there is no other documentation.  Eventually, API documentation will
be made available on go.dev.

## Building

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
