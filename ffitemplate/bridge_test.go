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

package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"text/template"
	"time"
)

// Compile the rendered templates, not a hand-maintained copy of the bridge.
// The fixture supplies interface stubs and never connects to a database.
func testGeneratedBridge(t *testing.T, fixture string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	dir := t.TempDir()
	pkg := filepath.Join(dir, "pkg")
	if err := os.Mkdir(pkg, 0750); err != nil {
		t.Fatal(err)
	}
	repo, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	write := func(name string, contents []byte) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), contents, 0600); err != nil {
			t.Fatal(err)
		}
	}
	run := func(args ...string) []byte {
		t.Helper()
		cmd := exec.CommandContext(ctx, "go", args...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "GOWORK=off", "GOFLAGS=", "GOEXPERIMENT=cgocheck2")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("go %v: %v\n%s", args, err, out)
		}
		return out
	}
	if strings.TrimSpace(string(run("env", "CGO_ENABLED"))) != "1" {
		t.Skip("generated bridge requires CGO")
	}
	write("go.mod", fmt.Appendf(nil, `module example.com/fixture

go 1.26.9

require github.com/adbc-drivers/driverbase-go/driverbase v0.0.0
replace github.com/adbc-drivers/driverbase-go/driverbase => %q
replace github.com/adbc-drivers/driverbase-go/testutil => %q
`, filepath.ToSlash(filepath.Join(repo, "driverbase")), filepath.ToSlash(filepath.Join(repo, "testutil"))))
	write("fixture.go", []byte(`package fixture
import (
 "github.com/adbc-drivers/driverbase-go/driverbase"
 "github.com/apache/arrow-go/v18/arrow/memory"
)
func NewDriver(memory.Allocator) driverbase.DriverWithContext { return nil }
`))
	data := tmplData{Driver: "fixture.NewDriver", DriverImport: "example.com/fixture", DriverPackage: "fixture", Prefix: "Test", PrefixLower: "test", PrefixUpper: "TEST", PrefixNoCamel: "Test"}
	for _, file := range fileList {
		tmpl, err := template.ParseFiles(filepath.Join("_tmpl", file))
		if err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		if err := tmpl.Execute(&out, data); err != nil {
			t.Fatal(err)
		}
		write(filepath.Join("pkg", strings.TrimSuffix(file, Ext)), out.Bytes())
	}
	module := strings.TrimSpace(string(run("list", "-mod=mod", "-m", "-f", "{{.Dir}}", "github.com/apache/arrow-adbc/go/adbc")))
	// Module download messages can precede the resolved directory on a cold cache.
	lines := strings.Split(module, "\n")
	module = lines[len(lines)-1]
	adbcHeader, err := os.ReadFile(filepath.Join(module, "drivermgr", "arrow-adbc", "adbc.h"))
	if err != nil {
		t.Fatal(err)
	}
	write("pkg/adbc.h", adbcHeader)
	checks, err := os.ReadFile(filepath.Join("testdata", fixture+".go.txt"))
	if err != nil {
		t.Fatal(err)
	}
	write("pkg/checks.go", checks)
	write("pkg/checks_test.go", []byte("package main\nimport \"testing\"\nfunc TestGeneratedBridge(t *testing.T) { checkBridge(t) }\n"))
	args := []string{"test", "-mod=mod", "-count=1", "-gcflags=all=-d=checkptr=2", "-tags", "driverlib"}
	if runtime.GOOS != "windows" {
		args = append(args, "-race")
	}
	args = append(args, "./pkg")
	t.Log(string(run(args...)))
}
