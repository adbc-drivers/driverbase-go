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
	"errors"
	"net/url"
	"testing"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/stretchr/testify/require"
)

func TestSafeParseURL(t *testing.T) {
	for _, tc := range []struct{ name, input, diagnostic string }{
		{"escape", "mysql://user:password-sentinel@host/db%zz?token=token-sentinel", "invalid URL escape"},
		{"host character", "mysql://user:password-sentinel@bad host/db", "invalid character in host name"},
		{"port", "mysql://user:password-sentinel@host:secret-port/db", "invalid port"},
		{"password as port", "mysql://user:password-sentinel/db", "invalid port"},
		{"bracket", "mysql://user:password-sentinel@[::1/db", "missing ']' in host"},
		{"invalid host", "mysql://user:password-sentinel@[invalid]/db", "invalid host"},
		{"IP literal", "mysql://user:password-sentinel@[127.0.0.1]/db", "invalid IP-literal"},
		{"scheme", ":password-sentinel", "missing protocol scheme"},
		{"control", "mysql://host/\npassword-sentinel", "net/url: invalid control character in URL"},
		{"userinfo", "mysql://bad user:password-sentinel@host/db", "net/url: invalid userinfo"},
		{"colon", "1:password-sentinel", "first path segment in URL cannot contain colon"},
		{"absolute", "https://example.com/path?q=value#fragment", ""},
		{"relative", "path/to/resource", ""},
		{"empty", "", ""},
		{"opaque", "scheme:opaque", ""},
		{"credentials", "mysql://user:password-sentinel@host/db?token=token-sentinel", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := driverbase.SafeParseURL(tc.input)
			if tc.diagnostic == "" {
				expected, expectedErr := url.Parse(tc.input)
				require.NoError(t, expectedErr)
				require.NoError(t, err)
				require.Equal(t, expected, parsed)
				return
			}
			require.Nil(t, parsed)
			require.EqualError(t, err, "failed to parse URL: "+tc.diagnostic)
			require.NotContains(t, err.Error(), "password-sentinel")
			require.NotContains(t, err.Error(), "token-sentinel")
			require.Nil(t, errors.Unwrap(err))
		})
	}
}
