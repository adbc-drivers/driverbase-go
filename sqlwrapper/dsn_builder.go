// Copyright (c) 2025 ADBC Drivers Contributors
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

package sqlwrapper

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
)

// DSNBuilder handles DSN construction from URI and credential components.
// Individual drivers can override this to provide database-specific DSN construction logic.
type DSNBuilder interface {
	BuildDSN(opts map[string]string) (string, error)
}

// DefaultDSNBuilder provides basic URI+credentials DSN construction.
// It attempts to intelligently handle both URI-based and non-URI DSN formats.
type DefaultDSNBuilder struct{}

// BuildDSN constructs a DSN from the provided options.
// It supports both URI-based formats (e.g., postgresql://...) and
// non-URI formats (e.g., MySQL's user:password@tcp(host)/db).
func (d *DefaultDSNBuilder) BuildDSN(opts map[string]string) (string, error) {
	baseURI := opts[adbc.OptionKeyURI]
	username := opts[adbc.OptionKeyUsername]
	password := opts[adbc.OptionKeyPassword]

	// If no base URI provided, this is an error
	if baseURI == "" {
		return "", fmt.Errorf("missing required option %s", adbc.OptionKeyURI)
	}

	// If no credentials provided, return original URI
	if username == "" && password == "" {
		return baseURI, nil
	}

	// Inject credentials into the DSN
	return d.injectCredentials(baseURI, username, password)
}

// injectCredentials handles inserting username/password into different DSN formats.
func (d *DefaultDSNBuilder) injectCredentials(baseURI, username, password string) (string, error) {
	// Handle URI-based formats (e.g., postgresql://host/db)
	if strings.Contains(baseURI, "://") {
		return d.injectCredentialsURI(baseURI, username, password)
	}

	// Handle non-URI formats (e.g., MySQL's host:port/db or user:pass@host:port/db)
	return d.injectCredentialsNonURI(baseURI, username, password)
}

// injectCredentialsURI handles URI-based DSN formats.
func (d *DefaultDSNBuilder) injectCredentialsURI(baseURI, username, password string) (string, error) {
	u, err := url.Parse(baseURI)
	if err != nil {
		return "", fmt.Errorf("invalid URI format: %w", err)
	}

	// Set user info if provided
	if username != "" {
		if password != "" {
			u.User = url.UserPassword(username, password)
		} else {
			u.User = url.User(username)
		}
	}

	return u.String(), nil
}

// injectCredentialsNonURI handles non-URI DSN formats.
// This primarily handles formats like MySQL's user:pass@tcp(host:port)/db.
func (d *DefaultDSNBuilder) injectCredentialsNonURI(baseURI, username, password string) (string, error) {
	// Check if credentials are already present in the DSN
	if strings.Contains(baseURI, "@") {
		// DSN already has credentials, we need to replace them
		parts := strings.SplitN(baseURI, "@", 2)
		if len(parts) == 2 {
			// Replace existing credentials
			if password != "" {
				return fmt.Sprintf("%s:%s@%s", url.QueryEscape(username), url.QueryEscape(password), parts[1]), nil
			}
			return fmt.Sprintf("%s@%s", url.QueryEscape(username), parts[1]), nil
		}
	}

	// No existing credentials, prepend them
	if password != "" {
		return fmt.Sprintf("%s:%s@%s", url.QueryEscape(username), url.QueryEscape(password), baseURI), nil
	}
	return fmt.Sprintf("%s@%s", url.QueryEscape(username), baseURI), nil
}
