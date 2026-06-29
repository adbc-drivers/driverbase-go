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

package gadgets_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/adbc-drivers/driverbase-go/driverbase/gadgets"
	"github.com/stretchr/testify/assert"
)

func TestBackoffDefault(t *testing.T) {
	b := &gadgets.Backoff{}
	_ = b.Next()
	assert.Equal(t, time.Second, b.Start)
	assert.Equal(t, 10*time.Second, b.Max)
	assert.Equal(t, 1.5, b.Mul)
}

func TestBackoff(t *testing.T) {
	for _, maxBackoff := range []time.Duration{time.Second, 10 * time.Second, 30 * time.Second} {
		t.Run(maxBackoff.String(), func(t *testing.T) {
			b := &gadgets.Backoff{
				Max: maxBackoff,
			}
			for range 200 {
				backoff := b.Next()
				assert.LessOrEqual(t, backoff, maxBackoff, "backoff %v exceeds max %v", backoff, maxBackoff)
			}
		})
	}
}

func TestRetryInvalid(t *testing.T) {
	err := gadgets.Retry(&gadgets.Backoff{}, 0, func() error {
		return nil
	})
	assert.ErrorContains(t, err, "maxTries must be > 0")
}

func TestRetryNoRetries(t *testing.T) {
	counter := 0
	err := gadgets.Retry(&gadgets.Backoff{}, 1, func() error {
		counter += 1
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, counter)
}

func TestRetryDefault(t *testing.T) {
	counter := 0
	// no need to pass backoff explicitly
	err := gadgets.Retry(nil, 2, func() error {
		counter += 1
		if counter == 1 {
			return errors.New("fail")
		}
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 2, counter)
}

func TestRetryExhausted(t *testing.T) {
	counter := 0
	err := gadgets.Retry(&gadgets.Backoff{Max: time.Millisecond}, 4, func() error {
		counter += 1
		return fmt.Errorf("fail %d", counter)
	})
	assert.Equal(t, 4, counter)
	assert.ErrorContains(t, err, "fail 4")
}

func TestRetryEventual(t *testing.T) {
	counter := 0
	err := gadgets.Retry(&gadgets.Backoff{Max: time.Millisecond}, 10, func() error {
		counter += 1
		if counter < 5 {
			return fmt.Errorf("fail %d", counter)
		}
		return nil
	})
	assert.Equal(t, 5, counter)
	assert.NoError(t, err)
}

func TestRetryAborted(t *testing.T) {
	counter := 0
	err := gadgets.Retry(&gadgets.Backoff{Max: time.Millisecond}, 10, func() error {
		counter += 1
		if counter < 5 {
			return fmt.Errorf("fail %d", counter)
		}
		return gadgets.NoRetry(fmt.Errorf("fail %d", counter))
	})
	assert.Equal(t, 5, counter)
	assert.ErrorContains(t, err, "fail 5")
	assert.ErrorContains(t, err, "operation failed and could not be retried")
}

func TestRetryLog(t *testing.T) {
	counter := 0

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	err := gadgets.RetryWithLog(context.Background(), logger, "foobar", &gadgets.Backoff{Max: time.Millisecond}, 10, func() error {
		counter += 1
		if counter < 5 {
			return fmt.Errorf("fail %d", counter)
		}
		return nil
	})
	assert.Equal(t, 5, counter)
	assert.NoError(t, err)

	log := buf.String()
	assert.Contains(t, log, "retrying: foobar")
	assert.Contains(t, log, "fail 1")
	assert.Contains(t, log, "fail 4")
	t.Log(log)
}

func TestRetryCancel(t *testing.T) {
	counter := 0

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()
	err := gadgets.RetryWithLog(ctx, logger, "foobar", &gadgets.Backoff{}, 1000, func() error {
		time.Sleep(5 * time.Millisecond)
		counter += 1
		return fmt.Errorf("fail %d", counter)
	})

	assert.Less(t, counter, 1000)
	assert.ErrorIs(t, err, context.Canceled)
}
