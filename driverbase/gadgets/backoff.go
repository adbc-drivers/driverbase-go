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

package gadgets

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"time"
)

func SleepCtx(ctx context.Context, duration time.Duration) error {
	if ctx == nil {
		time.Sleep(duration)
		return nil
	}

	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Backoff computes a retry interval with random jitter, similar to
// gax.Backoff.
type Backoff struct {
	Start time.Duration
	Max   time.Duration
	Mul   float64

	currentMax time.Duration
}

func (b *Backoff) Next() time.Duration {
	// Defaults chosen somewhat arbitrarily
	if b.Max <= 0 {
		b.Max = 10 * time.Second
	}
	if b.Start <= 0 {
		b.Start = time.Second
	}
	if b.Start > b.Max {
		b.Start = b.Max
	}
	if b.Mul <= 0 {
		b.Mul = 1.5
	}
	if b.currentMax <= 0 {
		b.currentMax = b.Start
	}

	// Random duration between 1ns and nextBackoff
	// https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
	backoff := time.Duration(max(int64(1), rand.Int64N(1+int64(b.currentMax))))
	b.currentMax = min(time.Duration(float64(b.currentMax)*b.Mul), b.Max)
	return backoff
}

func RetryWithLog(ctx context.Context, logger *slog.Logger, operation string, b *Backoff, maxTries int, op func() error) error {
	if maxTries <= 0 {
		return errors.New("driverbase.gadgets.RetryWithLog: maxTries must be > 0")
	}

	if b == nil {
		b = &Backoff{}
	}
	if ctx == nil {
		ctx = context.Background()
	}

	var err error
	for i := range maxTries {
		err = op()
		if err == nil {
			return nil
		} else if errors.Is(err, errNoRetry) {
			if logger != nil {
				logger.DebugContext(ctx, "no retry: "+operation, "error", err)
			}
			return err
		}
		pause := b.Next()
		if logger != nil {
			logger.DebugContext(ctx, "retrying: "+operation, "error", err, "backoff", pause)
		}
		if i == maxTries-1 {
			// Don't unnecessarily sleep on the last iteration
			break
		}
		if err := SleepCtx(ctx, pause); err != nil {
			return err
		}
	}
	return err
}

func Retry(b *Backoff, maxTries int, op func() error) error {
	return RetryWithLog(context.Background(), nil, "", b, maxTries, op)
}

var errNoRetry = errors.New("operation failed and could not be retried")

func NoRetry(err error) error {
	if err == nil {
		return nil
	}
	return errors.Join(err, errNoRetry)
}
