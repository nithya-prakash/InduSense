package reliability

import (
	"context"
	"errors"
	"time"
)

// ErrPermanent wraps an error that retrying cannot fix. RetryWithBackoff
// stops immediately on this error instead of burning through its attempt
// budget on something that will never succeed.
type ErrPermanent struct{ Err error }

func (e *ErrPermanent) Error() string { return e.Err.Error() }
func (e *ErrPermanent) Unwrap() error { return e.Err }

// RetryWithBackoff calls fn up to maxAttempts times, doubling the delay
// between attempts starting at baseDelay (1s, 2s, 4s, 8s, 16s, ...). It stops
// early on ctx cancellation or a permanent error.
func RetryWithBackoff(ctx context.Context, maxAttempts int, baseDelay time.Duration, sleep func(time.Duration), fn func() error) error {
	var lastErr error
	delay := baseDelay

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}

		var permanent *ErrPermanent
		if errors.As(err, &permanent) {
			return err
		}

		lastErr = err
		if attempt == maxAttempts {
			break
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		sleep(delay)
		delay *= 2
	}
	return lastErr
}
