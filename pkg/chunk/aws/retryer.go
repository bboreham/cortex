package aws

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/request"
	ot "github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"

	"github.com/cortexproject/cortex/pkg/util"
)

// Map Cortex Backoff into AWS Retryer interface
type retryer struct {
	*util.Backoff
	maxRetries int
}

var _ request.Retryer = &retryer{}

func newRetryer(ctx context.Context, cfg util.BackoffConfig) *retryer {
	return &retryer{
		Backoff:    util.NewBackoff(ctx, cfg),
		maxRetries: cfg.MaxRetries,
	}
}

func (r *retryer) withRetrys(req *request.Request) {
	req.Retryer = r
}

// RetryRules return the retry delay that should be used by the SDK before
// making another request attempt for the failed request.
func (r *retryer) RetryRules(req *request.Request) time.Duration {
	if sp := ot.SpanFromContext(req.Context()); sp != nil {
		sp.LogFields(otlog.Int("retry", r.NumRetries()))
	}
	return r.Backoff.NextDelay()
}

// ShouldRetry returns if the failed request is retryable.
func (r *retryer) ShouldRetry(req *request.Request) bool {
	var d client.DefaultRetryer
	return d.ShouldRetry(req)
}

// MaxRetries is the number of times a request may be retried before
// failing.
func (r *retryer) MaxRetries() int {
	return r.maxRetries
}
