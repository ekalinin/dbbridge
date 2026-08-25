package grpcconnect

import (
	"connectrpc.com/connect"
	"connectrpc.com/otelconnect"
)

// TracingInterceptor gives every Connect RPC a server span and makes the
// caller's W3C trace context its parent, so a trace started by the calling
// application reaches the query it submits (spec §11).
//
// It belongs first in the interceptor chain: the rate limiter and the
// authenticator run inside the span, which is where a rejected call is worth
// seeing.
func TracingInterceptor() (connect.Interceptor, error) {
	return otelconnect.NewInterceptor(
		// otelconnect treats a remote span as untrusted by default and attaches
		// it as a link rather than as a parent, which would leave a caller's
		// trace and ours as two disconnected trees. dbbridge sits behind
		// authentication on a private network, so the caller's context is taken
		// at face value - the same treatment the REST transport gives it.
		otelconnect.WithTrustRemote(),
		// Tracing only. The metric set is the one spec §11 defines, and
		// otelconnect's RPC instruments would silently add to it.
		otelconnect.WithoutMetrics(),
	)
}
