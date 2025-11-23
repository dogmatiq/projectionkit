package awsx

import (
	"context"
)

// Do executes an AWS API request.
//
// fn is a function that is called to execute the request, typically a method on
// one of the AWS client types.
//
// m is a function that mutates the input value before it is sent and returns
// any options that should be used when sending the request.
func Do[In, Out, Options any](
	ctx context.Context,
	fn func(context.Context, *In, ...func(*Options)) (Out, error),
	m func(any) []func(*Options),
	in *In,
) (out Out, err error) {
	var options []func(*Options)
	if m != nil {
		options = m(in)
	}
	return fn(ctx, in, options...)
}
