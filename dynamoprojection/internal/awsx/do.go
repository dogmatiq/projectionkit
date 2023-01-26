package awsx

import (
	"context"

	"github.com/aws/aws-sdk-go/aws/request"
)

// Do executes an AWS API request.
//
// fn is a function that is called to execute the request, typically a method on
// a *dynamodb.DynamoDB client.
//
// dec is a decorator function that mutates the input value before it is sent
// and returns any options that should be used when sending the request.
func Do[In, Out any](
	ctx context.Context,
	fn func(context.Context, *In, ...request.Option) (Out, error),
	dec func(*In) []request.Option,
	in *In,
	options ...request.Option,
) (out Out, err error) {
	options = append(options, Decorate(in, dec)...)
	return fn(ctx, in, options...)
}

// Decorate mutates an input value in-place and returns any options that should
// be used when sending the request.
func Decorate[In any](
	in *In,
	dec func(*In) []request.Option,
) []request.Option {
	if dec != nil {
		return dec(in)
	}

	return nil
}
