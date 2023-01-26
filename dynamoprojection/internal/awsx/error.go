package awsx

import (
	"errors"

	"github.com/aws/aws-sdk-go/aws/awserr"
)

// IsErrorCode returns true if err is an AWS error with the given code.
func IsErrorCode(err error, code string) bool {
	var awsErr awserr.Error
	return errors.As(err, &awsErr) && awsErr.Code() == code
}
