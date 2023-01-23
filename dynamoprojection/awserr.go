package dynamoprojection

import (
	"errors"

	"github.com/aws/aws-sdk-go/aws/awserr"
)

// isErrorCode returns true if err is an AWS error with the given code.
func isErrorCode(err error, code string) bool {
	var awsErr awserr.Error
	return errors.As(err, &awsErr) && awsErr.Code() == code
}
