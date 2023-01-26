package dynamoprojection

import (
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// HandlerOption is used to alter the behavior of AWS DynamoDB projection
// handler.
type HandlerOption interface {
	applyOptionToAdaptor(*decorators)
}

// ResourceRepositoryOption is used to alter the behavior ResourceRepository.
type ResourceRepositoryOption interface {
	applyResourceRepositoryOption(*decorators)
}

// TableOption is used to alter the behavior operations related to table
// manipulations.
type TableOption interface {
	applyTableOption(*decorators)
}

type decorators struct {
	decorateGetItem            func(*dynamodb.GetItemInput) []request.Option
	decoratePutItem            func(*dynamodb.PutItemInput) []request.Option
	decorateDeleteItem         func(*dynamodb.DeleteItemInput) []request.Option
	decorateTransactWriteItems func(*dynamodb.TransactWriteItemsInput) []request.Option
	decorateCreateTableItem    func(*dynamodb.CreateTableInput) []request.Option
	decorateDeleteTableItem    func(*dynamodb.DeleteTableInput) []request.Option
}

type options struct {
	applyOptionToAdaptorFunc          func(*decorators)
	applyResourceRepositoryOptionFunc func(*decorators)
	applyTableOptionFunc              func(*decorators)
}

func (o *options) applyOptionToAdaptor(decorators *decorators) {
	if o.applyOptionToAdaptorFunc != nil {
		o.applyOptionToAdaptorFunc(decorators)
	}
}

func (o *options) applyResourceRepositoryOption(decorators *decorators) {
	if o.applyResourceRepositoryOptionFunc != nil {
		o.applyResourceRepositoryOptionFunc(decorators)
	}
}

func (o *options) applyTableOption(decorators *decorators) {
	if o.applyTableOptionFunc != nil {
		o.applyTableOptionFunc(decorators)
	}
}

// WithDecorateGetItem adds a decorator for GetItem operation. The decorator can
// modify the passed GetItemInput structure and return a slice of request.Option
// to alter the request prior to its execution.
func WithDecorateGetItem(
	dec func(*dynamodb.GetItemInput) []request.Option,
) interface {
	HandlerOption
	ResourceRepositoryOption
} {
	return &options{
		applyOptionToAdaptorFunc: func(d *decorators) {
			d.decorateGetItem = dec
		},
		applyResourceRepositoryOptionFunc: func(d *decorators) {
			d.decorateGetItem = dec
		},
	}
}

// WithDecoratePutItem adds a decorator for PutItem operation. The decorator can
// modify the passed PutItemInput structure and return a slice of request.Option
// to alter the request prior to its execution.
func WithDecoratePutItem(
	dec func(*dynamodb.PutItemInput) []request.Option,
) interface {
	HandlerOption
	ResourceRepositoryOption
} {
	return &options{
		applyOptionToAdaptorFunc: func(d *decorators) {
			d.decoratePutItem = dec
		},
		applyResourceRepositoryOptionFunc: func(d *decorators) {
			d.decoratePutItem = dec
		},
	}
}

// WithDecorateDeleteItem adds a decorator for DeleteItem operation. The
// decorator can modify the passed DeleteItemInput structure and return a slice
// of request.Option to alter the request prior to its execution.
func WithDecorateDeleteItem(
	dec func(*dynamodb.DeleteItemInput) []request.Option,
) interface {
	HandlerOption
	ResourceRepositoryOption
} {
	return &options{
		applyOptionToAdaptorFunc: func(d *decorators) {
			d.decorateDeleteItem = dec
		},
		applyResourceRepositoryOptionFunc: func(d *decorators) {
			d.decorateDeleteItem = dec
		},
	}
}

// WithDecorateTransactWriteItems adds a decorator for TransactWriteItems
// operation. The decorator can modify the passed TransactWriteItemsInput
// structure and return a slice of request.Option to alter the request prior to
// its execution.
func WithDecorateTransactWriteItems(
	dec func(*dynamodb.TransactWriteItemsInput) []request.Option,
) interface {
	HandlerOption
	ResourceRepositoryOption
} {
	return &options{
		applyOptionToAdaptorFunc: func(d *decorators) {
			d.decorateTransactWriteItems = dec
		},
		applyResourceRepositoryOptionFunc: func(d *decorators) {
			d.decorateTransactWriteItems = dec
		},
	}
}

// WithDecorateCreateTable adds a decorator for CreateTable operation. The
// decorator can modify the passed CreateTableInput structure and return
// a slice of request.Option to alter the request prior to its execution.
func WithDecorateCreateTable(
	dec func(*dynamodb.CreateTableInput) []request.Option,
) TableOption {
	return &options{
		applyTableOptionFunc: func(d *decorators) {
			d.decorateCreateTableItem = dec
		},
	}
}

// WithDecorateDeleteTable adds a decorator for DeleteTable operation. The
// decorator can modify the passed DeleteTableInput structure and return a slice
// of request.Option to alter the request prior to its execution.
func WithDecorateDeleteTable(
	dec func(*dynamodb.DeleteTableInput) []request.Option,
) TableOption {
	return &options{
		applyTableOptionFunc: func(d *decorators) {
			d.decorateDeleteTableItem = dec
		},
	}
}
