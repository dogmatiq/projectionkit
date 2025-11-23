package identity

import (
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
)

// Key returns a handler's unique key.
func Key(h configurable) [16]byte {
	var c configurer
	h.Configure(&c)
	return uuidpb.MustParseAsByteArray(c.key)
}

var _ dogma.ProjectionConfigurer = (*configurer)(nil)

type configurable interface {
	Configure(dogma.ProjectionConfigurer)
}

type configurer struct {
	key string
}

func (c *configurer) Identity(_ string, key string)   { c.key = key }
func (c *configurer) Routes(...dogma.ProjectionRoute) {}
func (c *configurer) Disable(...dogma.DisableOption)  {}
