package identity

import (
	"uuid"

	"github.com/dogmatiq/dogma"
)

// Key returns a handler's unique key.
func Key(h configurable) uuid.UUID {
	var c configurer
	h.Configure(&c)
	return uuid.MustParse(c.key)
}

var _ dogma.ProjectionConfigurer = (*configurer)(nil)

type configurable interface {
	Configure(dogma.ProjectionConfigurer)
}

type configurer struct {
	key string
}

func (c *configurer) Identity(_ string, key string)                     { c.key = key }
func (c *configurer) Routes(...dogma.ProjectionRoute)                   {}
func (c *configurer) Disable(...dogma.DisableOption)                    {}
func (c *configurer) ConcurrencyPreference(dogma.ConcurrencyPreference) {}
