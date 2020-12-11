package sqlprojection

// An Option configures the optional behavior of an SQL projection.
type Option interface {
	applyAdaptorOption(*adaptor)
}

type adaptorOptionFunc func(*adaptor)

func (f adaptorOptionFunc) applyAdaptorOption(a *adaptor) {
	f(a)
}

// WithDriver returns an Option that forces use of a specific Driver.
//
// It takes precedence over any WithCandidateDriver() option.
func WithDriver(d Driver) Option {
	return adaptorOptionFunc(func(a *adaptor) {
		a.resolved = 1
		a.candidates = nil
		a.selected = d
	})
}

// WithCandidateDriver returns an Option that adds d as a candidate for
// selection as the driver to use.
func WithCandidateDriver(d Driver) Option {
	return adaptorOptionFunc(func(a *adaptor) {
		if a.resolved == 0 {
			a.candidates = append(a.candidates, d)
		}
	})
}
