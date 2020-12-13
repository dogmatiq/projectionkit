package sqlprojection

import (
	"context"
	"database/sql"
	"fmt"
	"sync/atomic"

	"github.com/dogmatiq/cosyne"
	"go.uber.org/multierr"
)

// SelectDriver returns the appropriate driver implementation to use with the
// given database from a list of candidate drivers.
func SelectDriver(ctx context.Context, db *sql.DB, candidates []Driver) (Driver, error) {
	var err error

	for _, d := range candidates {
		e := d.IsCompatibleWith(ctx, db)
		if e == nil {
			return d, nil
		}

		err = multierr.Append(err, fmt.Errorf(
			"%T is not compatible with %T: %w",
			d,
			db.Driver(),
			e,
		))
	}

	return nil, multierr.Append(err, fmt.Errorf(
		"could not find a driver that is compatible with %T",
		db.Driver(),
	))
}

// An Option configures the optional behavior of an SQL projection.
type Option interface {
	applyCandidateSetOption(*candidateSet)
}

type adaptorOptionFunc func(*candidateSet)

func (f adaptorOptionFunc) applyCandidateSetOption(s *candidateSet) {
	f(s)
}

// WithDriver returns an Option that forces use of a specific Driver.
//
// It takes precedence over any WithCandidateDriver() option.
func WithDriver(d Driver) Option {
	return adaptorOptionFunc(func(s *candidateSet) {
		s.resolved = 1
		s.candidates = []Driver{d}
	})
}

// WithCandidateDrivers returns an Option that adds candidate drivers for
// selection as the driver to use.
func WithCandidateDrivers(drivers ...Driver) Option {
	return adaptorOptionFunc(func(s *candidateSet) {
		if s.resolved == 0 {
			s.candidates = append(s.candidates, drivers...)
		}
	})
}

// candidateSet is a set of drivers that are candidates for use with a
// particular database.
type candidateSet struct {
	m          cosyne.Mutex
	resolved   uint32
	db         *sql.DB
	candidates []Driver
}

// init sets up the candidate set.
//
// If options is empty the default options are applied.
func (s *candidateSet) init(db *sql.DB, options []Option) {
	s.db = db

	if len(options) == 0 {
		options = []Option{
			WithCandidateDrivers(BuiltInDrivers()...),
		}
	}

	for _, opt := range options {
		opt.applyCandidateSetOption(s)
	}
}

// resolve selects the appropriate driver from the candidates.
func (s *candidateSet) resolve(ctx context.Context) (Driver, error) {
	if atomic.LoadUint32(&s.resolved) == 0 {
		// If the resolved flag is 0 then a.selected has not been populated yet.
		// We acquire the mutex to ensure we're the only goroutine attempting
		// selection.
		if err := s.m.Lock(ctx); err != nil {
			return nil, err
		}
		defer s.m.Unlock()

		// Ensure that no another goroutine selected the driver while we were
		// waiting to acquire the mutex.
		if atomic.LoadUint32(&s.resolved) == 0 {
			// If not, it's our turn to try selection.
			d, err := SelectDriver(ctx, s.db, s.candidates)
			if err != nil {
				return nil, err
			}

			s.db = nil
			s.candidates = []Driver{d}
			atomic.StoreUint32(&s.resolved, 1)
		}
	}

	return s.candidates[0], nil
}
