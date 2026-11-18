package groupcache

import "errors"

// ErrNotFound is returned by a Getter when the requested key does not
// exist in the underlying data source.
//
// Returning ErrNotFound (directly or wrapped) tells groupcache that the
// lookup completed successfully but produced no value. The error is
// propagated to callers unchanged, is not cached, and is not retried
// against a different peer. A Getter must not use an empty value to
// signal a missing key: empty values are valid cached values.
type ErrNotFound struct {
	Msg string
}

func (e *ErrNotFound) Error() string {
	if e == nil || e.Msg == "" {
		return "groupcache: not found"
	}
	return e.Msg
}

// Is allows errors.Is(err, &ErrNotFound{}) to match any ErrNotFound,
// regardless of its message.
func (e *ErrNotFound) Is(target error) bool {
	_, ok := target.(*ErrNotFound)
	return ok
}

// IsNotFound reports whether err indicates that the requested key does
// not exist.
func IsNotFound(err error) bool {
	return errors.Is(err, &ErrNotFound{})
}
