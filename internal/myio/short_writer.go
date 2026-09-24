package myio

import "io"

// shortWriter violates the io.Writer contract on purpose: it reports
// n < len(p) with a nil error. io.Copy against it returns io.ErrShortWrite,
// which makes it useful to test how callers handle short writes.
type shortWriter struct{}

// ShortWriter returns an io.Writer that always short-writes.
func ShortWriter() io.Writer {
	return &shortWriter{}
}

func (w *shortWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	return len(p) - 1, nil
}
