package echoform

import (
	"context"
	"errors"
	"io"
	"mime"
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/mazrean/formstream"
)

type Parser struct {
	*formstream.Parser
	reader io.Reader
}

func NewParser(c echo.Context, options ...formstream.ParserOption) (*Parser, error) {
	contentType := c.Request().Header.Get("Content-Type")
	d, params, err := mime.ParseMediaType(contentType)
	if err != nil || d != "multipart/form-data" {
		return nil, http.ErrNotMultipart
	}

	boundary, ok := params["boundary"]
	if !ok {
		return nil, http.ErrMissingBoundary
	}

	return &Parser{
		Parser: formstream.NewParser(boundary, options...),
		reader: c.Request().Body,
	}, nil
}

// Parse parses the request body.
// It returns the echo.HTTPError if the hook function returns an echo.HTTPError.
func (p *Parser) Parse() error {
	err := p.Parser.Parse(p.reader)
	return p.mapError(err)
}

// ParseWithContext parses the request body until ctx is cancelled.
// It returns the echo.HTTPError if the hook function returns an echo.HTTPError.
func (p *Parser) ParseWithContext(ctx context.Context) error {
	err := p.Parser.ParseWithContext(ctx, p.reader)
	return p.mapError(err)
}

func (p *Parser) mapError(err error) error {
	var httpErr *echo.HTTPError
	if errors.As(err, &httpErr) {
		return httpErr
	}

	return err
}
