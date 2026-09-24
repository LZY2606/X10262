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
	return p.unwrapHTTPError(p.Parser.Parse(p.reader))
}

// ParseContext parses the request body, honoring ctx for cancellation.
// It returns the echo.HTTPError if the hook function returns an echo.HTTPError.
func (p *Parser) ParseContext(ctx context.Context) error {
	return p.unwrapHTTPError(p.Parser.ParseContext(ctx, p.reader))
}

func (p *Parser) unwrapHTTPError(err error) error {
	var httpErr *echo.HTTPError
	if errors.As(err, &httpErr) {
		return httpErr
	}

	return err
}
