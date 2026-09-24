package httpform

import (
	"context"
	"io"
	"mime"
	"net/http"

	"github.com/mazrean/formstream"
)

type Parser struct {
	*formstream.Parser
	reader io.Reader
}

func NewParser(req *http.Request, options ...formstream.ParserOption) (*Parser, error) {
	contentType := req.Header.Get("Content-Type")
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
		reader: req.Body,
	}, nil
}

func (p *Parser) Parse() error {
	return p.Parser.Parse(p.reader)
}

// ParseContext parses the request body, honoring ctx for cancellation.
func (p *Parser) ParseContext(ctx context.Context) error {
	return p.Parser.ParseContext(ctx, p.reader)
}
