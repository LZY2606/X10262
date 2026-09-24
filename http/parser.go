package httpform

import "context"
import (
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

// ParseWithContext parses the request body until ctx is cancelled.
func (p *Parser) ParseWithContext(ctx context.Context) error {
	return p.Parser.ParseWithContext(ctx, p.reader)
}
