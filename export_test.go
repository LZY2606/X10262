package formstream

// This file exposes read-only views of internal state for tests only.
// It is compiled exclusively into the test binary and is not part of the
// public API. Nothing here duplicates production logic; every accessor
// observes the same state the production parse path mutates.

// LiveCancelableReadersForTest reports how many cancelableReader instances
// still have goroutines running. Tests use it to prove that cancellation
// and parse completion leave no goroutine or pipe behind.
func LiveCancelableReadersForTest() int64 {
	return liveCancelableReaders.Load()
}

// ParserBudgetsForTest returns the remaining resource budgets of p. Tests
// use it to prove that a failed parse leaves no hidden residue behind a
// later successful parse built from identical inputs.
func ParserBudgetsForTest(p *Parser) (maxParts, maxHeaders uint, maxMemSize, maxMemFileSize DataSize) {
	return p.maxParts, p.maxHeaders, p.maxMemSize, p.maxMemFileSize
}
