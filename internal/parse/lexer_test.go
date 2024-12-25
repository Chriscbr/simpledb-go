package parse

import "testing"

func TestLexer1(t *testing.T) {
	lexer := NewLexer("1,2,3")
	checkToken(t, lexer, Int, "1")
	checkToken(t, lexer, Comma, ",")
	checkToken(t, lexer, Int, "2")
	checkToken(t, lexer, Comma, ",")
	checkToken(t, lexer, Int, "3")
	checkToken(t, lexer, EOF, "")
}

func TestLexer2(t *testing.T) {
	lexer := NewLexer("abc='abc',bar='bar'")
	checkToken(t, lexer, Identifier, "abc")
	checkToken(t, lexer, Equal, "=")
	checkToken(t, lexer, String, "abc")
	checkToken(t, lexer, Comma, ",")
	checkToken(t, lexer, Identifier, "bar")
	checkToken(t, lexer, Equal, "=")
	checkToken(t, lexer, String, "bar")
	checkToken(t, lexer, EOF, "")
}

func TestLexer3(t *testing.T) {
	lexer := NewLexer("select id from foo where bar = 'baz'")
	checkToken(t, lexer, Keyword, "select")
	checkToken(t, lexer, Identifier, "id")
	checkToken(t, lexer, Keyword, "from")
	checkToken(t, lexer, Identifier, "foo")
	checkToken(t, lexer, Keyword, "where")
	checkToken(t, lexer, Identifier, "bar")
	checkToken(t, lexer, Equal, "=")
	checkToken(t, lexer, String, "baz")
	checkToken(t, lexer, EOF, "")
}

func checkToken(t *testing.T, lexer *Lexer, typ TokenType, lit string) {
	token := lexer.NextToken()
	if token.Literal != lit {
		t.Fatalf("expected %s, got %s", lit, token.Literal)
	}
	if token.Type != typ {
		t.Fatalf("expected %s, got %s", typ, token.Type)
	}
}
