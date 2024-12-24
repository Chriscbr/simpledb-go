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

func checkToken(t *testing.T, lexer *Lexer, typ Type, lit string) {
	token, err := lexer.NextToken()
	if err != nil {
		t.Fatal(err)
	}
	if token.Literal != lit {
		t.Fatalf("expected %s, got %s", lit, token.Literal)
	}
	if token.Type != typ {
		t.Fatalf("expected %s, got %s", typ, token.Type)
	}
}
