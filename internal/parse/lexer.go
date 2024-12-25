package parse

import (
	"bufio"
	"io"
	"strings"
	"unicode"
)

var keywords = []string{"select", "from", "where", "and", "insert", "into", "values", "delete", "update", "set", "create", "table", "int", "varchar", "view", "as", "index", "on"}

type TokenType string

type Token struct {
	Type    TokenType
	Literal string
}

func NewToken(t TokenType, l string) Token {
	return Token{Type: t, Literal: l}
}

func (t Token) String() string {
	if t.Type == EOF {
		return "EOF"
	} else if t.Type == LexerError {
		return "lexing error: " + t.Literal
	} else if t.Type == Int {
		return t.Literal
	} else if t.Type == String {
		return t.Literal
	} else if t.Type == Keyword {
		return t.Literal
	} else if t.Type == Identifier {
		return t.Literal
	} else if t.Type == Equal {
		return "="
	} else if t.Type == Comma {
		return ","
	} else if t.Type == OpenParen {
		return "("
	} else if t.Type == CloseParen {
		return ")"
	}
	return t.Literal
}

type Lexer struct {
	reader *bufio.Reader
}

const (
	EOF        TokenType = "EOF"
	Int        TokenType = "INT"
	String     TokenType = "STRING"
	Keyword    TokenType = "KEYWORD"
	Identifier TokenType = "IDENTIFIER"
	Equal      TokenType = "EQUAL"
	Comma      TokenType = "COMMA"
	OpenParen  TokenType = "OPEN_PAREN"
	CloseParen TokenType = "CLOSE_PAREN"
	LexerError TokenType = "LEXER_ERROR" // used for syntax errors
)

func NewLexer(query string) *Lexer {
	return &Lexer{bufio.NewReader(strings.NewReader(query))}
}

func (l *Lexer) peek() byte {
	ch, err := l.reader.Peek(1)
	if err != nil {
		if err == io.EOF {
			return 0
		}
		panic(err)
	}
	return ch[0]
}

func (l *Lexer) readChar() byte {
	ch, err := l.reader.ReadByte()
	if err != nil {
		if err == io.EOF {
			return 0
		}
		panic(err)
	}
	return ch
}

func (l *Lexer) readInt() (string, error) {
	var sb strings.Builder
	ch := l.peek()
	for ch >= '0' && ch <= '9' {
		sb.WriteByte(l.readChar())
		ch = l.peek()
	}
	return sb.String(), nil
}

func (l *Lexer) readString() (string, error) {
	var sb strings.Builder
	l.readChar()
	ch := l.peek()
	for ch != '\'' && ch != 0 {
		sb.WriteByte(l.readChar())
		ch = l.peek()
	}
	if ch == 0 {
		return "", NewSyntaxError("unterminated string")
	}
	l.readChar() // consume the closing '
	return sb.String(), nil
}

func (l *Lexer) readIdentifier() (string, error) {
	var sb strings.Builder
	ch := l.peek()
	for isLetter(ch) || (ch >= '0' && ch <= '9') {
		sb.WriteByte(l.readChar())
		ch = l.peek()
	}
	return sb.String(), nil
}

func (l *Lexer) skipWhitespace() {
	for {
		ch := l.peek()
		if ch == 0 {
			break
		}
		if !unicode.IsSpace(rune(ch)) {
			break
		}
		l.readChar()
	}
}

func (l *Lexer) NextToken() Token {
	var t Token

	l.skipWhitespace()
	ch := l.peek()
	if ch == 0 {
		return NewToken(EOF, "")
	} else if ch == ',' {
		t = NewToken(Comma, ",")
	} else if ch == '=' {
		t = NewToken(Equal, "=")
	} else if ch == '(' {
		t = NewToken(OpenParen, "(")
	} else if ch == ')' {
		t = NewToken(CloseParen, ")")
	} else if ch >= '0' && ch <= '9' {
		i, err := l.readInt()
		if err != nil {
			return NewToken(LexerError, err.Error())
		}
		t = NewToken(Int, i)
		return t
	} else if ch == '\'' {
		s, err := l.readString()
		if err != nil {
			return NewToken(LexerError, err.Error())
		}
		t = NewToken(String, s)
		return t
	} else if isLetter(ch) {
		s, err := l.readIdentifier()
		if err != nil {
			return NewToken(LexerError, err.Error())
		}
		if isKeyword(s) {
			t = NewToken(Keyword, s)
		} else {
			t = NewToken(Identifier, s)
		}
		return t
	}
	l.readChar()
	return t
}

func isLetter(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

func isKeyword(s string) bool {
	for _, keyword := range keywords {
		if strings.ToLower(s) == keyword {
			return true
		}
	}
	return false
}
