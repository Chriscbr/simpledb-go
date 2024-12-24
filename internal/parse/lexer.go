package parse

import (
	"bufio"
	"fmt"
	"io"
	"strings"
	"unicode"
)

type Type string

type Token struct {
	Type    Type
	Literal string
}

func NewToken(t Type, l string) Token {
	return Token{Type: t, Literal: l}
}

type Lexer struct {
	reader *bufio.Reader
}

const (
	EOF        Type = "EOF"
	Comma      Type = "COMMA"
	Int        Type = "INT"
	String     Type = "STRING"
	Identifier Type = "IDENTIFIER"
	Equal      Type = "="
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
	return sb.String(), nil
}

func (l *Lexer) readIdentifier() (string, error) {
	var sb strings.Builder
	ch := l.peek()
	for isLetter(ch) {
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

func (l *Lexer) NextToken() (Token, error) {
	var t Token

	l.skipWhitespace()
	ch := l.peek()
	fmt.Println("peeking", fmt.Sprintf("%c", ch))
	if ch == 0 {
		return NewToken(EOF, ""), nil
	} else if ch == ',' {
		t = NewToken(Comma, ",")
	} else if ch == '=' {
		t = NewToken(Equal, "=")
	} else if ch >= '0' && ch <= '9' {
		t = NewToken(Int, string(ch))
	} else if ch == '\'' {
		s, err := l.readString()
		if err != nil {
			return Token{}, err
		}
		t = NewToken(String, s)
	} else if isLetter(ch) {
		s, err := l.readIdentifier()
		if err != nil {
			return Token{}, err
		}
		t = NewToken(Identifier, s)
		return t, nil
	}
	l.readChar()
	return t, nil
}

func isLetter(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}
