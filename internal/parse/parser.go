package parse

import (
	"fmt"
	"simpledb/internal/query"
	"simpledb/internal/record"
	"strconv"
	"strings"
)

type Parser struct {
	lex     *Lexer
	curTok  Token
	prevTok Token
}

func NewParser(lex *Lexer) *Parser {
	p := &Parser{lex: lex}
	p.nextToken()
	return p
}

func (p *Parser) nextToken() {
	p.prevTok = p.curTok
	p.curTok = p.lex.NextToken()
}

func (p *Parser) matchInt() bool {
	return p.curTok.Type == Int
}

func (p *Parser) matchString() bool {
	return p.curTok.Type == String
}

func (p *Parser) matchId() bool {
	return p.curTok.Type == Identifier
}

func (p *Parser) matchKeyword(keyword string) bool {
	return p.curTok.Type == Keyword && strings.ToLower(p.curTok.Literal) == keyword
}

func (p *Parser) matchDelim(delim TokenType) bool {
	return p.curTok.Type == delim
}

func (p *Parser) eatInt() (int32, error) {
	if !p.matchInt() {
		return 0, NewSyntaxError("expected integer")
	}
	p.nextToken()
	val, err := strconv.ParseInt(p.prevTok.Literal, 10, 32)
	if err != nil {
		return 0, NewSyntaxError(fmt.Sprintf("invalid integer constant: %s", p.curTok.Literal))
	}
	return int32(val), nil
}

func (p *Parser) eatString() (string, error) {
	if !p.matchString() {
		return "", NewSyntaxError("expected string")
	}
	p.nextToken()
	return p.prevTok.Literal, nil
}

func (p *Parser) eatId() (string, error) {
	if !p.matchId() {
		return "", NewSyntaxError("expected identifier")
	}
	p.nextToken()
	return p.prevTok.Literal, nil
}

func (p *Parser) eatKeyword(keyword string) error {
	if !p.matchKeyword(keyword) {
		return NewSyntaxError(fmt.Sprintf("expected keyword %s", keyword))
	}
	p.nextToken()
	return nil
}

func (p *Parser) eatDelim(delim TokenType) error {
	if !p.matchDelim(delim) {
		return NewSyntaxError(fmt.Sprintf("expected delimiter %s after token %s", delim, p.prevTok.String()))
	}
	p.nextToken()
	return nil
}

func (p *Parser) Field() (string, error) {
	return p.eatId()
}

func (p *Parser) Constant() (record.Constant, error) {
	if p.matchString() {
		s, err := p.eatString()
		if err != nil {
			return record.Constant{}, err
		}
		return record.NewStringConstant(s), nil
	} else if p.matchInt() {
		val, err := p.eatInt()
		if err != nil {
			return record.Constant{}, err
		}
		return record.NewIntConstant(val), nil
	}
	return record.Constant{}, NewSyntaxError("expected integer or string constant")
}

func (p *Parser) Expression() (query.Expression, error) {
	if p.matchId() {
		field, err := p.Field()
		if err != nil {
			return query.Expression{}, err
		}
		return query.NewFieldExpression(field), nil
	}
	constant, err := p.Constant()
	if err != nil {
		return query.Expression{}, err
	}
	return query.NewConstantExpression(constant), nil
}

func (p *Parser) Term() (*query.Term, error) {
	lhs, err := p.Expression()
	if err != nil {
		return nil, err
	}
	if err := p.eatDelim(Equal); err != nil {
		return nil, err
	}
	rhs, err := p.Expression()
	if err != nil {
		return nil, err
	}
	return query.NewTerm(lhs, rhs), nil
}

func (p *Parser) Predicate() (*query.Predicate, error) {
	term, err := p.Term()
	if err != nil {
		return nil, err
	}
	pred := query.NewPredicate([]*query.Term{term})
	if p.matchKeyword("and") {
		p.nextToken()
		right, err := p.Predicate()
		if err != nil {
			return nil, err
		}
		pred.ConjoinWith(right)
	}
	return pred, nil
}

func (p *Parser) Query() (*QueryData, error) {
	if err := p.eatKeyword("select"); err != nil {
		return nil, err
	}
	fields, err := p.selectList()
	if err != nil {
		return nil, err
	}
	if err := p.eatKeyword("from"); err != nil {
		return nil, err
	}
	tables, err := p.tableList()
	if err != nil {
		return nil, err
	}
	pred := query.NewPredicate(nil)
	if p.matchKeyword("where") {
		p.nextToken()
		pred, err = p.Predicate()
		if err != nil {
			return nil, err
		}
	}
	return NewQueryData(fields, tables, pred), nil
}

func (p *Parser) UpdateCmd() (interface{}, error) {
	if p.matchKeyword("insert") {
		return p.Insert()
	} else if p.matchKeyword("update") {
		return p.Update()
	} else if p.matchKeyword("delete") {
		return p.Delete()
	} else if p.matchKeyword("create") {
		return p.Create()
	}
	return nil, NewSyntaxError("expected insert, update, delete, or create")
}

func (p *Parser) Create() (interface{}, error) {
	if err := p.eatKeyword("create"); err != nil {
		return nil, err
	}
	if p.matchKeyword("table") {
		return p.CreateTable()
	} else if p.matchKeyword("view") {
		return p.CreateView()
	} else if p.matchKeyword("index") {
		return p.CreateIndex()
	}
	return nil, NewSyntaxError("expected table, view, or index")
}

func (p *Parser) Delete() (*DeleteData, error) {
	if err := p.eatKeyword("delete"); err != nil {
		return nil, err
	}
	if err := p.eatKeyword("from"); err != nil {
		return nil, err
	}
	tblname, err := p.eatId()
	if err != nil {
		return nil, err
	}
	pred := query.NewPredicate(nil)
	if p.matchKeyword("where") {
		p.nextToken()
		pred, err = p.Predicate()
		if err != nil {
			return nil, err
		}
	}
	return NewDeleteData(tblname, pred), nil
}

func (p *Parser) Insert() (*InsertData, error) {
	if err := p.eatKeyword("insert"); err != nil {
		return nil, err
	}
	if err := p.eatKeyword("into"); err != nil {
		return nil, err
	}
	tblname, err := p.eatId()
	if err != nil {
		return nil, err
	}
	if err := p.eatDelim(OpenParen); err != nil {
		return nil, err
	}
	fields, err := p.fieldList()
	if err != nil {
		return nil, err
	}
	if err := p.eatDelim(CloseParen); err != nil {
		return nil, err
	}
	if err := p.eatKeyword("values"); err != nil {
		return nil, err
	}
	if err := p.eatDelim(OpenParen); err != nil {
		return nil, err
	}
	values, err := p.constList()
	if err != nil {
		return nil, err
	}
	if err := p.eatDelim(CloseParen); err != nil {
		return nil, err
	}
	return NewInsertData(tblname, fields, values), nil
}

func (p *Parser) Update() (*UpdateData, error) {
	if err := p.eatKeyword("update"); err != nil {
		return nil, err
	}
	tblname, err := p.eatId()
	if err != nil {
		return nil, err
	}
	if err := p.eatKeyword("set"); err != nil {
		return nil, err
	}
	fldname, err := p.Field()
	if err != nil {
		return nil, err
	}
	if err := p.eatDelim(Equal); err != nil {
		return nil, err
	}
	newval, err := p.Expression()
	if err != nil {
		return nil, err
	}
	pred := query.NewPredicate(nil)
	if p.matchKeyword("where") {
		p.nextToken()
		pred, err = p.Predicate()
		if err != nil {
			return nil, err
		}
	}
	return NewUpdateData(tblname, fldname, newval, pred), nil
}

func (p *Parser) CreateTable() (*CreateTableData, error) {
	if err := p.eatKeyword("table"); err != nil {
		return nil, err
	}
	tblname, err := p.eatId()
	if err != nil {
		return nil, err
	}
	if err := p.eatDelim(OpenParen); err != nil {
		return nil, err
	}
	sch, err := p.fieldDefs()
	if err != nil {
		return nil, err
	}
	if err := p.eatDelim(CloseParen); err != nil {
		return nil, err
	}
	return NewCreateTableData(tblname, sch), nil
}

func (p *Parser) CreateView() (*CreateViewData, error) {
	if err := p.eatKeyword("view"); err != nil {
		return nil, err
	}
	viewname, err := p.eatId()
	if err != nil {
		return nil, err
	}
	if err := p.eatKeyword("as"); err != nil {
		return nil, err
	}
	query, err := p.Query()
	if err != nil {
		return nil, err
	}
	return NewCreateViewData(viewname, query), nil
}

func (p *Parser) CreateIndex() (*CreateIndexData, error) {
	if err := p.eatKeyword("index"); err != nil {
		return nil, err
	}
	indexname, err := p.eatId()
	if err != nil {
		return nil, err
	}
	if err := p.eatKeyword("on"); err != nil {
		return nil, err
	}
	tblname, err := p.eatId()
	if err != nil {
		return nil, err
	}
	if err := p.eatDelim(OpenParen); err != nil {
		return nil, err
	}
	fieldname, err := p.Field()
	if err != nil {
		return nil, err
	}
	if err := p.eatDelim(CloseParen); err != nil {
		return nil, err
	}
	return NewCreateIndexData(indexname, tblname, fieldname), nil
}

func (p *Parser) selectList() ([]string, error) {
	fields := []string{}
	for {
		field, err := p.Field()
		if err != nil {
			return nil, err
		}
		fields = append(fields, field)
		if !p.matchDelim(Comma) {
			break
		}
		p.nextToken()
	}
	return fields, nil
}

func (p *Parser) tableList() ([]string, error) {
	tables := []string{}
	for {
		table, err := p.eatId()
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
		if !p.matchDelim(Comma) {
			break
		}
		p.nextToken()
	}
	return tables, nil
}

func (p *Parser) fieldList() ([]string, error) {
	fields := []string{}
	for {
		field, err := p.Field()
		if err != nil {
			return nil, err
		}
		fields = append(fields, field)
		if !p.matchDelim(Comma) {
			break
		}
		p.nextToken()
	}
	return fields, nil
}

func (p *Parser) constList() ([]record.Constant, error) {
	values := []record.Constant{}
	for {
		constant, err := p.Constant()
		if err != nil {
			return nil, err
		}
		values = append(values, constant)
		if !p.matchDelim(Comma) {
			break
		}
		p.nextToken()
	}
	return values, nil
}

func (p *Parser) fieldDefs() (*record.Schema, error) {
	sch, err := p.fieldDef()
	if err != nil {
		return nil, err
	}
	for p.matchDelim(Comma) {
		p.nextToken()
		sch2, err := p.fieldDef()
		if err != nil {
			return nil, err
		}
		sch.AddAll(sch2)
	}
	return sch, nil
}

func (p *Parser) fieldDef() (*record.Schema, error) {
	fldname, err := p.Field()
	if err != nil {
		return nil, err
	}
	return p.fieldType(fldname)
}

func (p *Parser) fieldType(fldname string) (*record.Schema, error) {
	sch := record.NewSchema()
	if p.matchKeyword("int") {
		p.nextToken()
		sch.AddIntField(fldname)
	} else if p.matchKeyword("varchar") {
		p.nextToken()
		if err := p.eatDelim(OpenParen); err != nil {
			return nil, err
		}
		length, err := p.eatInt()
		if err != nil {
			return nil, err
		}
		if err := p.eatDelim(CloseParen); err != nil {
			return nil, err
		}
		sch.AddStringField(fldname, int(length))
	} else {
		return nil, NewSyntaxError("expected int or varchar")
	}
	return sch, nil
}
