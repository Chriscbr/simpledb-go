package query

import "simpledb/internal/record"

type Expression struct {
	val     *record.Constant // using pointer to represent nullable constant
	fldname *string          // using pointer to represent nullable string
}

// NewConstantExpression creates a new expression that evaluates to a constant value.
func NewConstantExpression(val record.Constant) Expression {
	return Expression{val: &val}
}

// NewFieldExpression creates a new expression that evaluates to the value of a field.
func NewFieldExpression(fldname string) Expression {
	return Expression{fldname: &fldname}
}

// Evaluate evaluates the expression with respect to the
// current record of the specified scan.
func (e Expression) Evaluate(s record.Scan) (record.Constant, error) {
	if e.val != nil {
		return *e.val, nil
	} else {
		return s.GetVal(*e.fldname)
	}
}

// Returns the constant corresponding to the constant expression,
// or nil if the expression is a field reference.
func (e Expression) Constant() *record.Constant {
	return e.val
}

// Returns the field name corresponding to the field reference expression,
// or nil if the expression is a constant expression.
func (e Expression) FieldName() *string {
	return e.fldname
}

// AppliesTo determines if all of hte fields mentioned in this expression
// are contained in the specified schema.
func (e Expression) AppliesTo(sch *record.Schema) bool {
	if e.val != nil {
		return true
	} else {
		return sch.HasField(*e.fldname)
	}
}

// String returns a string representation of this expression.
func (e Expression) String() string {
	if e.val != nil {
		return e.val.String()
	} else {
		return *e.fldname
	}
}
