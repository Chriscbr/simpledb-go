package query

import (
	"fmt"
	"simpledb/internal/record"
)

// Term is a comparison between two expressions.
type Term struct {
	lhs Expression
	rhs Expression
}

// NewTerm creates a new term that compares two expressions for equality.
func NewTerm(lhs Expression, rhs Expression) *Term {
	return &Term{lhs: lhs, rhs: rhs}
}

// IsSatisfied returns true if both of the term's expressions evaluate
// to the same constant, with respect to the specified scan.
func (t *Term) IsSatisfied(s record.Scan) (bool, error) {
	lhsval, err := t.lhs.Evaluate(s)
	if err != nil {
		return false, err
	}
	rhsval, err := t.rhs.Evaluate(s)
	if err != nil {
		return false, err
	}
	return lhsval.Equal(rhsval), nil
}

// ReductionFactor calculates the extent to which selecting on the term
// reduces the number of records output by a query.
// For example if the reduction factor is 2, then the
// term cuts the size of the output in half.
func (t *Term) ReductionFactor(p Plan) (int, error) {
	if t.lhs.FieldName() != nil && t.rhs.FieldName() != nil {
		lhsname := *t.lhs.FieldName()
		rhsname := *t.rhs.FieldName()
		return max(p.DistinctValues(lhsname), p.DistinctValues(rhsname)), nil
	}
	if t.lhs.FieldName() != nil {
		lhsname := *t.lhs.FieldName()
		return p.DistinctValues(lhsname), nil
	}
	if t.rhs.FieldName() != nil {
		rhsname := *t.rhs.FieldName()
		return p.DistinctValues(rhsname), nil
	}
	// otherwise, the term equates two constants
	if t.lhs.Constant().Equal(*t.rhs.Constant()) {
		return 1, nil
	}
	return 0, fmt.Errorf("cannot calculate reduction factor for term %s", t.String())
}

// EquatesWithConstant determines if this term is of the form "F=c"
// where F is the specified field and c is some constant.
// If so, the method returns that constant, otherwise it returns nil.
func (t *Term) EquatesWithConstant(fldname string) *record.Constant {
	if t.lhs.FieldName() != nil && *t.lhs.FieldName() == fldname && t.rhs.FieldName() == nil {
		return t.rhs.Constant()
	}
	if t.rhs.FieldName() != nil && *t.rhs.FieldName() == fldname && t.lhs.FieldName() == nil {
		return t.lhs.Constant()
	}
	return nil
}

// EquatesWithField determines if this term is of the form "F1=F2"
// where F1 is the specified field and F2 is some other field.
// If so, the method returns that other field, otherwise it returns nil.
func (t *Term) EquatesWithField(fldname string) *string {
	if t.lhs.FieldName() != nil && *t.lhs.FieldName() == fldname && t.rhs.FieldName() != nil {
		return t.rhs.FieldName()
	}
	if t.rhs.FieldName() != nil && *t.rhs.FieldName() == fldname && t.lhs.FieldName() != nil {
		return t.lhs.FieldName()
	}
	return nil
}

// AppliesTo returns true if both of the term's expressions apply to the
// specified schema.
func (t *Term) AppliesTo(sch *record.Schema) bool {
	return t.lhs.AppliesTo(sch) && t.rhs.AppliesTo(sch)
}

// String returns a string representation of this term.
func (t *Term) String() string {
	return fmt.Sprintf("%s = %s", t.lhs.String(), t.rhs.String())
}
