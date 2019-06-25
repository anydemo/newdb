package newdb

import "fmt"

// Op enum of Op
type Op int

const (
	// OpEquals ==
	OpEquals Op = iota
	// OpGreaterThan >
	OpGreaterThan
	// OpLessThan <
	OpLessThan
	// OpGreaterThanOrEq >=
	OpGreaterThanOrEq
	// OpLessThanOrEq <=
	OpLessThanOrEq
	// OpLike LIKE
	OpLike
	// OpNotEquals !=
	OpNotEquals
)

func (op Op) String() (ret string) {
	switch op {
	case OpEquals:
		ret = "="
	case OpGreaterThan:
		ret = ">"
	case OpLessThan:
		ret = "<"
	case OpLessThanOrEq:
		ret = "<="
	case OpGreaterThanOrEq:
		ret = ">="
	case OpLike:
		ret = "LIKE"
	case OpNotEquals:
		ret = "!="
	default:
		ret = "UnsupportedOp"
	}
	return
}

// Predicate compares tuples to a specified Field value
type Predicate struct {
	Field   int
	Op      Op
	Operand Field
}

// Filter compares the field number of t specified in the constructor to the
// operand field specified in the constructor using the operator specific in
// the constructor. The comparison can be made through Field's compare
// method.
func (p Predicate) Filter(tuple *Tuple) bool {
	if p.Field >= len(tuple.Fields) {
		return false
	}
	return tuple.Fields[p.Field].Compare(p.Op, p.Operand)
}

func (p Predicate) String() string {
	return fmt.Sprintf("f=%v\top=%v\toperand=%v", p.Field, p.Op.String(), p.Operand.String())
}

// OpIterator operation iterator interface
type OpIterator interface {
	// Open opens the iterator. This must be called before any of the other methods.
	Open() error
	// Close Closes the iterator. When the iterator is closed, calling next(), hasNext(), or rewind() should return error
	Close()
	// HasNext Returns true if the iterator has more tuples.
	HasNext() bool
	// Next Returns the next tuple from the operator (typically implementing by reading
	// from a child operator or an access method).
	Next() *Tuple
	// Rewind Resets the iterator to the start.
	Rewind() error
	// TupleDesc Returns the TupleDesc associated with this OpIterator.
	TupleDesc() *TupleDesc
	Error() error
}

var _ OpIterator = (*Filter)(nil)

// Filter is an operator that implements a relational projection.
type Filter struct {
	Child OpIterator
	Pred  *Predicate

	open bool
	next *Tuple

	Err error
}

// NewFilter create new filter
func NewFilter(predicate *Predicate, child OpIterator) *Filter {
	return &Filter{Child: child, Pred: predicate}
}
func (f *Filter) Error() error {
	return f.Err
}

// Open open iterator
// see #OpIterator
func (f *Filter) Open() error {
	f.open = true
	return nil
}

// Close close iterator
func (f *Filter) Close() {
	f.open = false
	f.next = nil
}

// HasNext if has next elem
func (f *Filter) HasNext() (ret bool) {
	if !f.open {
		f.Err = fmt.Errorf("Operator not yet open")
		return false
	}
	if f.next == nil {
		f.next, f.Err = f.fetchNext()
	}
	return f.next != nil
}

func (f *Filter) fetchNext() (*Tuple, error) {
	for f.Child.HasNext() {
		if f.Err != nil {
			return nil, f.Err
		}
		tuple := f.Child.Next()
		if err := f.Error(); err != nil {
			return nil, err
		}
		if f.Pred.Filter(tuple) {
			return tuple, f.Error()
		}
	}
	return nil, nil
}

// Next next tuple
func (f *Filter) Next() (ret *Tuple) {
	if f.next == nil {
		f.next, f.Err = f.fetchNext()
		if err := f.Error(); err != nil {
			return
		}
		if f.next == nil {
			f.Err = fmt.Errorf("no such element")
		}
	}
	ret = f.next
	f.next = nil
	return
}

// Rewind restart the iterator
func (f *Filter) Rewind() error {
	f.Close()
	return f.Open()
}

// TupleDesc returns the TupleDesc associated with this OpIterator.
func (f Filter) TupleDesc() *TupleDesc {
	return f.Child.TupleDesc()
}
