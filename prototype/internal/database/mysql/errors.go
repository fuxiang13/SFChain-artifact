package mysql

import (
	"fmt"
)

// DatabaseError represents a database error
type DatabaseError struct {
	Operation string
	Err       error
}

func (de *DatabaseError) Error() string {
	return fmt.Sprintf("Database operation '%s' failed: %v", de.Operation, de.Err)
}

func (de *DatabaseError) Unwrap() error {
	return de.Err
}

// NewDatabaseError creates a database error
func NewDatabaseError(operation string, err error) *DatabaseError {
	return &DatabaseError{
		Operation: operation,
		Err:       err,
	}
}

// WrapDatabaseError wraps a database error
func WrapDatabaseError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return &DatabaseError{
		Operation: operation,
		Err:       err,
	}
}
