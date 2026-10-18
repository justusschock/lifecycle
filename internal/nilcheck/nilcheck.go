// Package nilcheck detects nil interface values, including interfaces that
// hold a typed nil pointer, map, slice, channel or function.
//
// It is an implementation detail of go-lifecycle and not part of its API.
package nilcheck

import "reflect"

// IsNil reports whether value is nil or an interface holding a nil value of
// a nilable kind.
func IsNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
