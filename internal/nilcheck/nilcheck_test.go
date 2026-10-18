package nilcheck

import (
	"io"
	"testing"
)

type valueReader struct{}

func (valueReader) Read([]byte) (int, error) { return 0, io.EOF }

func TestIsNil(t *testing.T) {
	var typedNilPointer *valueReader
	var nilInterface io.Reader
	var typedNilInInterface io.Reader = typedNilPointer
	var nilMap map[string]int
	var nilSlice []int
	var nilFunc func()
	var nilChan chan int

	tests := []struct {
		name  string
		value any
		want  bool
	}{
		{name: "untyped nil", value: nil, want: true},
		{name: "nil interface", value: nilInterface, want: true},
		{name: "typed nil pointer in interface", value: typedNilInInterface, want: true},
		{name: "nil map", value: nilMap, want: true},
		{name: "nil slice", value: nilSlice, want: true},
		{name: "nil func", value: nilFunc, want: true},
		{name: "nil chan", value: nilChan, want: true},
		{name: "value type", value: valueReader{}, want: false},
		{name: "non-nil pointer", value: &valueReader{}, want: false},
		{name: "zero int", value: 0, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := IsNil(test.value); got != test.want {
				t.Fatalf("IsNil(%#v) = %v, want %v", test.value, got, test.want)
			}
		})
	}
}
