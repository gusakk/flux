// Copyright © 2019 Dell Inc. or its subsidiaries.
// All Rights Reserved.
// This software contains the intellectual property of Dell Inc. or is licensed to Dell Inc.
// from third parties. Use of this software and the intellectual property
// contained therein is expressly limited to the terms and conditions of the
// License Agreement under which it is provided by or on behalf of Dell Inc. or its subsidiaries.

package intern

import (
	"reflect"
	"testing"
	"unsafe"
)

func TestInternString(t *testing.T) {
	// register string cache
	RegisterStringCache(100000)
	// add string to the pool
	newstr := InternString(string([]byte("csvdata")))
	// get string from the pool
	internedstr := InternString(string([]byte("csvdata")))
	// check that the strings above are equal (data and pointers)
	if stringptr(newstr) != stringptr(internedstr) {
		t.Errorf("string pointers should be equal, newstr: %v, internedstr: %v",
			stringptr(newstr), stringptr(internedstr))
	}
}

func TestString(t *testing.T) {
	// register string cache
	RegisterStringCache(100000)
	// add string to the pool
	newstr := InternString(string([]byte("csvdata")))
	// get string from the pool
	internedstr := String(string([]byte("csvdata")))
	// check that the strings above are equal (data and pointers)
	if stringptr(newstr) != stringptr(internedstr) {
		t.Errorf("string pointers should be equal, newstr: %v, internedstr: %v",
			stringptr(newstr), stringptr(internedstr))
	}
}

// stringptr returns a pointer to the string data.
func stringptr(s string) uintptr {
	return (*reflect.StringHeader)(unsafe.Pointer(&s)).Data
}