/*
// =====================================================================================
//
//       Filename:  BytesString.go
//
//    Description:  ref from fast http
//
//        Version:  1.0
//        Created:  06/23/2018 02:34:41 PM
//       Revision:  none
//       Compiler:  go1.10.3
//
//         Author:  boyi.gw, boyi.gw@alibaba-inc.com
//        Company:  Alibaba Group
//
// =====================================================================================
*/

package utils

import (
	"reflect"
	"unsafe"
)

/*
// ===  FUNCTION  ======================================================================
//         Name:  String2Bytes
//  Description:  return GoString's buffer slice(enable modify string)
// =====================================================================================
*/
func String2Bytes(s string) []byte {
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh := reflect.SliceHeader{
		Data: sh.Data,
		Len:  sh.Len,
		Cap:  sh.Len,
	}
	return *(*[]byte)(unsafe.Pointer(&bh))
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Bytes2String
//  Description:  convert b to string without copy
// =====================================================================================
*/
func Bytes2String(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  StringPointer
//  Description:  returns &s[0]
// =====================================================================================
*/
func StringPointer(s string) unsafe.Pointer {
	p := (*reflect.StringHeader)(unsafe.Pointer(&s))
	return unsafe.Pointer(p.Data)
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  BytesPointer
//  Description:  returns &b[0]
// =====================================================================================
*/
func BytesPointer(b []byte) unsafe.Pointer {
	p := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	return unsafe.Pointer(p.Data)
}
