package utils

import (
	"crypto/md5"
	"encoding/binary"
)

/*
// ===  FUNCTION  ======================================================================
//         Name:  Md5
//  Description:  128位md5
// =====================================================================================
*/
func Md5(data []byte) [16]byte {
	return md5.Sum(data)
}

/*
// ===  FUNCTION  ======================================================================
//         Name:  Md5
//  Description:  64位md5
// =====================================================================================
*/
func Md5In64(data []byte) uint64 {
	var md5 = md5.Sum(data)
	var lowMd5 = md5[0:8]
	return binary.LittleEndian.Uint64(lowMd5)
}
