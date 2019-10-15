package filter

import (
	"strings"
)

const (
	Sep = ";"
)

var (
	f = &Filter{
		collectionWhiteMap: make(map[string]bool),
		collectionBlackMap: make(map[string]bool),
	}
)

type Filter struct {
	collectionWhiteMap map[string]bool
	collectionBlackMap map[string]bool
}

func Init(collectionWhite, collectBlack string) {
	var collectionWhiteList, collectionBlackList []string
	if collectionWhite != "" {
		collectionWhiteList = strings.Split(collectionWhite, Sep)
	}
	if collectBlack != "" {
		collectionBlackList = strings.Split(collectBlack, Sep)
	}

	for _, ele := range collectionWhiteList {
		f.collectionWhiteMap[ele] = true
	}
	for _, ele := range collectionBlackList {
		f.collectionBlackMap[ele] = true
	}
}

func IsFilter(collection string) bool {
	if len(f.collectionWhiteMap) != 0 {
		return !f.collectionWhiteMap[collection]
	} else if len(f.collectionBlackMap) != 0 {
		return f.collectionBlackMap[collection]
	}
	return false
}

func FilterList(collectionList []string) []string {
	ret := make([]string, 0, len(collectionList))
	for _, ele := range collectionList {
		if !IsFilter(ele) {
			ret = append(ret, ele)
		}
	}
	return ret
}