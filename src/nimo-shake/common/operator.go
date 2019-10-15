package utils

import (
	"strings"
)

func AppendStringList(input []string, list []*string) ([]string) {
	for _, ele := range list {
		input = append(input, *ele)
	}
	return input
}

func StringListToMap(input []string) map[string]struct{} {
	mp := make(map[string]struct{}, len(input))
	for _, ele := range input {
		mp[ele] = struct{}{}
	}
	return mp
}

// @vinllen. see BulkError.Error. -1 means not found
func FindFirstErrorIndexAndMessage(error string) (int, string, bool) {
	subIndex := "index["
	subMsg := "msg["
	subDup := "dup["
	index := strings.Index(error, subIndex)
	if index == -1 {
		return index, "", false
	}

	indexVal := 0
	for i := index + len(subIndex); i < len(error) && error[i] != ']'; i++ {
		// fmt.Printf("%c %d\n", rune(error[i]), int(error[i] - '0'))
		indexVal = indexVal * 10 + int(error[i] - '0')
	}

	index = strings.Index(error, subMsg)
	if index == -1 {
		return indexVal, "", false
	}

	i := index + len(subMsg)
	stack := 0
	for ; i < len(error); i++ {
		if error[i] == ']' {
			if stack == 0 {
				break
			} else {
				stack -= 1
			}
		} else if error[i] == '[' {
			stack += 1
		}
	}
	msg := error[index + len(subMsg): i]

	index = strings.Index(error, subDup)
	if index == -1 {
		return indexVal, msg, false
	}
	i = index + len(subMsg)
	for ; i < len(error) && error[i] != ']'; i++ {}
	dupVal := error[index + len(subMsg):i]

	return indexVal, msg, dupVal == "true"
}