package utils

import (
	"fmt"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
)

var (
	StopTraverseSonErr = fmt.Errorf("stop traverse")
)

type ShardNode struct {
	Shard    *dynamodbstreams.Shard
	ShardArn string
	Sons     map[string]*ShardNode
	Table    string
}

// build shard tree base on the input shard list, return root node
func BuildShardTree(shards []*dynamodbstreams.Shard, table string, ShardArn string) *ShardNode {
	pathMap := make(map[string]*ShardNode, len(shards)) // store the inheritance relationship
	inDegree := make(map[string]bool, len(shards))
	// initial
	for _, shard := range shards {
		pathMap[*shard.ShardId] = &ShardNode{
			Shard:    shard,
			ShardArn: ShardArn,
			Sons:     make(map[string]*ShardNode),
			Table:    table,
		}

		inDegree[*shard.ShardId] = false
	}

	// build path
	for _, shard := range shards {
		node := pathMap[*shard.ShardId]
		father := shard.ParentShardId
		if father != nil {
			if _, ok := pathMap[*father]; !ok {
				// father node isn't exist on the pathMap, which means deleted
				continue
			}
			inDegree[*shard.ShardId] = true
			pathMap[*father].Sons[*shard.ShardId] = node
		}
	}

	// root is fake node
	rootNode := &ShardNode{
		Shard:    nil,
		ShardArn: "",
		Sons:     make(map[string]*ShardNode),
		Table:    table,
	}
	for key, val := range inDegree {
		if val == false {
			rootNode.Sons[key] = pathMap[key]
			// fmt.Printf("root->%s\n", key)
		}
	}
	return rootNode
}

// dfs
func TraverseShard(node *ShardNode, callback func(node *ShardNode) error) error {
	if node == nil {
		return nil
	}

	if node.Shard != nil {
		// skip root node
		if err := callback(node); err != nil {
			if err != StopTraverseSonErr {
				return err
			} else {
				// return if error == StopTraverseSonErr
				return nil
			}
		}
		// fmt.Printf("%s->%s\n", *node.Shard.ParentShardId, *node.Shard.ShardId)
	}

	for _, son := range node.Sons {
		if err := TraverseShard(son, callback); err != nil {
			return err
		}
	}

	return nil
}

// calculate md5. TODO: add UT
func CalMd5(root *ShardNode) uint64 {
	if root == nil {
		return 0
	}

	list := make([]string, 0, len(root.Sons))
	for _, son := range root.Sons {
		ret := CalMd5(son)
		list = append(list, fmt.Sprintf("%s->%d", *son.Shard.ShardId, ret))
	}

	sort.Strings(list)
	concat := strings.Join(list, ";")
	// fmt.Println("concat: ", concat)
	return Md5In64(String2Bytes(concat))
}

func PrintShardTree(node *ShardNode) (string, error) {
	newSet := make([]string, 0)
	err := TraverseShard(node, func(node *ShardNode) error {
		var father string
		if node.Shard.ParentShardId != nil {
			father = *node.Shard.ParentShardId
		} else {
			father = "nil"
		}
		inheritance := strings.Join([]string{father, *node.Shard.ShardId}, "->")
		newSet = append(newSet, inheritance)

		return nil
	})
	if err != nil {
		return "", err
	}

	return strings.Join(newSet, "\n"), nil
}