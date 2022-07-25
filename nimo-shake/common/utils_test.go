package utils

import (
	"testing"
	"fmt"

	"github.com/vinllen/mgo/bson"
	"github.com/stretchr/testify/assert"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go/aws"
	"strings"
	"reflect"
)

func TestCompareBson(t *testing.T) {
	// test CompareBson

	var nr int
	{
		fmt.Printf("TestCompareBson case %d.\n", nr)
		nr++

		first := bson.M{
			"a": 1,
			"b": 2,
			"c": []string{"1", "aaa"},
		}
		second := bson.M{
			"c": []string{"1", "aaa"},
			"b": 2,
			"a": 1,
		}
		equal, err := CompareBson(first, second)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, equal, "should be equal")
	}

	{
		fmt.Printf("TestCompareBson case %d.\n", nr)
		nr++

		first := bson.M{
			"a": 1,
			"b": 2,
			"c": []string{"1", "aaa"},
		}
		second := bson.M{
			"c": []string{"1", "aaa"},
			"b": 2,
		}
		equal, err := CompareBson(first, second)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, false, equal, "should be equal")
	}

	{
		fmt.Printf("TestCompareBson case %d.\n", nr)
		nr++

		first := bson.M{
			"a": 1,
			"b": 2,
			"c": []string{"1", "aaa"},
		}
		second := bson.M{
			"_id": "fuck",
			"c": []string{"1", "aaa"},
			"b": 2,
			"a": 1,
		}
		equal, err := CompareBson(first, second)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, equal, "should be equal")
	}
}

func TestBuildShardTree_TraverseShard_CalMd5(t *testing.T) {
	// test BuildShardTree and TraverseShard and CalMd5

	var nr int
	{
		fmt.Printf("TestBuildShardTree_TraverseShard_CalMd5 case %d.\n", nr)
		nr++

		// inheritance relationship: father->son direct edge
		set := map[string]struct{} {
			"1->2": {},
			"2->3": {},
			"2->4": {},
			"5->6": {},
			"7->8": {},
			"7->9": {},
			"10->11": {},
			"11->12": {},
			"12->13": {},
			"13->14": {},
			"13->15": {},
			"nil->1": {},
			"nil->5": {},
			"nil->7": {},
			"nil->10": {},
		}
		shards := make([]*dynamodbstreams.Shard, 0)
		for key := range set {
			inheritance := strings.Split(key, "->")
			var father string
			if inheritance[0] != "nil" {
				father = inheritance[0]
			}
			shards = append(shards, &dynamodbstreams.Shard{
				ParentShardId: aws.String(father),
				ShardId:       aws.String(inheritance[1]),
			})
		}
		root := BuildShardTree(shards, "test-table", "test-arn")

		newSet := make(map[string]struct{})
		err := TraverseShard(root, func(node *ShardNode) error {
			var father string
			if *node.Shard.ParentShardId != "" {
				father = *node.Shard.ParentShardId
			} else {
				father = "nil"
			}
			inheritance := strings.Join([]string{father, *node.Shard.ShardId}, "->")
			newSet[inheritance] = struct{}{}

			return nil
		})

		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, reflect.DeepEqual(set, newSet), "should be equal")
	}

	// test empty shard list
	{
		fmt.Printf("TestBuildShardTree_TraverseShard_CalMd5 case %d.\n", nr)
		nr++

		// inheritance relationship: father->son direct edge
		set := map[string]struct{} {
		}
		shards := make([]*dynamodbstreams.Shard, 0)
		for key := range set {
			inheritance := strings.Split(key, "->")
			var father string
			if inheritance[0] != "nil" {
				father = inheritance[0]
			}
			shards = append(shards, &dynamodbstreams.Shard{
				ParentShardId: aws.String(father),
				ShardId:       aws.String(inheritance[1]),
			})
		}
		root := BuildShardTree(shards, "test-table", "test-arn")

		newSet := make(map[string]struct{})
		err := TraverseShard(root, func(node *ShardNode) error {
			var father string
			if *node.Shard.ParentShardId != "" {
				father = *node.Shard.ParentShardId
			} else {
				father = "nil"
			}
			inheritance := strings.Join([]string{father, *node.Shard.ShardId}, "->")
			newSet[inheritance] = struct{}{}

			return nil
		})

		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, reflect.DeepEqual(set, newSet), "should be equal")
	}

	{
		fmt.Printf("TestBuildShardTree_TraverseShard_CalMd5 case %d.\n", nr)
		nr++

		// inheritance relationship: father->son direct edge
		set := map[string]struct{} {
			"1->2": {},
			"1->3": {},
			"2->4": {},
			"2->5": {},
			"2->6": {},
			"2->7": {},
			"3->8": {},
			"3->9": {},
			"6->10": {},
			"10->11": {},
			"11->12": {},
		}
		shards := make([]*dynamodbstreams.Shard, 0)
		for key := range set {
			inheritance := strings.Split(key, "->")
			var father string
			if inheritance[0] != "nil" {
				father = inheritance[0]
			}
			shards = append(shards, &dynamodbstreams.Shard{
				ParentShardId: aws.String(father),
				ShardId:       aws.String(inheritance[1]),
			})
		}
		root := BuildShardTree(shards, "test-table", "test-arn")

		newSet := make(map[string]struct{})
		err := TraverseShard(root, func(node *ShardNode) error {
			var father string
			if *node.Shard.ParentShardId != "" {
				father = *node.Shard.ParentShardId
			} else {
				father = "nil"
			}
			inheritance := strings.Join([]string{father, *node.Shard.ShardId}, "->")
			newSet[inheritance] = struct{}{}

			return nil
		})

		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, reflect.DeepEqual(set, newSet), "should be equal")
	}

	// test extra node
	{
		fmt.Printf("TestBuildShardTree_TraverseShard_CalMd5 case %d.\n", nr)
		nr++

		// inheritance relationship: father->son direct edge
		set := map[string]struct{} {
			"1->2": {},
			"1->3": {},
			"2->4": {},
			"2->5": {},
			"2->6": {},
			"2->7": {},
			"3->8": {},
			"3->9": {},
			"6->10": {},
			"10->11": {},
			"11->12": {},
		}
		shards := make([]*dynamodbstreams.Shard, 0)
		for key := range set {
			inheritance := strings.Split(key, "->")
			var father string
			if inheritance[0] != "nil" {
				father = inheritance[0]
			}
			shards = append(shards, &dynamodbstreams.Shard{
				ParentShardId: aws.String(father),
				ShardId:       aws.String(inheritance[1]),
			})
		}

		// add two extra nodes
		shards = append(shards, &dynamodbstreams.Shard{
			ParentShardId: nil,
			ShardId:       aws.String("13"),
		})
		shards = append(shards, &dynamodbstreams.Shard{
			ParentShardId: nil,
			ShardId:       aws.String("14"),
		})

		root := BuildShardTree(shards, "test-table", "test-arn")

		newSet := make(map[string]struct{})
		err := TraverseShard(root, func(node *ShardNode) error {
			var father string
			if node.Shard.ParentShardId != nil && *node.Shard.ParentShardId != "" {
				father = *node.Shard.ParentShardId
			} else {
				father = "nil"
			}
			inheritance := strings.Join([]string{father, *node.Shard.ShardId}, "->")
			newSet[inheritance] = struct{}{}

			return nil
		})
		// fmt.Println(newSet)
		set["nil->13"] = struct{}{}
		set["nil->14"] = struct{}{}

		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, true, reflect.DeepEqual(set, newSet), "should be equal")
	}

	// test CalMd5
	{
		fmt.Printf("TestBuildShardTree_TraverseShard_CalMd5 case %d.\n", nr)
		nr++

		// inheritance relationship: father->son direct edge
		set := map[string]struct{} {
			"1->2": {},
			"1->3": {},
		}
		shards := make([]*dynamodbstreams.Shard, 0)
		for key := range set {
			inheritance := strings.Split(key, "->")
			var father string
			if inheritance[0] != "nil" {
				father = inheritance[0]
			}
			shards = append(shards, &dynamodbstreams.Shard{
				ParentShardId: aws.String(father),
				ShardId:       aws.String(inheritance[1]),
			})
		}
		root := BuildShardTree(shards, "test-table", "test-arn")
		root2 := BuildShardTree(shards, "test-table", "test-arn")
		md5 := CalMd5(root)
		md52 := CalMd5(root2)
		// fmt.Println(md5, md52)
		assert.Equal(t, true, root != root2, "should be equal")
		assert.Equal(t, md52, md5, "should be equal")

		// md53
		set3 := map[string]struct{} {
			"1->2": {},
			"1->3": {},
			"3->4": {},
		}
		shards3 := make([]*dynamodbstreams.Shard, 0)
		for key := range set3 {
			inheritance := strings.Split(key, "->")
			var father string
			if inheritance[0] != "nil" {
				father = inheritance[0]
			}
			shards3 = append(shards3, &dynamodbstreams.Shard{
				ParentShardId: aws.String(father),
				ShardId:       aws.String(inheritance[1]),
			})
		}
		root3 := BuildShardTree(shards3, "test-table", "test-arn")
		md53 := CalMd5(root3)
		assert.Equal(t, false, md5 == md53, "should be equal")

		// calculate empty shard lit
		set4 := map[string]struct{} {
		}
		shards4 := make([]*dynamodbstreams.Shard, 0)
		for key := range set4 {
			inheritance := strings.Split(key, "->")
			var father string
			if inheritance[0] != "nil" {
				father = inheritance[0]
			}
			shards4 = append(shards4, &dynamodbstreams.Shard{
				ParentShardId: aws.String(father),
				ShardId:       aws.String(inheritance[1]),
			})
		}
		root4 := BuildShardTree(shards4, "test-table", "test-arn")
		md54 := CalMd5(root4)
		assert.Equal(t, true, md54 != 0, "should be equal")
	}
}

func TestFindFirstErrorIndexAndMessage(t *testing.T) {
	// test FindFirstErrorIndexAndMessage

	var nr int
	{
		fmt.Printf("TestFindFirstErrorIndexAndMessage case %d.\n", nr)
		nr++

		error := "index[0], msg[xxxx]"
		index, msg, dup := FindFirstErrorIndexAndMessage(error)
		assert.Equal(t, 0, index, "should be equal")
		assert.Equal(t, "xxxx", msg, "should be equal")
		assert.Equal(t, false, dup, "should be equal")

		error = "index[10], msg[yyy]"
		index, msg, dup = FindFirstErrorIndexAndMessage(error)
		assert.Equal(t, 10, index, "should be equal")
		assert.Equal(t, "yyy", msg, "should be equal")
		assert.Equal(t, false, dup, "should be equal")

		error = "msg[yyy], index[10]"
		index, msg, dup = FindFirstErrorIndexAndMessage(error)
		assert.Equal(t, 10, index, "should be equal")
		assert.Equal(t, "yyy", msg, "should be equal")
		assert.Equal(t, false, dup, "should be equal")

		error = "index[1234567], msg[err: unknown error]"
		index, msg, dup = FindFirstErrorIndexAndMessage(error)
		assert.Equal(t, 1234567, index, "should be equal")
		assert.Equal(t, "err: unknown error", msg, "should be equal")
		assert.Equal(t, false, dup, "should be equal")

		error = "index[1234567], msg[err: unknown [error]]"
		index, msg, dup = FindFirstErrorIndexAndMessage(error)
		assert.Equal(t, 1234567, index, "should be equal")
		assert.Equal(t, "err: unknown [error]", msg, "should be equal")
		assert.Equal(t, false, dup, "should be equal")

		error = "index[1234567], msg[err: unknown [error]] dup[true]"
		index, msg, dup = FindFirstErrorIndexAndMessage(error)
		assert.Equal(t, 1234567, index, "should be equal")
		assert.Equal(t, "err: unknown [error]", msg, "should be equal")
		assert.Equal(t, true, dup, "should be equal")

		error = "index[1234567], msg[err: unknown [error]] dup[false]"
		index, msg, dup = FindFirstErrorIndexAndMessage(error)
		assert.Equal(t, 1234567, index, "should be equal")
		assert.Equal(t, "err: unknown [error]", msg, "should be equal")
		assert.Equal(t, false, dup, "should be equal")

		error = "index[1234567], msg[err: unknown [error]] dup[]"
		index, msg, dup = FindFirstErrorIndexAndMessage(error)
		assert.Equal(t, 1234567, index, "should be equal")
		assert.Equal(t, "err: unknown [error]", msg, "should be equal")
		assert.Equal(t, false, dup, "should be equal")
	}
}