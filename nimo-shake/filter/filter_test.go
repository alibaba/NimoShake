package filter

import (
	"testing"
	"fmt"

	"github.com/stretchr/testify/assert"
)

func TestFilter(t *testing.T) {
	// test InsertCkpt, UpdateCkpt, QueryCkpt, DropCheckpoint

	var nr int
	{
		fmt.Printf("TestCheckpointCRUD case %d.\n", nr)
		nr++

		f = &Filter{
			collectionWhiteMap: make(map[string]bool),
			collectionBlackMap: make(map[string]bool),
		}
		Init("abc;efg;a", "")

		assert.Equal(t, true, IsFilter("anc"), "should be equal")
		assert.Equal(t, true, IsFilter("ab"), "should be equal")
		assert.Equal(t, false, IsFilter("abc"), "should be equal")
		assert.Equal(t, false, IsFilter("efg"), "should be equal")
	}

	{
		fmt.Printf("TestCheckpointCRUD case %d.\n", nr)
		nr++

		f = &Filter{
			collectionWhiteMap: make(map[string]bool),
			collectionBlackMap: make(map[string]bool),
		}
		Init("","abc;efg;a")

		assert.Equal(t, false, IsFilter("anc"), "should be equal")
		assert.Equal(t, false, IsFilter("ab"), "should be equal")
		assert.Equal(t, true, IsFilter("abc"), "should be equal")
		assert.Equal(t, true, IsFilter("efg"), "should be equal")
	}

	{
		fmt.Printf("TestCheckpointCRUD case %d.\n", nr)
		nr++

		f = &Filter{
			collectionWhiteMap: make(map[string]bool),
			collectionBlackMap: make(map[string]bool),
		}
		Init("","")

		assert.Equal(t, false, IsFilter("anc"), "should be equal")
		assert.Equal(t, false, IsFilter("ab"), "should be equal")
		assert.Equal(t, false, IsFilter("abc"), "should be equal")
		assert.Equal(t, false, IsFilter("efg"), "should be equal")
	}
}