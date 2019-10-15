package filter

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"fmt"
)

func TestFilter(t *testing.T) {
	// test InsertCkpt, UpdateCkpt, QueryCkpt, DropCheckpoint

	var nr int
	{
		fmt.Printf("TestCheckpointCRUD case %d.\n", nr)
		nr++

		Init("abc;efg;a", "")

		assert.Equal(t, true, IsFilter("anc"), "should be equal")
		assert.Equal(t, true, IsFilter("ab"), "should be equal")
		assert.Equal(t, false, IsFilter("abc"), "should be equal")
		assert.Equal(t, false, IsFilter("efg"), "should be equal")
	}

	{
		fmt.Printf("TestCheckpointCRUD case %d.\n", nr)
		nr++

		Init("","abc;efg;a")

		assert.Equal(t, false, IsFilter("anc"), "should be equal")
		assert.Equal(t, false, IsFilter("ab"), "should be equal")
		assert.Equal(t, true, IsFilter("abc"), "should be equal")
		assert.Equal(t, true, IsFilter("efg"), "should be equal")
	}

	{
		fmt.Printf("TestCheckpointCRUD case %d.\n", nr)
		nr++

		Init("","")

		assert.Equal(t, false, IsFilter("anc"), "should be equal")
		assert.Equal(t, false, IsFilter("ab"), "should be equal")
		assert.Equal(t, false, IsFilter("abc"), "should be equal")
		assert.Equal(t, false, IsFilter("efg"), "should be equal")
	}
}