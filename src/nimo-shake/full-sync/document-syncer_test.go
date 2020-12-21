package full_sync

import (
	"testing"
	"fmt"
	"github.com/stretchr/testify/assert"
	"time"
)

func TestDocumentSyncer(t *testing.T) {
	// test documentSyncer main function

	var nr int
	{
		fmt.Printf("TestDocumentSyncer case %d.\n", nr)
		nr++

		UT_TestDocumentSyncer = true
		UT_TestDocumentSyncer_Chan = make(chan []interface{}, 1000)

		docSyncer := &documentSyncer{
			inputChan: make(chan interface{}, 10),
		}
		go docSyncer.Run()

		for i := 0; i < batchNumber - 5; i++ {
			docSyncer.inputChan<-i
		}

		out := <-UT_TestDocumentSyncer_Chan
		assert.Equal(t, batchNumber - 5, len(out), "should be equal")
		for i := 0; i < len(out); i++ {
			assert.Equal(t, i, out[i].(int), "should be equal")
		}
	}

	// output length > batchNumber
	{
		fmt.Printf("TestDocumentSyncer case %d.\n", nr)
		nr++

		UT_TestDocumentSyncer = true
		UT_TestDocumentSyncer_Chan = make(chan []interface{}, 1000)

		docSyncer := &documentSyncer{
			inputChan: make(chan interface{}, 10),
		}
		go docSyncer.Run()

		for i := 0; i < batchNumber + 5; i++ {
			docSyncer.inputChan<-i
		}

		out := make([]interface{}, 0)
		out1 := <-UT_TestDocumentSyncer_Chan
		assert.Equal(t, batchNumber, len(out1), "should be equal")
		out = append(out, out1...)

		out2 := <-UT_TestDocumentSyncer_Chan
		assert.Equal(t, 5, len(out2), "should be equal")
		out = append(out, out2...)

		for i := 0; i < len(out); i++ {
			assert.Equal(t, i, out[i].(int), "should be equal")
		}
	}

	// output timeout
	{
		fmt.Printf("TestDocumentSyncer case %d.\n", nr)
		nr++

		UT_TestDocumentSyncer = true
		UT_TestDocumentSyncer_Chan = make(chan []interface{}, 1000)

		docSyncer := &documentSyncer{
			inputChan: make(chan interface{}, 10),
		}
		go docSyncer.Run()

		for i := 0; i < batchNumber - 10; i++ {
			docSyncer.inputChan<-i
		}
		time.Sleep((batchTimeout + 2) * time.Second)
		for i := batchNumber - 10; i < batchNumber + 5; i++ {
			docSyncer.inputChan<-i
		}

		out := make([]interface{}, 0)
		out1 := <-UT_TestDocumentSyncer_Chan
		assert.Equal(t, batchNumber - 10, len(out1), "should be equal")
		out = append(out, out1...)

		out2 := <-UT_TestDocumentSyncer_Chan
		assert.Equal(t, 15, len(out2), "should be equal")
		out = append(out, out2...)
		fmt.Println(out)

		for i := 0; i < len(out); i++ {
			assert.Equal(t, i, out[i].(int), "should be equal")
		}
	}
}