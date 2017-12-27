package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"
)

func TestT5(t *testing.T) {
	pbhash := &PBHash{
		Matches:    map[string]map[string]int{},
		Random:     rand.New(rand.NewSource(time.Now().UnixNano())),
		Committed:  map[string]int{},
		Keys:       map[Transition]map[Transition]bool{},
		LevelCount: map[int]int{},
		State:      map[Transition]map[Transition]bool{},
	}

	_ = pbhash

	root := "/Users/pcbje/Downloads/t5"
	//root := "tests/spec"
	//root := "tests/ppt"

	t.Fail()
	list, err := ioutil.ReadDir(root)
	if err != nil {
		log.Panic(err)
	}
	for index, f := range list {
		if strings.HasPrefix(f.Name(), ".") {
			continue
		}

		fp, _ := os.Open(root + "/" + f.Name())
		pbhash.Process(index, f.Name(), bufio.NewReader(fp), true)
		fp.Close()
		//break
	}

	for docId, matches := range pbhash.Matches {
		for matchedDocId, count := range matches {
			fmt.Println(fmt.Sprintf("%v (%v)\t%v (%v)\t%v", docId, pbhash.Committed[docId], matchedDocId, pbhash.Committed[matchedDocId], count))
		}
	}

}

func main() {

}
