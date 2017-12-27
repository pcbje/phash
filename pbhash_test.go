package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"
)

func TestT5(t *testing.T) {
	pb := &PBHash{
		Random:     rand.New(rand.NewSource(time.Now().UnixNano())),
		Matches:    map[string]map[string]int{},
		Committed:  map[string]int{},
		Keys:       map[Transition]map[Transition]bool{},
		LevelCount: map[int]int{},
		State:      map[string]map[Transition]map[Transition]bool{},
	}

	root := "/Users/pcbje/Downloads/t5"
	//root := "tests/spec"

	list, _ := ioutil.ReadDir(root)

	for index, f := range list {
		if strings.HasPrefix(f.Name(), ".") {
			continue
		}

		filePointer, _ := os.Open(root + "/" + f.Name())

		docId := f.Name()
		reader := bufio.NewReader(filePointer)

		docFeatures := pb.GetFeatures(index, docId, reader, true)
		pb.CommitFeatures(docId, docFeatures)

		filePointer.Close()
	}

	for docId, matches := range pb.Matches {
		for matchedDocId, count := range matches {
			fmt.Println(fmt.Sprintf("%v (%v)\t%v (%v)\t%v", docId, pb.Committed[docId], matchedDocId, pb.Committed[matchedDocId], count))
		}
	}

	t.Fail()
}
