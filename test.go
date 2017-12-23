package main

import (
	"bufio"
	"strings"

	"math/rand"
	"time"
	"os"
	"io/ioutil"
	"log"
)

func main() {
	pbhash := PBHash{
		Matches: map[string]map[string]int{},
		Random:  rand.New(rand.NewSource(time.Now().UnixNano())),
		Sampled: map[string][]SampledHash{},
		Index: IndexEntry{
			Distance: 0,
			Children: map[uint32]map[string]IndexEntry{},
		},
		State: map[uint32]map[string]map[float64]IndexEntry{},
	}

	_ = pbhash

	root := "/Users/pcbje/Downloads/t5"
	list, err := ioutil.ReadDir(root)
	if err != nil {
		log.Panic(err)
	}
	for _, f := range list {
		if strings.HasPrefix(f.Name(), ".") {
			continue
		}
		fp, _ := os.Open(root +"/" + f.Name())
	 	pbhash.Process(f.Name(), bufio.NewReader(fp))
		fp.Close()
		//break
	}

}

/*
pbhash.Process("1234", bufio.NewReader(strings.NewReader("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Praesent molestie mi sed mollis hendrerit. Phasellus at vulputate sem. Nulla facilisi. Aenean vitae consectetur mauris, vitae tristique leo. Fusce eget elit felis. Vestibulum imperdiet dui et leo varius, et commodo tortor ultrices. Aliquam pharetra elementum nunc in vulputate. Vestibulum ultricies posuere suscipit. Sed a sodales mi. Curabitur ligula augue, ultricies vitae ante in, vulputate vulputate sem. Ut at tellus quam.")))
pbhash.Commit("1234")

pbhash.Process("5678", bufio.NewReader(strings.NewReader("Detter ")))
pbhash.Commit("5678")

pbhash.Process("4321", bufio.NewReader(strings.NewReader("Lorem ipsum dolor sit amet, consectetur adipiscing dasds. Praesent molestie mi sed mollis adSADzxzx<. Phasellus at vulputate sem. Nulla facilisi. Aenean vitae consectetur mauris, vitae tristique leo. Fusce eget elit felis. Vestibulum sadsadas dui et leo varius, et commodo tortor ultrices. Aliquam pharetra elementum nunc in vulputate. Vestibulum ultricies posuere suscipit. Sed a sodales mi. Curabitur ligula augue, ultricies vitae ante in, vulputate sem. Ut at tellus quam.")))
fmt.Println(pbhash.Matches["4321"])

pbhash.Process("0000", bufio.NewReader(strings.NewReader("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Praesent molestie mi sed mollis hendrerit. Phasellus at vulputate sem. Nulla facilisi. Aenean vitae consectetur mauris, vitae tristique leo. Fusce eget elit felis. ")))
fmt.Println(pbhash.Matches["0000"])
*/
