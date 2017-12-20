package main

import (
	"bufio"
	"fmt"
	"math"
	"math/rand"
	"strings"
	_ "time"
)

type IndexEntry struct {
	DocId string
	Hash uint32
	Offset   float64
	Children map[uint32]map[string]IndexEntry
	Last     bool
}

type SampledHash struct {
	Hash   uint32
	Random float64
	Index  float64
}

type Flexi struct {
	Index   IndexEntry
	Sampled map[string][]SampledHash
	Matches map[string]map[string]int
	State map[uint32]map[string]IndexEntry
	Random *rand.Rand
}

func (f Flexi) Sample(docId string, index float64, hash uint32) {
	if hash == 0 {
		return
	}

	var random float64

	if f.Random == nil {
		random = rand.Float64()
	} else {
		random = f.Random.Float64()
	}

	if random <= 1/math.Sqrt(index) {
		f.Sampled[docId] = append(f.Sampled[docId], SampledHash{
			Hash:   hash,
			Random: random,
			Index:  index,
		})
	}
}

func (f Flexi) Commit(docId string) {
	hashes := []SampledHash{}
	maxIndex := f.Sampled[docId][len(f.Sampled[docId])-1].Index
	threshold := 1 / math.Sqrt(maxIndex)

	for _, sampledHash := range f.Sampled[docId] {
		if sampledHash.Random <= threshold {
			hashes = append(hashes, sampledHash)
		}
	}

	wordLength := int(math.Sqrt(float64(len(hashes))))
	wordCount := wordLength
	words := make([][]SampledHash, wordCount)

	for _, hash := range hashes {
		wordIndex := rand.Intn(len(words))
		words[wordIndex] = append(words[wordIndex], hash)
	}

	for _, word := range words {
		item := f.Index

		for hashIndex, sampledHash := range word {
			if _, ok := item.Children[sampledHash.Hash]; !ok {
				item.Children[sampledHash.Hash] = map[string]IndexEntry{}
			}

			item.Children[sampledHash.Hash][docId] = IndexEntry{
				DocId: docId,
				Hash: 	  sampledHash.Hash,
				Offset:   sampledHash.Index,
				Children: map[uint32]map[string]IndexEntry{},
				Last:     hashIndex == len(word)-1,
			}
			

			item = item.Children[sampledHash.Hash][docId]
		}
	}
}

func (f Flexi) Match(docId string, index float64, hash uint32) {	
	if _, ok := f.State[hash]; ok {	
		for matchedDocId, state := range f.State[hash] {				
			for nextHash, nextStates := range state.Children {					
				if _, hasHash := f.State[nextHash]; !hasHash {
					f.State[nextHash] = map[string]IndexEntry{}
				}

				for _, nextState := range nextStates {
					f.State[nextHash][fmt.Sprintf("%v:%v", index, nextState.DocId)] = nextState
				}
			}

			if state.Last {					
				matchedDocId = strings.Split(matchedDocId, ":")[1]
				if _, notFirstMatch := f.Matches[docId][matchedDocId]; !notFirstMatch {
					f.Matches[docId][matchedDocId] = 0
				}

				f.Matches[docId][matchedDocId] = f.Matches[docId][matchedDocId] + 1					
			}		
		}
	}

	if _, ok := f.Index.Children[hash]; ok {				
		for _, state := range f.Index.Children[hash] {			
			for nextHash, nextStates := range state.Children {
				for _, nextState := range nextStates {
					if _, hasHash := f.State[nextHash]; !hasHash {
						f.State[nextHash] = map[string]IndexEntry{}
					}
					
					f.State[nextHash][fmt.Sprintf("%v:%v", index, nextState.DocId)] = nextState						
				}
			}
		}
	}
}

func (f Flexi) Process(docId string, reader *bufio.Reader) {
	hasher := NewBuzHash(12)

	f.State = map[uint32]map[string]IndexEntry{}
	f.Sampled[docId] = []SampledHash{}
	f.Matches[docId] = map[string]int{}	

	var err error
	var b byte
	var hash uint32

	var index float64
	index = 0.0
	b, err = reader.ReadByte()

	for err == nil {
		hasher.HashByte(b)
		hash = hasher.Sum32()

		//fmt.Println(index, hash)

		f.Sample(docId, index, hash)
		f.Match(docId, index, hash)

		b, err = reader.ReadByte()
		index += 1
	}
}

func main() {
	reader := bufio.NewReader(strings.NewReader("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Praesent molestie mi sed mollis hendrerit. Phasellus at vulputate sem. Nulla facilisi. Aenean vitae consectetur mauris, vitae tristique leo. Fusce eget elit felis. Vestibulum imperdiet dui et leo varius, et commodo tortor ultrices. Aliquam pharetra elementum nunc in vulputate. Vestibulum ultricies posuere suscipit. Sed a sodales mi. Curabitur ligula augue, ultricies vitae ante in, vulputate vulputate sem. Ut at tellus quam."))

	f := Flexi{
		Matches: map[string]map[string]int{},
		//Random: rand.New(rand.NewSource(time.Now().UnixNano())),
		Sampled: map[string][]SampledHash{},
		Index: IndexEntry{
			Offset:   0,
			Children: map[uint32]map[string]IndexEntry{},
		},
		State: map[uint32]map[string]IndexEntry{},
	}

	f.Process("1234", reader)
	f.Commit("1234")
	
	f.Process("4321", bufio.NewReader(strings.NewReader("Lorem ipsum dolor sit amet, consectetur adipiscing dasds. Praesent molestie mi sed mollis adSADzxzx<. Phasellus at vulputate sem. Nulla facilisi. Aenean vitae consectetur mauris, vitae tristique leo. Fusce eget elit felis. Vestibulum sadsadas dui et leo varius, et commodo tortor ultrices. Aliquam pharetra elementum nunc in vulputate. Vestibulum ultricies posuere suscipit. Sed a sodales mi. Curabitur ligula augue, ultricies vitae ante in, vulputate sem. Ut at tellus quam.")))
	fmt.Println(f.Matches["4321"])

	f.Process("0000", bufio.NewReader(strings.NewReader("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Praesent molestie mi sed mollis hendrerit. Phasellus at vulputate sem. Nulla facilisi. Aenean vitae consectetur mauris, vitae tristique leo. Fusce eget elit felis. Vestibulum imperdiet dui et leo varius, et commodo tortor ultrices. Aliquam pharetra elementum nunc in vulputate. Vestibulum ultricies posuere suscipit. Sed a sodales mi. Curabitur ligula augue, ultricies vitae ante in, vulputate vulputate sem. Ut at tellus quam.")))
	fmt.Println(f.Matches["0000"])


	//fmt.Println(f.Index)
}
