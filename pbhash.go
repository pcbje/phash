package main

import (
	"bufio"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"time"
)

type IndexEntry struct {
	Level    int
	DocId    string
	Hash     uint32
	Children map[uint32]map[string]IndexEntry
	Last     bool
}

type SampledHash struct {
	Hash   uint32
	Random float64
	Index  float64
}

type PBHash struct {
	Index   IndexEntry
	Sampled map[string][]SampledHash
	Matches map[string]map[string]int
	State   map[uint32]map[string]IndexEntry
	Random  *rand.Rand
}

func (pb PBHash) Sample(docId string, index float64, hash uint32) {
	if hash == 0 {
		return
	}

	var random float64

	if pb.Random == nil {
		random = rand.Float64()
	} else {
		random = pb.Random.Float64()
	}

	if random <= 1/math.Sqrt(index) {
		pb.Sampled[docId] = append(pb.Sampled[docId], SampledHash{
			Hash:   hash,
			Random: random,
			Index:  index,
		})
	}
}

func (pb PBHash) Commit(docId string) {
	hashes := []SampledHash{}
	maxIndex := pb.Sampled[docId][len(pb.Sampled[docId])-1].Index
	threshold := 1 / math.Sqrt(maxIndex)

	for _, sampledHash := range pb.Sampled[docId] {
		if sampledHash.Random <= threshold {
			hashes = append(hashes, sampledHash)
		}
	}

	wordLength := int(math.Sqrt(float64(len(hashes))))
	wordCount := wordLength
	randomwords := make([][]SampledHash, wordCount)

	partitions := make([][]SampledHash, wordCount)
	partitionSize := math.Ceil(maxIndex/float64(wordCount)) + 1

	for _, hash := range hashes {
		wordIndex := rand.Intn(len(randomwords))
		partition := int(math.Floor(hash.Index / partitionSize))
		randomwords[wordIndex] = append(randomwords[wordIndex], hash)
		partitions[partition] = append(partitions[partition], hash)
	}

	for _, word := range append(randomwords, partitions...) {
		item := pb.Index

		// Can't compare based just on a single hash...
		if len(word) == 1 {
			continue
		}

		for level, sampledHash := range word {
			if _, ok := item.Children[sampledHash.Hash]; !ok {
				item.Children[sampledHash.Hash] = map[string]IndexEntry{}
			}

			item.Children[sampledHash.Hash][docId] = IndexEntry{
				DocId:    docId,
				Hash:     sampledHash.Hash,
				Children: map[uint32]map[string]IndexEntry{},
				Last:     level == len(word)-1,
			}

			item = item.Children[sampledHash.Hash][docId]
		}
	}
}

func (pb PBHash) Match(docId string, index float64, hash uint32) {
	if _, ok := pb.State[hash]; ok {
		for matchedDocId, state := range pb.State[hash] {
			for nextHash, nextStates := range state.Children {
				if _, hasHash := pb.State[nextHash]; !hasHash {
					pb.State[nextHash] = map[string]IndexEntry{}
				}

				for _, nextState := range nextStates {
					pb.State[nextHash][fmt.Sprintf("%v:%v", index, nextState.DocId)] = nextState
				}
			}

			if state.Last {
				matchedDocId = strings.Split(matchedDocId, ":")[1]
				if _, notFirstMatch := pb.Matches[docId][matchedDocId]; !notFirstMatch {
					pb.Matches[docId][matchedDocId] = 0
				}

				pb.Matches[docId][matchedDocId] = pb.Matches[docId][matchedDocId] + 1
			}
		}
	}

	if _, ok := pb.Index.Children[hash]; ok {
		for _, state := range pb.Index.Children[hash] {
			for nextHash, nextStates := range state.Children {
				for _, nextState := range nextStates {
					if _, hasHash := pb.State[nextHash]; !hasHash {
						pb.State[nextHash] = map[string]IndexEntry{}
					}

					pb.State[nextHash][fmt.Sprintf("%v:%v", index, nextState.DocId)] = nextState
				}
			}
		}
	}
}

func (pb PBHash) Process(docId string, reader *bufio.Reader) {
	hasher := NewBuzHash(12)

	pb.State = map[uint32]map[string]IndexEntry{}
	pb.Sampled[docId] = []SampledHash{}
	pb.Matches[docId] = map[string]int{}

	var err error
	var b byte
	var hash uint32

	var index float64
	index = 0.0
	b, err = reader.ReadByte()

	for err == nil {
		hasher.HashByte(b)
		hash = hasher.Sum32()

		pb.Sample(docId, index, hash)
		pb.Match(docId, index, hash)

		b, err = reader.ReadByte()
		index += 1
	}
}

func main() {
	pbhash := PBHash{
		Matches: map[string]map[string]int{},
		Random:  rand.New(rand.NewSource(time.Now().UnixNano())),
		Sampled: map[string][]SampledHash{},
		Index: IndexEntry{
			Children: map[uint32]map[string]IndexEntry{},
		},
		State: map[uint32]map[string]IndexEntry{},
	}

	pbhash.Process("1234", bufio.NewReader(strings.NewReader("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Praesent molestie mi sed mollis hendrerit. Phasellus at vulputate sem. Nulla facilisi. Aenean vitae consectetur mauris, vitae tristique leo. Fusce eget elit felis. Vestibulum imperdiet dui et leo varius, et commodo tortor ultrices. Aliquam pharetra elementum nunc in vulputate. Vestibulum ultricies posuere suscipit. Sed a sodales mi. Curabitur ligula augue, ultricies vitae ante in, vulputate vulputate sem. Ut at tellus quam.")))
	pbhash.Commit("1234")

	pbhash.Process("5678", bufio.NewReader(strings.NewReader("Detter ")))
	pbhash.Commit("5678")

	pbhash.Process("4321", bufio.NewReader(strings.NewReader("Lorem ipsum dolor sit amet, consectetur adipiscing dasds. Praesent molestie mi sed mollis adSADzxzx<. Phasellus at vulputate sem. Nulla facilisi. Aenean vitae consectetur mauris, vitae tristique leo. Fusce eget elit felis. Vestibulum sadsadas dui et leo varius, et commodo tortor ultrices. Aliquam pharetra elementum nunc in vulputate. Vestibulum ultricies posuere suscipit. Sed a sodales mi. Curabitur ligula augue, ultricies vitae ante in, vulputate sem. Ut at tellus quam.")))
	fmt.Println(pbhash.Matches["4321"])

	pbhash.Process("0000", bufio.NewReader(strings.NewReader("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Praesent molestie mi sed mollis hendrerit. Phasellus at vulputate sem. Nulla facilisi. Aenean vitae consectetur mauris, vitae tristique leo. Fusce eget elit felis. Vestibulum imperdiet dui et leo varius, et commodo tortor ultrices. Aliquam pharetra elementum nunc in vulputate. Vestibulum ultricies posuere suscipit. Sed a sodales mi. Curabitur ligula augue, ultricies vitae ante in, vulputate vulputate sem. Ut at tellus quam.")))
	fmt.Println(pbhash.Matches["0000"])

	//fmt.Println(pbhash.Index)
}
