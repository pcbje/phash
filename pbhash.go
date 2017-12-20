package main

import (	
	"strings"
	"bufio"
	"fmt"
	"math/rand"
	"math"
)

type IndexEntry struct {
	Offset uint64
	Children map[uint32][]IndexEntry
	Last bool
}

type SampledHash struct {
	Hash uint32
	Random float64
	Index float64
}

type Flexi struct {
	Index IndexEntry
	Sampled map[string][]SampledHash
}

func (f Flexi) Sample(docId string, index float64, hash uint32) {
	if hash == 0 {
		return
	}	

	random := rand.Float64()

	if random <= 1 / math.Sqrt(index) {
		f.Sampled[docId] = append(f.Sampled[docId], SampledHash{
			Hash: hash,
			Random: random,
			Index: index,
		})
	}
}

func (f Flexi) Commit(docId string) {
	hashes := []SampledHash{}
	maxIndex := f.Sampled[docId][len(f.Sampled[docId]) - 1].Index
	threshold := 1 / math.Sqrt(maxIndex)

	for _, sampledHash := range f.Sampled[docId] {
		if sampledHash.Random <= threshold {
			hashes = append(hashes, sampledHash)
		}
	}

	wordLength := int(math.Sqrt(float64(len(hashes))))
	wordCount := wordLength
	words := make([][]SampledHash, wordCount)

	for hashIndex, hash := range hashes	{
		wordIndex := hashIndex % wordCount
		words[wordIndex] = append(words[wordIndex], hash)
	}


	for _, word := range words {
		item := f.Index

		for hashIndex, sampledHash := range word {
			if _, ok := item.Children[sampledHash.Hash]; !ok {
				item.Children[sampledHash.Hash] = []IndexEntry{}
			}

			item.Children[sampledHash.Hash] = append(
				item.Children[sampledHash.Hash],
				IndexEntry{
					Offset: uint64(sampledHash.Index),
					Children: map[uint32][]IndexEntry{},
					Last: hashIndex == len(word) - 1,
				},
			)

			item = item.Children[sampledHash.Hash][len(item.Children[sampledHash.Hash]) - 1]
		} 
	}
}

func (f Flexi) Match(docId string, hash uint32) {	
	// for validNextStates in state[hash]
}

func (f Flexi) Process(docId string, reader *bufio.Reader) {	
	hasher := NewBuzHash(31)

	f.Sampled[docId] = []SampledHash{}

	var err error
	var b byte
	var hash uint32

	var index float64
	index = 0.0
	b, err = reader.ReadByte()

	for err == nil {
		hasher.HashByte(b)	
		hash = hasher.Sum32()

		f.Sample(docId, index, hash)
		f.Match(docId, hash)

		b, err = reader.ReadByte()
		index += 1
	}   	
}

func main() {
	reader := bufio.NewReader(strings.NewReader("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))

	f := Flexi{
		Sampled: map[string][]SampledHash{},
		Index: IndexEntry{
			Offset: 0,
			Children: map[uint32][]IndexEntry{},
		},
	}

	f.Process("1234", reader)
	f.Commit("1234")

	fmt.Println(f.Index)
}