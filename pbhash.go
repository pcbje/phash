package main

import (
	"bufio"
	"fmt"
	"math"
	"math/rand"

	"log"
)

type IndexEntry struct {
	Level    int
	DocId    string
	Hash     uint32
	Distance float64
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
	Committed map[string]int
	Matches map[string]map[string]int
	State   map[uint32]map[string]map[float64]IndexEntry
	Random  *rand.Rand
}


func (pb PBHash) Commit(docId string) {
	if len(pb.Sampled[docId]) == 0 {
		log.Print(fmt.Sprintf("No hashes sampled for doc: %v", docId))
		return
	}

	hashes := pb.Sampled[docId]

	threshold := 1.0 / float64(len(pb.Sampled[docId]))
	wordLength := int(math.Sqrt(math.Sqrt(float64(len(hashes) / 2))))
	wordCount := (len(hashes) / 2) / wordLength
	randomwords := make([][]SampledHash, wordCount)
	partitionSize := math.Max(1, math.Sqrt(float64(len(hashes) / 2)))
	partitions := make([][]SampledHash, int(partitionSize))

	var w uint32
	w = uint32(len(randomwords))
	var i float64
	i = 0
	for _, hash := range hashes {
		if hash.Random > threshold {
			continue
		}

		wordIndex := hash.Hash % w
		partition := int(math.Min(float64(len(partitions) - 1), math.Floor(i / partitionSize)))
		randomwords[wordIndex] = append(randomwords[wordIndex], hash)
		partitions[partition] = append(partitions[partition], hash)
		i += 1
	}

	cw := 0
	for _, word := range append(randomwords, partitions...) {
		item := pb.Index

		// Won't compare based on just a single hash...
		if len(word) <= 1 {
			continue
		}

		cw += 1
		for level, sampledHash := range word {
			if _, ok := item.Children[sampledHash.Hash]; !ok {
				item.Children[sampledHash.Hash] = map[string]IndexEntry{}
			}
			_ = level

			/*distance := 0.0
			if level > 0 {
				distance = word[level].Index - word[level-1].Index
			}

			item.Children[sampledHash.Hash][docId] = IndexEntry{
				DocId:    docId,
				Hash:     sampledHash.Hash,
				Distance: distance,
				Children: map[uint32]map[string]IndexEntry{},
				Last:     level == len(word)-1,
				Level: level,
			}*/

			//item = item.Children[sampledHash.Hash][docId]
		}
	}

	pb.Committed[docId] = cw
}

func (pb PBHash) Match(docId string, index float64, hash uint32) {
	if _, ok := pb.State[hash]; ok {
		for matchedDocId, positions := range pb.State[hash] {

			for position, state := range positions {
				_ = position
				//fmt.Println(state.Level)
				actualDistance := index - position
				if actualDistance/state.Distance > 1.1 {
					delete(positions, position)
					if len(positions) == 0 {
						delete(pb.State, hash)
					}
					continue
				}

				for nextHash, nextStates := range state.Children {
					if _, hasHash := pb.State[nextHash]; !hasHash {
						pb.State[nextHash] = map[string]map[float64]IndexEntry{}
					}

					for _, nextState := range nextStates {
						if _, hasDoc := pb.State[nextHash][nextState.DocId]; !hasDoc {
							pb.State[nextHash][nextState.DocId] = map[float64]IndexEntry{}
						}

						pb.State[nextHash][nextState.DocId][index] = nextState
					}
				}

				// We have a matching word.
				if state.Last {
					if _, notFirstMatch := pb.Matches[docId][matchedDocId]; !notFirstMatch {
						pb.Matches[docId][matchedDocId] = 0
					}

					pb.Matches[docId][matchedDocId] = pb.Matches[docId][matchedDocId] + 1
					delete(pb.State, state.Hash)
					//log.Println("> match: " + docId + " = " + matchedDocId, pb.Matches[docId][matchedDocId])
				}
			}

		}
	}

	if _, ok := pb.Index.Children[hash]; ok {
		for _, state := range pb.Index.Children[hash] {
			for nextHash, nextStates := range state.Children {
				for _, nextState := range nextStates {
					if _, hasHash := pb.State[nextHash]; !hasHash {
						pb.State[nextHash] = map[string]map[float64]IndexEntry{}
					}

					if _, hasDoc := pb.State[nextHash][nextState.DocId]; !hasDoc {
						pb.State[nextHash][nextState.DocId] = map[float64]IndexEntry{}
					}

					pb.State[nextHash][nextState.DocId][index] = nextState
				}
			}
		}
	}
}

func (pb PBHash) Process(index int, docId string, reader *bufio.Reader, match bool) {
	feature := Create()
	_ = feature
	pb.State = map[uint32]map[string]map[float64]IndexEntry{}
	pb.Sampled[docId] = []SampledHash{}
	pb.Matches[docId] = map[string]int{}

	pb.Sampled[docId] = feature.Compute(index, pb, docId, reader, match)
  pb.Commit(docId)

	//fmt.Println(docId)
}

/*
/*hasher.HashByte(b)
hash = hasher.Sum32()

pb.Match(docId, index, hash)
if index >= windowSize {
	pb.Sample(docId, index, hash)
}
*/
