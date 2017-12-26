package main

import (
	"bufio"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"log"
	"strconv"
)

type IndexEntry struct {
	Level    int
	DocId    string
	Hash     uint32
	Distance float64
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
	State   map[string]map[string]bool
	Keys 		map[string]map[string]bool
	LevelCount   map[string]int
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
		//item := pb.Index

		// Won't compare based on just a single hash...
		if len(word) <= 1 {
			continue
		}

		cw += 1
		level := len(word) - 1
		var key string
		var pkey string
		pkey = ""
		var index float64
		var distance float64
		var sampledHash SampledHash
		for level > -1 {
			sampledHash = word[level]

			index = 0.0
			distance = 0.0

			if level > 0 {
				index = sampledHash.Index
				distance = word[level].Index - word[level-1].Index
				key = fmt.Sprintf("%v\x00%v\x00%v\x00%v\x00%v", sampledHash.Hash, docId, index, distance, level)
			} else {
				key = fmt.Sprintf("%v", sampledHash.Hash)
			}

			if _, ok := pb.Keys[key]; !ok {
				pb.Keys[key] = map[string]bool{}
			}

			if pkey != "" && level < len(word)-1 {
				pb.Keys[key][pkey] = level == len(word)-2
			}

			pkey = key
			level -= 1
		}
	}

	pb.Committed[docId] = cw
}

func (pb PBHash) Match(docId string, index float64, ihash uint32) {
	hash := fmt.Sprintf("%v", ihash)

	var parts []string

	var matchDocId string
	var matchIndex float64
	var matchDistance float64
	var matchLevel string
	var matchKey string

	var nextHash string
	var nextDocId string
	var nextIndex string
	var nextDistance string
	var nextLevel string
	var nextKey string

	if _, ok := pb.State[hash]; ok {

		for thisKey, isLast := range pb.State[hash] {
			parts = strings.Split(thisKey, "\x00")
			matchDocId = parts[0]
			matchIndex, _ = strconv.ParseFloat(parts[1], 64)
			matchDistance, _ = strconv.ParseFloat(parts[2], 64)
			matchLevel = parts[3]

			pb.LevelCount[matchLevel] += 1

			matchKey = fmt.Sprintf("%v\x00%v\x00%v\x00%v\x00%v", hash, matchDocId, matchIndex, matchDistance, matchLevel)

			actualDistance := index - matchIndex
			if actualDistance/matchDistance > 1.1 {
				delete(pb.Keys[hash], thisKey)
				if len(pb.Keys[hash]) == 0 {
					delete(pb.Keys, matchKey)
				}
				continue
			}

			if isLast {
				//log.Print("Match!")
			}

			for nextKeyStr, nextIsLast := range pb.Keys[matchKey] {
				parts = strings.Split(nextKeyStr, "\x00")
				nextHash = parts[0]
				nextDocId = parts[1]
				nextIndex = parts[2]
				nextDistance = parts[3]
				nextLevel = parts[4]

				nextKey = fmt.Sprintf("%v\x00%v\x00%v\x00%v", nextDocId, nextIndex, nextDistance, nextLevel)

				if _, ok := pb.State[nextHash]; !ok {
						pb.State[nextHash] = map[string]bool{}
				}

				pb.State[nextHash][nextKey] = nextIsLast
			}
		}
}

	if _, ok := pb.Keys[hash]; ok {

		for nextKey, nextIsLast := range pb.Keys[hash] {
			parts = strings.Split(nextKey, "\x00")
			// sampledHash.Hash, docId, index, distance, level
			nextHash = parts[0]
			nextDocId = parts[1]
			nextIndex = parts[2]
			nextDistance = parts[3]
			nextLevel = parts[4]

			pb.LevelCount["0"] += 1

			if _, ok := pb.State[nextHash]; !ok {
					pb.State[nextHash] = map[string]bool{}
			}

			nextKey = fmt.Sprintf("%v\x00%v\x00%v\x00%v", nextDocId, nextIndex, nextDistance, nextLevel)

			pb.State[nextHash][nextKey] = nextIsLast
		}
	}
}

func (pb PBHash) Process(index int, docId string, reader *bufio.Reader, match bool) {
	feature := Create()
	_ = feature
	pb.State = map[string]map[string]bool{}
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
