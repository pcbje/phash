package main

import (
	"bufio"
	"log"
	"math"
	"math/rand"
)

type Feature struct {
	Hash   uint32
	Index  float64
}

type Transition struct {
	Hash     uint32
	DocId    string
	Position float64
	Distance float64
	Level    int
	Last     bool
}

type PBHash struct {
	Committed  map[string]int
	Matches    map[string]map[string]int
	State      map[string]map[Transition]map[Transition]bool
	Keys       map[Transition]map[Transition]bool
	LevelCount map[int]int
	Random     *rand.Rand
}

func (pb *PBHash) GetFeatures(index int, docId string, reader *bufio.Reader, match bool) []Feature {
	/*
		entropyScale := bins << entropyPower (1024000)
		i := 1
		for i < int(windowSize) {
			p := float64(i) / windowSize
			entropyTable[i] = int((-p * math.Log2(p) / 6) * float64(entropyScale))
			i += 1
		}
	*/
	entropyTable := []int{
		0, 16000, 26666, 35320, 42666, 49040, 54640, 59596, 64000, 67921, 71415, 74523, 77281,
		79718, 81858, 83724, 85333, 86701, 87843, 88771, 89497, 90030, 90380, 90554, 90562, 90409, 90102,
		89648, 89050, 88316, 87448, 86453, 85333, 84093, 82736, 81266, 79687, 78000, 76210, 74318, 72327,
		70240, 68060, 65788, 63426, 60977, 58443, 55824, 53124, 50344, 47485, 44550, 41539, 38453, 35296,
		32067, 28768, 25400, 21965, 18464, 14897, 11266, 7572, 3816, 0}

	var (
		windowSize          float64 = 64.0
		fileIndex           float64 = 0.0
		entropyPower        uint32  = 10
		popularWindowIndex  int     = 0
		windowIndex         int     = 0
		previousEntropy     int     = 0
		popularityThreshold int     = 16

		ascii  []int    = make([]int, 256)
		window []byte   = make([]byte, int(windowSize))
		hashes []uint32 = make([]uint32, int(windowSize))
		scores []int    = make([]int, int(windowSize))
		counts []int    = make([]int, int(windowSize))
		buffer []byte   = make([]byte, 1024)

		features []Feature = []Feature{}
		hasher   *BuzHash  = NewBuzHash(uint32(31))
	)

	pb.Matches[docId] = map[string]int{}
	pb.State[docId] = map[Transition]map[Transition]bool{}

	for {
		readLength, _ := reader.Read(buffer)
		bufferIndex := 0

		if readLength == 0 {
			break
		}

		for bufferIndex < readLength {
			nextByte := buffer[bufferIndex]
			dropByte := window[windowIndex]

			bufferIndex++

			hasher.HashByte(nextByte)

			hashes[windowIndex] = hasher.Sum32()
			window[windowIndex] = nextByte

			dropDiff := 0
			if fileIndex >= windowSize {
				ascii[dropByte]--
				dropDiff = entropyTable[ascii[dropByte]+1] - entropyTable[ascii[dropByte]]
			}

			ascii[nextByte]++

			nextDiff := entropyTable[ascii[nextByte]] - entropyTable[ascii[nextByte]-1]
			entropy := previousEntropy - dropDiff + nextDiff
			previousEntropy = entropy

			counts[windowIndex] = 0
			scores[windowIndex] = entropy >> entropyPower

			if scores[windowIndex] > scores[popularWindowIndex] {
				popularWindowIndex = windowIndex
			} else if windowIndex == popularWindowIndex {
				// Popular index has been dropped and we need to find the current popular index.
				maxDistance := windowSize - float64(popularityThreshold)
				currentIndex := float64(windowIndex - 1)

				if currentIndex < 0 {
					currentIndex = windowSize - 1
				}

				popularWindowIndex = int(currentIndex)

				currentDistance := 0.0
				for currentDistance < maxDistance {
					currentDistance++

					currentIndex--
					if currentIndex < 0 {
						currentIndex = windowSize - 1
					}

					reachableScore := scores[int(currentIndex)] + int(currentDistance)

					if scores[int(currentIndex)] > scores[popularWindowIndex] && reachableScore >= popularityThreshold {
						popularWindowIndex = int(currentIndex)
					}
				}
			}

			counts[popularWindowIndex]++

			if counts[popularWindowIndex] == popularityThreshold {
				pb.Match(docId, fileIndex, hashes[popularWindowIndex])

				features = append(features, Feature{
					Hash:   hashes[popularWindowIndex],
					Index:  fileIndex,
				})
			}

			fileIndex++
			windowIndex++

			if windowIndex == int(windowSize) {
				windowIndex = 0
			}
		}
	}

	// Cleanup.
	delete(pb.State, docId)

	return features
}

func (pb *PBHash) CommitFeatures(docId string, features []Feature) {
	var (
		minWordLength        int         = 5
		expectedFeatureCount float64     = float64(len(features)) / math.Sqrt(2)
		wordLength           float64     = math.Max(float64(minWordLength), math.Sqrt(math.Sqrt(expectedFeatureCount)))
		wordCount            float64     = math.Max(1, math.Floor(expectedFeatureCount/wordLength))
		partitionCount       float64     = wordCount
		threshold            float64     = 1.0 / math.Sqrt(float64(len(features)))
		partitionSize        float64     = math.Ceil(expectedFeatureCount / partitionCount)
		addedFeatures        float64     = 0
		partition            float64     = 0
		randomwords          [][]Feature = make([][]Feature, int(wordCount))
		partitions           [][]Feature = make([][]Feature, int(partitionCount))
	)

	for _, hash := range features {
		if partitionCount > 0 {
			partition = math.Floor(addedFeatures / partitionSize)
			partition = math.Max(0, math.Min(partition, partitionCount-1))

			// "Embedded score += 1"
			partitions[int(partition)] = append(partitions[int(partition)], hash)
		}

		wordIndex := int(addedFeatures) % len(randomwords)

		// "Similarity score += 1"
		randomwords[wordIndex] = append(randomwords[wordIndex], hash)
		addedFeatures += 1
	}

	for _, word := range append(randomwords, partitions...) {
		// Won't compare such short words...
		if len(word) <= 3 {
			continue
		}

		pb.Committed[docId]++

		var key *Transition

		// Last part of the word.
		var pkey *Transition = &Transition{
			DocId: docId,
			Last:  true,
		}

		level := len(word) - 1

		for level > -1 {
			// First key is special (hash-only to start a run)
			if level == 0 {
				key = &Transition{
					Hash: word[level].Hash,
				}
			} else {
				key = &Transition{
					Hash:     word[level].Hash,
					// Math.Log(Distance)?
					Distance: word[level].Index - word[level-1].Index,
				}
			}

			if _, ok := pb.Keys[*key]; !ok {
				pb.Keys[*key] = map[Transition]bool{}
			}

			pb.Keys[*key][*pkey] = false

			pkey = key
			level--
		}
	}
}

func (pb *PBHash) Match(docId string, index float64, ihash uint32) {
	hash := &Transition{Hash: ihash}

	if _, ok := pb.State[docId][*hash]; ok {
		for transition, _ := range pb.State[docId][*hash] {
			actualDistance := index - transition.Position

			// Too far away.
			if actualDistance/transition.Distance > 1.25 {
				delete(pb.Keys[*hash], *&transition)
				if len(pb.Keys[*hash]) == 0 {
					delete(pb.Keys, *hash)
				}
				continue
			}

			for nextState, _ := range pb.Keys[*&transition] {
				// We have a match.
				if nextState.Last {
					pb.Matches[docId][transition.DocId] += 1
					delete(pb.State[docId][*hash], *&transition)
					continue
				}

				nextTransition := &Transition{Hash: nextState.Hash, Position: index}

				if _, ok := pb.State[docId][*nextTransition]; !ok {
					pb.State[docId][*nextTransition] = map[Transition]bool{}
				}

				pb.State[docId][*nextTransition][*&nextState] = false

				// Allow same transition multiple times?
				delete(pb.State[docId][*hash], *&transition)
			}
		}
	}

	if _, ok := pb.Keys[*hash]; ok {
		for transition, _ := range pb.Keys[*hash] {
			pb.LevelCount[0] += 1

			tr := &Transition{Hash: transition.Hash, Position: index}

			if _, ok := pb.State[docId][*tr]; !ok {
				pb.State[docId][*tr] = map[Transition]bool{}
			}

			pb.State[docId][*tr][*&transition] = false
		}
	}
}
