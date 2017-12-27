package main

import (
	"bufio"
	"log"
	"math"
	"math/rand"
)

type Feature struct {
	Hash   uint32
	Random float64
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

	seenHashes := map[uint32]bool{}

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

			// Ignore very low and very high ([0, 1000]) entropy windows.
			if scores[windowIndex] < 100 || scores[windowIndex] > 980 {
				scores[windowIndex] = 0
			}

			if scores[windowIndex] > scores[popularWindowIndex] {
				popularWindowIndex = windowIndex
			} else if windowIndex == popularWindowIndex {
				// Popular index has been dropped and we need to find the new popular index.
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

				if _, seenHash := seenHashes[hashes[popularWindowIndex]]; !seenHash {
					randomNumber := pb.Random.Float64()

					if randomNumber <= 1.0/math.Sqrt(float64(len(features))) {
						seenHashes[hashes[popularWindowIndex]] = true
						features = append(features, Feature{
							Hash:   hashes[popularWindowIndex],
							Random: randomNumber,
							Index:  fileIndex,
						})
					}
				}
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
		expectedFactor       float64     = math.Sqrt(2)
		minWordLength        int         = 5
		expectedFeatureCount float64     = float64(len(features)) / expectedFactor
		wordLength           float64     = math.Max(float64(minWordLength), math.Sqrt(math.Sqrt(expectedFeatureCount)))
		wordCount            float64     = math.Max(1, expectedFeatureCount/wordLength)
		threshold            float64     = 1.0 / math.Sqrt(float64(len(features)))
		partitionCount       float64     = math.Floor((expectedFeatureCount) / wordLength)
		randomwords          [][]Feature = make([][]Feature, int(wordCount))
		partitions           [][]Feature = make([][]Feature, int(partitionCount))
		partitionSize        float64     = math.Ceil(expectedFactor / partitionCount)
		w                    int         = len(randomwords)
		i                    float64     = 0
		partition            float64     = 0
	)

	for _, hash := range features {
		if hash.Random > threshold {
			continue
		}

		if partitionCount > 0 {
			partition = math.Floor(i / partitionSize)
			partition = math.Max(0, math.Min(partition, partitionCount-1))
			partitions[int(partition)] = append(partitions[int(partition)], hash)
		}

		randomwords[int(i)%w] = append(randomwords[int(i)%w], hash)
		i += 1
	}

	for _, word := range append(randomwords, partitions...) {
		// Won't compare short words...
		if len(word) < minWordLength {
			continue
		}

		pb.Committed[docId]++

		var key *Transition
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
					DocId:    docId,
					Position: word[level].Index,
					Distance: word[level].Index - word[level-1].Index,
					Level:    level,
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
			pb.LevelCount[transition.Level] += 1

			actualDistance := index - transition.Position
			if actualDistance/transition.Distance > 1.25 {
				delete(pb.Keys[*hash], *&transition)
				if len(pb.Keys[*hash]) == 0 {
					delete(pb.Keys, *hash)
				}
				continue
			}

			for nextState, _ := range pb.Keys[*&transition] {
				// Wrong docId.
				if nextState.DocId != transition.DocId {
					log.Panic("This is not supposed to happen...")
				}

				// We have a match.
				if nextState.Last {
					pb.Matches[docId][transition.DocId] += 1
					delete(pb.State[docId][*hash], *&transition)
					continue
				}

				nextTransition := &Transition{Hash: nextState.Hash}

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

			tr := &Transition{Hash: transition.Hash}

			if _, ok := pb.State[docId][*tr]; !ok {
				pb.State[docId][*tr] = map[Transition]bool{}
			}

			pb.State[docId][*tr][*&transition] = false
		}
	}
}
