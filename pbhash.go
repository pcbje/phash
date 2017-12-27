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
	State      map[Transition]map[Transition]bool
	Keys       map[Transition]map[Transition]bool
	LevelCount map[int]int
	Random     *rand.Rand
}

func (f PBHash) GetFeatures(index int, pb PBHash, docId string, reader *bufio.Reader, match bool) []Feature {
	/*
		entropyScale := 1024000
		i := 1
		for i < int(windowSize) {
			p := float64(i) / windowSize
			entropyTable[i] = int((-p * math.Log2(p) / 6) * float64(entropyScale))
			i += 1
		}
	*/
	entropyTable := []int{0, 16000, 26666, 35320, 42666, 49040, 54640, 59596, 64000, 67921, 71415, 74523, 77281,
		79718, 81858, 83724, 85333, 86701, 87843, 88771, 89497, 90030, 90380, 90554, 90562, 90409, 90102,
		89648, 89050, 88316, 87448, 86453, 85333, 84093, 82736, 81266, 79687, 78000, 76210, 74318, 72327,
		70240, 68060, 65788, 63426, 60977, 58443, 55824, 53124, 50344, 47485, 44550, 41539, 38453, 35296,
		32067, 28768, 25400, 21965, 18464, 14897, 11266, 7572, 3816, 0}

	var (
		windowSize          float64 = 64.0
		entropyPower        uint32  = 10
		fileIndex           float64 = 0.0
		popularWindowIndex  int     = 0
		windowIndex         int     = 0
		previousEntropy     int     = 0
		popularityThreshold int     = 8

		ascii  []int    = make([]int, 256)
		window []byte   = make([]byte, int(windowSize))
		hashes []uint32 = make([]uint32, int(windowSize))
		scores []int    = make([]int, int(windowSize))
		counts []int    = make([]int, int(windowSize))
		buffer []byte   = make([]byte, 1024)

		features []Feature = []Feature{}
		hasher   *BuzHash  = NewBuzHash(uint32(16))
	)

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

			if scores[windowIndex] < 100 || scores[windowIndex] > 980 {
				scores[windowIndex] = 0
			}

			if scores[windowIndex] > scores[popularWindowIndex] {
				popularWindowIndex = windowIndex
			} else if windowIndex == popularWindowIndex {
				popularWindowIndex = 0
				for index, score := range scores {
					if score > scores[popularWindowIndex] {
						popularWindowIndex = index
					}
				}
			}

			counts[popularWindowIndex]++

			if counts[popularWindowIndex] == popularityThreshold {
				pb.Match(docId, fileIndex, hashes[popularWindowIndex])

				randomNumber := pb.Random.Float64()

				if randomNumber <= 1.0/float64(len(features)) {
					features = append(features, Feature{
						Hash:   hashes[popularWindowIndex],
						Random: randomNumber,
						Index:  fileIndex,
					})
				}
			}

			fileIndex++
			windowIndex++

			if windowIndex == int(windowSize) {
				windowIndex = 0
			}
		}
	}

	log.Print(index, match, len(features), float64(len(features)*100))
	return features
}

func (pb PBHash) Commit(docId string, features []Feature) {
	if len(features) == 0 {
		log.Print("No hashes sampled for doc: ", docId)
		return
	}

	var (
		wordLength     int         = int(math.Sqrt(math.Sqrt(float64(len(features) / 2))))
		wordCount      int         = (len(features) / 2) / wordLength
		threshold      float64     = 1.0 / float64(len(features))
		partitionSize  float64     = math.Max(4, float64(wordLength))
		partitionCount int         = (len(features) / 2) / int(partitionSize)
		randomwords    [][]Feature = make([][]Feature, wordCount)
		partitions     [][]Feature = make([][]Feature, partitionCount)
		w              uint32      = uint32(len(randomwords))
		i              float64     = 0
		cw             int         = 0
	)

	for _, hash := range features {
		if hash.Random > threshold {
			continue
		}

		wordIndex := hash.Hash % w
		partition := int(math.Min(float64(len(partitions)-1), math.Floor(i/partitionSize)))
		randomwords[wordIndex] = append(randomwords[wordIndex], hash)

		if len(partitions) > 1 {
			partitions[partition] = append(partitions[partition], hash)
		}
		i += 1
	}

	for _, word := range append(randomwords, partitions...) {
		// Won't compare short words...
		if len(word) < 4 {
			continue
		}

		cw += 1
		level := len(word) - 1
		var key *Transition
		var pkey *Transition
		pkey = &Transition{DocId: docId, Last: true}

		var sampledHash Feature
		for level > -1 {
			sampledHash = word[level]

			if level > 0 {
				key = &Transition{
					Hash:     sampledHash.Hash,
					DocId:    docId,
					Position: sampledHash.Index,
					Distance: word[level].Index - word[level-1].Index,
					Level:    level,
				}
			} else {
				key = &Transition{
					Hash: sampledHash.Hash,
				}
			}

			if _, ok := pb.Keys[*key]; !ok {
				pb.Keys[*key] = map[Transition]bool{}
			}

			pb.Keys[*key][*pkey] = level == len(word)-2

			pkey = key
			level -= 1
		}
	}

	pb.Committed[docId] = cw
}

func (pb PBHash) Match(docId string, index float64, ihash uint32) {
	hash := &Transition{Hash: ihash}

	if _, ok := pb.State[*hash]; ok {
		for transition, _ := range pb.State[*hash] {
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
				// Wrong doc.
				if nextState.DocId != transition.DocId {
					log.Panic("This is not supposed to happen...")
				}

				// We have a match.
				if nextState.Last {
					pb.Matches[docId][transition.DocId] += 1
					delete(pb.State[*hash], *&transition)
					continue
				}

				nextTransition := &Transition{Hash: nextState.Hash}

				if _, ok := pb.State[*nextTransition]; !ok {
					pb.State[*nextTransition] = map[Transition]bool{}
				}

				pb.State[*nextTransition][*&nextState] = false

				// Allow same transition multiple times?
				delete(pb.State[*hash], *&transition)
			}
		}
	}

	if _, ok := pb.Keys[*hash]; ok {
		for transition, _ := range pb.Keys[*hash] {
			pb.LevelCount[0] += 1

			tr := &Transition{Hash: transition.Hash}

			if _, ok := pb.State[*tr]; !ok {
				pb.State[*tr] = map[Transition]bool{}
			}

			pb.State[*tr][*&transition] = false
		}
	}
}

func (pb PBHash) Process(index int, docId string, reader *bufio.Reader, match bool) {
	pb.State = map[Transition]map[Transition]bool{}

	pb.Matches[docId] = map[string]int{}

	docFeatures := pb.GetFeatures(index, pb, docId, reader, match)
	pb.Commit(docId, docFeatures)
}
