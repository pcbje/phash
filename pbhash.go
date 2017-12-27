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

type Config struct {
	WindowSize   int
	Bins         int
	EntropyPower uint
	EntropyScale int
	Entropy64    []int
}

func GetConfig() Config {
	return Config{
		WindowSize:   64,
		Bins:         1000,
		EntropyPower: 10,
		/*
			i := 1
			for i < c.WindowSize {
				p := float64(i) / float64(c.WindowSize)
				c.Entropy64[i] = int((-p * math.Log2(p) / 6) * float64(c.EntropyScale))
				i += 1
			}
		*/
		Entropy64:    []int{0, 16000, 26666, 35320, 42666, 49040, 54640, 59596, 64000, 67921, 71415, 74523, 77281,
			79718, 81858, 83724, 85333, 86701, 87843, 88771, 89497, 90030, 90380, 90554, 90562, 90409, 90102,
			89648, 89050, 88316, 87448, 86453, 85333, 84093, 82736, 81266, 79687, 78000, 76210, 74318, 72327,
			70240, 68060, 65788, 63426, 60977, 58443, 55824, 53124, 50344, 47485, 44550, 41539, 38453, 35296,
			32067, 28768, 25400, 21965, 18464, 14897, 11266, 7572, 3816, 0},
		// c.Bins * (1 << c.EntropyPower)
		EntropyScale: 1024000,
	}
}


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

type Feature struct {
	Config          Config
	Hasher          *BuzHash
	Ascii           []int
	Window          []byte
	Ranks           []int
	PreviousEntropy int
	Position        int
}


func (f Feature) Compute(index int, pb PBHash, docId string, reader *bufio.Reader, match bool) []SampledHash {
	buf := make([]byte, 1024)
	var drop byte
	var old_diff int
	var new_diff int
	var entropy int
	var score int
	var v int
	var p float64
	i := 0
	p = 0.0
	score = 0
	max_score := 0
	max_index := 0
	fs := 0.0

	ws := float64(f.Config.WindowSize)

	hashes := make([]uint32, f.Config.WindowSize)
	scores := make([]int, f.Config.WindowSize)
	counts := make([]int, f.Config.WindowSize)
	m := map[uint32]float64{}

	features := []SampledHash{}

	y := 0
	_ = fmt.Println


	var k int
	for {
		v, _ = reader.Read(buf)
		if v == 0 {
			break
		}

		j := 0
		for j < v {
			b := buf[j]
			f.Hasher.HashByte(b)
			hashes[i] = f.Hasher.Sum32()

			drop = f.Window[i]
			f.Window[i] = b

			old_diff = 0
			if p >= ws {
				f.Ascii[drop] = f.Ascii[drop] - 1
				old_diff = f.Config.Entropy64[f.Ascii[drop]+1] - f.Config.Entropy64[f.Ascii[drop]]
			}

			f.Ascii[b] = f.Ascii[b] + 1

			new_diff = f.Config.Entropy64[f.Ascii[b]] - f.Config.Entropy64[f.Ascii[b]-1]
			entropy = f.PreviousEntropy - old_diff + new_diff

			f.PreviousEntropy = entropy

			score = entropy>>f.Config.EntropyPower

			if score < 100 || score > 980 {
				score = 0
			}

			scores[i] = score
			counts[i] = 0

			if score > max_score {
				max_score = score
				max_index = i
			} else if i == max_index {
				max_index = 0
				max_score = 0

				k = 0
				for k < f.Config.WindowSize {
					if scores[k] > max_score {
						max_score = scores[k]
						max_index = k
					}
					k++
				}
			}

			counts[max_index] += 1



			if counts[max_index] == 8 {
				pb.Match(docId, p, hashes[max_index])
			}

			if counts[max_index] == 8 {
				r := rand.Float64()

				if r <= 1.0/fs {
					fs = fs + 1.0

					if _, ok := m[hashes[max_index]]; !ok {
						m[hashes[max_index]] = 0
					}

					if m[hashes[max_index]] < 1 {
						features = append(features, SampledHash{
							Hash:   hashes[max_index],
							Random: r,
							Index:  p,
						})

						m[hashes[max_index]] += 1
					}

					//counts[max_index] = -99
				}
			} else {
				y += 1
			}

			j++
			i++
			p++
			if i == f.Config.WindowSize {
				i = 0
			}
		}
	}

	log.Print(index, match, len(features), y, float64(len(features)*100)/float64(y))
	return features
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
	partitionSize := math.Max(1, float64(wordLength))
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
		// Won't compare short words...
		if len(word) < 4 {
			continue
		}

		cw += 1
		level := len(word) - 1
		var key string
		var pkey string
		pkey = fmt.Sprintf("done\x00%v", docId)
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


			pb.Keys[key][pkey] = level == len(word)-2


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

		for thisKey, _ := range pb.State[hash] {
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

			if _, last := pb.Keys[matchKey][fmt.Sprintf("done\x00%v", matchDocId)]; last {
				pb.Matches[docId][matchDocId] += 1
				delete(pb.State[hash], thisKey)
			}

			for nextKeyStr, nextIsLast := range pb.Keys[matchKey] {
				parts = strings.Split(nextKeyStr, "\x00")
				if len(parts) == 2 {
					continue
				}
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


func Create() Feature {
	_ = log.Print

	c := GetConfig()

	return Feature{
		Config:          c,
		Hasher:          NewBuzHash(uint32(16)),
		Ascii:           make([]int, 256),
		Window:          make([]byte, c.WindowSize),
		PreviousEntropy: 0,
		Position:        0,
	}
}
