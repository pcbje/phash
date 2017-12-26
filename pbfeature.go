package main

import (
	"bufio"
	"log"
	"math/rand"
)

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

	hashes := make([]uint32, f.Config.WindowSize)
	scores := make([]int, f.Config.WindowSize)
	counts := make([]int, f.Config.WindowSize)
	m := map[uint32]float64{}

	features := []SampledHash{}

	y := 0

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
			if f.Ascii[drop] > 0 {
				f.Ascii[drop] = f.Ascii[drop] - 1
				old_diff = f.Config.Entropy64[f.Ascii[drop]+1] - f.Config.Entropy64[f.Ascii[drop]]
			}

			f.Ascii[b] = f.Ascii[b] + 1

			new_diff = f.Config.Entropy64[f.Ascii[b]] - f.Config.Entropy64[f.Ascii[b]-1]
			entropy = f.PreviousEntropy - old_diff + new_diff

			f.PreviousEntropy = entropy

			score = f.Config.EntropyRanks[entropy>>f.Config.EntropyPower]

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

			if counts[max_index] >= 32 {
				r := rand.Float64()

				if r <= 1.0/fs {
					fs = fs + 1.0

					if _, ok := m[hashes[max_index]]; !ok {
						m[hashes[max_index]] = 0
					}

					if m[hashes[max_index]] <= 2 {
						features = append(features, SampledHash{
							Hash:   hashes[max_index],
							Random: r,
							Index:  p,
						})
						m[hashes[max_index]] += 1
					}

					counts[max_index] = -99
				}
			} else {
				y += 1
			}

			if match && counts[max_index] == 32 {
			  // For matching
			  pb.Match(docId, p, hashes[max_index])
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

func Create() Feature {
	_ = log.Print

	c := GetConfig()

	return Feature{
		Config:          c,
		Hasher:          NewBuzHash(uint32(31)),
		Ascii:           make([]int, 256),
		Window:          make([]byte, c.WindowSize),
		PreviousEntropy: 0,
		Position:        0,
	}
}
