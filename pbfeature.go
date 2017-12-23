package main

import (
  "math"
  "log"
  "bufio"
)

type Config struct {
  WindowSize int
  Bins int
  EntropyPower uint
  EntropyScale int
  Entropy64 []int
  EntropyRanks []int
}

type Feature struct {
  Config Config
  Hasher *BuzHash
	Ascii []int
	Window []byte
	Ranks []int
	PreviousEntropy int
	Position int
}

func (f Feature) Compute(reader *bufio.Reader) {
  buf := make([]byte, 1024)
  var drop byte
  var old_diff int
  var new_diff int
  var entropy int
  var score int
  var hash uint32
	var v int
  i := 0
  p := 0
  score = 0
  max_score := 0
  max_index := 0

  scores := make([]int, f.Config.WindowSize)
  counts := make([]int, f.Config.WindowSize)

  hashes := []uint32{}
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

        drop = f.Window[i]
        f.Window[i] = b

        old_diff = 0
        if f.Ascii[drop] > 0 {
          f.Ascii[drop] = f.Ascii[drop] - 1
          old_diff = f.Config.Entropy64[f.Ascii[drop] + 1] - f.Config.Entropy64[f.Ascii[drop]]
        }

        f.Ascii[b] = f.Ascii[b] + 1

        new_diff = f.Config.Entropy64[f.Ascii[b]] - f.Config.Entropy64[f.Ascii[b] - 1]
        entropy = f.PreviousEntropy - old_diff + new_diff

        f.PreviousEntropy = entropy

        score = f.Config.EntropyRanks[entropy >> f.Config.EntropyPower]

        scores[i] = score
        counts[i] = 0;

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
            k++;
          }
        }

        counts[max_index] += 1

        if counts[max_index] == 16 {
          // For commit
          hash = f.Hasher.Sum32()
          hashes = append(hashes, hash)
        } else {
          y += 1
        }

        /*if counts[max_index] == 8 {
          // For matching
          hash = f.Hasher.Sum32()
          _ = hash
        }*/

				j++;
        i++;
        p++;
        if i == f.Config.WindowSize {
          i = 0
        }
			}
	}

  log.Print(len(hashes), y, float64(len(hashes)*100)/float64(y))
}

func Create() Feature {
  _ = log.Print

  c := Config{
    WindowSize: 64,
    Bins: 1000,
    EntropyPower: 10,
    Entropy64: make([]int, 65),
    EntropyRanks: ENTROPY_RANKS(),
  }

  c.EntropyScale = c.Bins * (1 << c.EntropyPower)

  i := 1
  for i < c.WindowSize {
    p := float64(i) / float64(c.WindowSize)
    c.Entropy64[i] = int((-p * math.Log2(p) / 6) * float64(c.EntropyScale))
    i += 1
  }

	return Feature{
    Config: c,
    Hasher: NewBuzHash(uint32(31)),
		Ascii: make([]int, 256),
		Window: make([]byte, c.WindowSize),
    PreviousEntropy: 0,
    Position: 0,
	}
}
