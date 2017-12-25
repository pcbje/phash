### Hash generation
Based on sdhash, but using a rolling hash (buzhash), instead of sha1
Hashes are grouped into words.
The word length / partition size is Sqrt(|hashes|). Each hash in a word is assigned the distance to it's predecessor.
There are two classes of words: global and local. All global words contain one hash for each partition, while a local word for a partition contain all hashes for that partition. This enables matching of both similarity and embeddedness.
The number of identical words in two files is their similarity.

### Matching
Tree structure
Inspired by Aho-Corasick, but we allow overlapping, different words

At any position in a file, there is a number of accepted hashes. Each accepted hash is associated with a set of valid offsets. If the current hash is within a valid offset, the successors of that hash/offset pairs become valid hashes.

### Hybrid data structure
If we call how far in a word matching as come as a "run", we expect the distribution of run lengths to be skewed towards low (experiment). This means that it is not necessary to keep the deeper (suffix) levels of words in memory. This should enable both memory and speed efficiency at scale.
Prefix in memory
Suffix on disk
