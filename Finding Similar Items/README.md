

Locality-Sensitive Hashing (LSH) is an algorithm which reduces the dimensionality of high-dimensional data. LSH hashes input items so that similar items map to the same “buckets” with high probability (the number of buckets being much smaller than the universe of possible input items). The Scala script implements this algorithm with minhash signatures based on Jaccard similarity, which reduces the dimensions of the data while keeping their similarity relatively close. 

To run the algorithm, you need to provide two arguments.

1. The first argument is the path of the input file. I provided a dataset called "video_small_num.csv" as an example in this directory. The input file needs to conform the format of the example dataset. 

2. The second argument is the path to the output file followed by its file name.