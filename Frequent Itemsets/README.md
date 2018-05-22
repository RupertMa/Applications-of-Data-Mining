# Applications-of-Data-Mining

Savasere, Omiecinski and Navathe (SON) Algorithm is an efficient algorithm to find frequent itemsets and avoid both false positives and false negatives. The Scala script contained in this directory implements this algorithm using Spark. For each chunk of the distributed file system, A-Priori algorithm is used to find all frequent itemsets. You may to use other in-memory algorithms, such as the simple, randomized algorithm instead.  

To run the algorithm, you need to input three arguments.  

1. The first argument is a case number. Case number 1 will make the algorithm find all sizes of frequent product itemsets. Case number 2 will make the algorithm find all sizes of frequent reviewer itemsets.  

2. The second argument is the absolute path of the input file. I provided a dataset called "beauty.csv" as an example in this directory. The input file needs to conform the format of the dataset.  

3. The third argument is the support parameter for SON algorithm.  