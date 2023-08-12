# Scanleft in Apache Spark
This implement similar method to the RDD class
as the regular *.scanleft()* that is part of the scala collections.

## Implementation
The implementation send the result of each partition to the next. If there are *N* partitions, there will be *N* calls to compute the result of each partition. Then, the results are applied to the entire RDD

## Performance
The large scale performance is not tested since I just did this for fun and only in local mode.
It is recomended to only do numerical methods such as accumulative sum etc.