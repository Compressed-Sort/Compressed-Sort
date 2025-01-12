# Sorting-compressed-time-series

This is the repository for Compressed-Sort algorithm. 

The evaluation code is based on IoTDB.

User Guide for building from source could be found here: https://github.com/apache/iotdb

## File Structure
+ `iotdb/`: include the scripts and database used for evaluations
    + `OrderSensitiveTimeEncoder&OrderSensitiveValueEncoder`: the encoding method proposed in the paper
    + `CompressedBubbleSorter`: the compressed bubble sort algorithm proposed in the paper
    + `CompressedMergeSorter`: the compressed merge sort algorithm proposed in the paper
+ `datasets/`: include all the public datasets used for evaluations


## Steps

+ `Compresseion performance`: all the test code for encoder performance is in `EncodeDecodeTest.java`
+ `Compressed bubble sort performance`: all the test code for Memtable sort performance is in `CompressedBubbleSorterTest.java`
+ `Compressed merge sort performance`: all the test code for Compaction sort performance is in `CompressedMergeSorterTest.java`