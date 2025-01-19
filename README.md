# Sorting-compressed-time-series

This is the repository for Compressed-Sort algorithm. 

The evaluation code is based on IoTDB.

User Guide for building IoTDB from source could be found here: https://github.com/apache/iotdb

User Guide for building TsFile from source could be found here: https://github.com/apache/tsfile

## File Structure
+ `iotdb/`: include the scripts and database used for evaluations
    + `OrderSensitiveTimeEncoder&OrderSensitiveValueEncoder`: the encoding method proposed in the paper
    + `CompressedBubbleSorter`: the compressed bubble sort algorithm proposed in the paper
    + `CompressedMergeSorter`: the compressed merge sort algorithm proposed in the paper
+ `tsfile/`: show the compressed-sort operator implemented for this file format
    + `TsFileSorter`: the operator for compressed-bubble sort
    + `UnsortedFileGenerator`: generate a file containing unordered data by specifying the dataset
    + `SortTest`: test the compressed sort operator
+ `datasets/`: include all the public datasets used for evaluations


## Steps

+ `Compresseion performance`: all the test code for encoder performance is in `EncodeDecodeTest.java`
+ `Compressed bubble sort performance`: all the test code for Memtable sort performance is in `CompressedBubbleSorterTest.java`
    + `prepareData()`: choose the dataset that will be used in the following test
    + `testSortTime()`: for the specified parameters, test the sort time of different methods.
    + `testCompressedBubbleSortMemory() & testOldSortMemory()`: for the specified parameters, test the heap usage for compressed bubble sort and other methods
+ `Compressed merge sort performance`: all the test code for Compaction sort performance is in `CompressedMergeSorterTest.java`
    + `prepareData()`: choose the dataset that will be used in the following test
    + `testMergeTime()`: for the specified parameters, test the merge time of different methods
    + `testNewMemoryPrepare() & testOldMemoryPrepare()`: for the specified parameters, prepare the compressed data that will be used in the following test
    + `testNewMemory() & testOldMemory()`: for the specified parameters, test the heap usage for compressed merge sort and other methods.