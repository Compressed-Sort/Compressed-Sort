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
    + `Samsung`: This dataset is generated using standard commercial protocols, networks, and devices commonly found in IoT applications, corresponding to several real-world usage scenarios.
    + `CarNet`: This is a vehicle dataset from our industrial partner that contains data from over 7 million vehicles of a specific brand during real-world operation, totaling over 200 million time series. 
    + `ShipNet`: This dataset, obtained from the U.S. Coast Guard’s AIS network, contains historical vessel traffic data. It includes information on vessel location, time, and characteristics, and is formatted for analysis in geographic information system software.
    + `Exponential`: This is an artificial dataset in which we use 1 as the time interval of occurrence and an exponential distribution with a parameter of 1 to simulate delays in the time transmission process. For the value column, we generated random values uniformly distributed between -100 and 100.

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