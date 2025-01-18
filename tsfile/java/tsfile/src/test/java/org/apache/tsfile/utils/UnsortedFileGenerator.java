package org.apache.tsfile.utils;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.schema.Schema;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class UnsortedFileGenerator {
    // generate unsorted page and chunk in a file
    private static final Logger LOG = LoggerFactory.getLogger(FileGenerator.class);
    public static String outputDataFile =
            TsFileGeneratorForTest.getTestTsFilePath("root.sg1", 0, 0, 0);
    public static Schema schema;
    private static int ROW_COUNT = 1000;
    private static TsFileWriter innerWriter;
    private static String inputDataFile;
    private static String errorOutputDataFile;
    private static final TSFileConfig config = TSFileDescriptor.getInstance().getConfig();


    public static void generateFile(int rowCount, int maxNumberOfPointsInPage) throws IOException {
        ROW_COUNT = rowCount;
        int oldMaxNumberOfPointsInPage = config.getMaxNumberOfPointsInPage();
        config.setMaxNumberOfPointsInPage(maxNumberOfPointsInPage);

        prepare();
        write();
        config.setMaxNumberOfPointsInPage(oldMaxNumberOfPointsInPage);
    }

    public static void prepare() throws IOException {
        File file = new File(outputDataFile);
        if (!file.getParentFile().exists()) {
            Assert.assertTrue(file.getParentFile().mkdirs());
        }
        inputDataFile = TsFileGeneratorForTest.getTestTsFilePath("root.sg1", 0, 0, 1);
        file = new File(inputDataFile);
        if (!file.getParentFile().exists()) {
            Assert.assertTrue(file.getParentFile().mkdirs());
        }
        errorOutputDataFile = TsFileGeneratorForTest.getTestTsFilePath("root.sg1", 0, 0, 2);
        file = new File(errorOutputDataFile);
        if (!file.getParentFile().exists()) {
            Assert.assertTrue(file.getParentFile().mkdirs());
        }
        generateTestSchema();
        generateSampleInputDataFile();
    }

    public static void after() {
        after(outputDataFile);
    }

    public static void after(String filePath) {
        File file = new File(inputDataFile);
        if (file.exists()) {
            file.delete();
        }
        file = new File(filePath);
        if (file.exists()) {
            file.delete();
        }
        file = new File(errorOutputDataFile);
        if (file.exists()) {
            file.delete();
        }
    }

    private static void generateSampleInputDataFile() throws IOException {
        long[] times = new long[ROW_COUNT];
        long[] values = new long[ROW_COUNT];
        prepareData(times, values);
        File file = new File(inputDataFile);
        if (file.exists()) {
            file.delete();
        }
        file.getParentFile().mkdirs();
        FileWriter fw = new FileWriter(file);

        for (int i = 0; i < ROW_COUNT; i++) {
            // write d1
            String d1 = "d1," + times[i] + ",s1," + values[i] + ",s2," + values[i] + ",s3," + values[i];
            fw.write(d1 + "\r\n");
            String d2 = "d2," + times[i] + ",s1," + values[i] + ",s2," + values[i];
            fw.write(d2 + "\r\n");
        }
        fw.close();
    }

    public static void write() throws IOException {
        write(outputDataFile);
    }

    public static void write(String filePath) throws IOException {
        File file = new File(filePath);
        File errorFile = new File(errorOutputDataFile);
        if (file.exists()) {
            file.delete();
        }
        if (errorFile.exists()) {
            errorFile.delete();
        }

        innerWriter = new TsFileWriter(file, schema, config);

        // write
        try {
            writeToTsFile(schema);
        } catch (WriteProcessException e) {
            LOG.warn("Write to tsfile error", e);
        }
        LOG.info("write to file successfully!!");
    }

    private static void generateTestSchema() {
        schema = new Schema();
        List<IMeasurementSchema> schemaList = new ArrayList<>();
        schemaList.add(
                new MeasurementSchema(
                        "s1", TSDataType.INT64, TSEncoding.valueOf(config.getValueEncoder(TSDataType.INT64))));
        schemaList.add(
                new MeasurementSchema(
                        "s2", TSDataType.INT64, TSEncoding.valueOf(config.getValueEncoder(TSDataType.INT64))));
        schemaList.add(
                new MeasurementSchema(
                        "s3", TSDataType.INT64, TSEncoding.valueOf(config.getValueEncoder(TSDataType.INT64))));
        MeasurementGroup measurementGroup = new MeasurementGroup(false, schemaList);
        schema.registerMeasurementGroup(new Path("d1"), measurementGroup);

        schemaList.clear();
        schemaList.add(
                new MeasurementSchema(
                        "s1", TSDataType.INT64, TSEncoding.valueOf(config.getValueEncoder(TSDataType.INT64))));
        schemaList.add(
                new MeasurementSchema(
                        "s2", TSDataType.INT64, TSEncoding.valueOf(config.getValueEncoder(TSDataType.INT64))));
        schemaList.add(new MeasurementSchema("s4", TSDataType.TEXT, TSEncoding.PLAIN));
        measurementGroup = new MeasurementGroup(false, schemaList);
        schema.registerMeasurementGroup(new Path("d2"), measurementGroup);
    }


    private static void writeToTsFile(Schema schema) throws IOException, WriteProcessException {
        Scanner in = getDataFile(inputDataFile);
        long lineCount = 0;
        long startTime = System.currentTimeMillis();
        long endTime = System.currentTimeMillis();
        assert in != null;
        while (in.hasNextLine()) {
            if (lineCount % 1000000 == 0) {
                LOG.info("write line:{},use time:{}s", lineCount, (endTime - startTime) / 1000);
            }
            String str = in.nextLine();
            TSRecord record = RecordUtils.parseSimpleTupleRecord(str, schema);
            innerWriter.writeRecord(record);
            lineCount++;
        }
        endTime = System.currentTimeMillis();
        LOG.info("write line:{},use time:{}s", lineCount, (endTime - startTime) / 1000);
        innerWriter.close();
        in.close();
    }

    private static Scanner getDataFile(String path) {
        File file = new File(path);
        try {
            return new Scanner(file);
        } catch (FileNotFoundException e) {
            LOG.warn("Get data from file {} error, will return null Scanner", path, e);
            return null;
        }
    }

    public static long[] getTimes(){
        long[] times = new long[ROW_COUNT];
        long[] values = new long[ROW_COUNT];
        prepareData(times, values);
        return times;
    }

    public static long[] getValues(){
        long[] times = new long[ROW_COUNT];
        long[] values = new long[ROW_COUNT];
        prepareData(times, values);
        return values;
    }

    private static void prepareData(long[] times, long[] values) {
        //samsung dataset
        readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/samsung/s-10_cleaned_new.csv", 2, ROW_COUNT+1, 0, times);
        readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/samsung/s-10_cleaned_new.csv", 2, ROW_COUNT+1, 1, values);

        //artificial dataset
        //readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/artificial/exponential/exponential_1_1000_new_new.csv", 2, ROW_NUM+1, 0, times);
        //readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/artificial/exponential/exponential_1_1000_new_new.csv", 2, ROW_NUM+1, 1, values);

        //carnet dataset
        //readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/swzl/swzl_clean.csv", 2, ROW_NUM+1, 0, times);
        //readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/swzl/swzl_clean.csv", 2, ROW_NUM+1, 1, values);

        //shipnet dataset
        //readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/ship/shipNet.csv", 2, ROW_NUM+1, 1, times);
        //readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/ship/shipNet.csv", 2, ROW_NUM+1, 4, values);
    }

    public static boolean readCSV(String filePath, int line_begin, int line_end, int col, long[] data) {
        // Read the CSV file from column `col`
        // starting from line `line_begin` to line `line_end` into the `data` array
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(filePath));
            String line;
            int currentLine = 1;
            int index = 0;

            while ((line = reader.readLine()) != null) {
                if (currentLine >= line_begin && currentLine <= line_end) {
                    String[] tokens = line.split(",");
                    data[index] = (long) Double.parseDouble(tokens[col]);
                    index++;
                }
                currentLine++;
            }
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
