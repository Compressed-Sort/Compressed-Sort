package org.apache.iotdb.db.storageengine.dataregion.compaction.inner;

import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.JsonFormatConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.MeasurementGroup;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.BooleanDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.DoubleDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.StringDataPoint;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class TsFilegenerator {
    private static final Logger LOG = LoggerFactory.getLogger(TsFilegenerator.class);
    public static String outputDataFile = "D:\\senior\\DQ\\research\\compressed_sort_paper\\code\\vldb25\\tsfile_segment_data\\output.tsfile";
    public static Schema schema;
    private static int ROW_COUNT = 1000;
    private static TsFileWriter innerWriter;
    private static String inputDataFile;
    private static String errorOutputDataFile;
    private static final TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
    private static long beginTime;
    private static long endTime;


    public static TsFileResource generateFile(int beginIndex, int rowCount, int maxNumberOfPointsInPage, String fileName) throws IOException {
        ROW_COUNT = rowCount;
        outputDataFile = fileName;
        int oldMaxNumberOfPointsInPage = config.getMaxNumberOfPointsInPage();
        config.setMaxNumberOfPointsInPage(maxNumberOfPointsInPage);

        prepare(beginIndex);
        write();
        config.setMaxNumberOfPointsInPage(oldMaxNumberOfPointsInPage);
        TsFileResource resource = new TsFileResource(new File(outputDataFile));
        resource.updateStartTime("d1", beginTime);
        resource.updateEndTime("d1", endTime);
        return resource;
    }

    public static void prepare(int beginIndex) throws IOException {
        File file = new File(outputDataFile);
        if (!file.getParentFile().exists()) {
            Assert.assertTrue(file.getParentFile().mkdirs());
        }
        inputDataFile = "D:\\senior\\DQ\\research\\compressed_sort_paper\\code\\vldb25\\tsfile_segment_data\\1.tsfile";
        file = new File(inputDataFile);
        if (!file.getParentFile().exists()) {
            Assert.assertTrue(file.getParentFile().mkdirs());
        }
        errorOutputDataFile = "D:\\senior\\DQ\\research\\compressed_sort_paper\\code\\vldb25\\tsfile_segment_data\\2.tsfile";
        file = new File(errorOutputDataFile);
        if (!file.getParentFile().exists()) {
            Assert.assertTrue(file.getParentFile().mkdirs());
        }
        generateTestSchema();
        generateSampleInputDataFile(beginIndex);
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

    private static void generateSampleInputDataFile(int beginIndex) throws IOException {
        long[] times = new long[ROW_COUNT];
        long[] values = new long[ROW_COUNT];
        prepareData(beginIndex, times, values);
        sortTimeWithValue(times, values);
        beginTime = times[0];
        endTime = times[ROW_COUNT - 1];
        File file = new File(inputDataFile);
        if (file.exists()) {
            file.delete();
        }
        file.getParentFile().mkdirs();
        FileWriter fw = new FileWriter(file);

        for (int i = 0; i < ROW_COUNT; i++) {
            // write d1
            String d1 = "d1," + times[i] + ",s1," + values[i];
            fw.write(d1 + "\r\n");
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
        MeasurementSchema measurementSchema = new MeasurementSchema("s1", TSDataType.INT64);
        List<MeasurementSchema> schemaList = new ArrayList<>();
        schemaList.add(measurementSchema);
        MeasurementGroup measurementGroup = new MeasurementGroup(false, schemaList);
        schema.registerMeasurementGroup(new Path("d1"), measurementGroup);
        //schema.registerTimeseries(new Path("d1"), measurementSchema);
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
            TSRecord record = parseSimpleTupleRecord(str, schema);
            innerWriter.write(record);
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


    private static void prepareData(int beginIndex, long[] times, long[] values) {
        //samsung dataset
        readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/samsung/s-10_cleaned_new.csv", beginIndex + 2, beginIndex + ROW_COUNT+1, 0, times);
        readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/samsung/s-10_cleaned_new.csv", beginIndex + 2, beginIndex + ROW_COUNT+1, 1, values);

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

    public static TSRecord parseSimpleTupleRecord(String str, Schema schema) {
        // split items
        String[] items = str.split(JsonFormatConstant.TSRECORD_SEPARATOR);
        // get deviceId and timestamp, then create a new TSRecord
        String deviceId = items[0].trim();
        long timestamp;
        try {
            timestamp = Long.parseLong(items[1].trim());
        } catch (NumberFormatException e) {
            LOG.warn("given timestamp is illegal:{}", str);
            // return a TSRecord without any data points
            return new TSRecord(-1, deviceId);
        }
        TSRecord ret = new TSRecord(timestamp, deviceId);

        // loop all rest items except the last one
        String measurementId;
        TSDataType type;
        for (int i = 2; i < items.length - 1; i += 2) {
            // get measurementId and value
            measurementId = items[i].trim();
            MeasurementGroup measurementGroup = schema.getSeriesSchema(new Path(deviceId));
            IMeasurementSchema measurementSchema =
                    measurementGroup == null
                            ? null
                            : measurementGroup.getMeasurementSchemaMap().get(measurementId);
            if (measurementSchema == null) {
                LOG.warn("measurementId:{},type not found, pass", measurementId);
                continue;
            }
            type = measurementSchema.getType();
            String value = items[i + 1].trim();
            // if value is not null, wrap it with corresponding DataPoint and add to
            // TSRecord
            if (!"".equals(value)) {
                try {
                    switch (type) {
                        case INT32:
                            ret.addTuple(new IntDataPoint(measurementId, Integer.parseInt(value)));
                            break;
                        case INT64:
                            ret.addTuple(new LongDataPoint(measurementId, Long.parseLong(value)));
                            break;
                        case FLOAT:
                            ret.addTuple(new FloatDataPoint(measurementId, Float.parseFloat(value)));
                            break;
                        case DOUBLE:
                            ret.addTuple(new DoubleDataPoint(measurementId, Double.parseDouble(value)));
                            break;
                        case BOOLEAN:
                            ret.addTuple(new BooleanDataPoint(measurementId, Boolean.parseBoolean(value)));
                            break;
                        case TEXT:
                            ret.addTuple(new StringDataPoint(measurementId, BytesUtils.valueOf(items[i + 1])));
                            break;
                        default:
                            LOG.warn("unsupported data type:{}", type);
                            break;
                    }
                } catch (NumberFormatException e) {
                    LOG.warn("parsing measurement meets error, omit it", e);
                }
            }
        }
        return ret;
    }

    public static void sortTimeWithValue(long[] times, long[] values) {
        // 将数据列和时间列根据时间列的大小进行排序
        long[][] sorted = new long[times.length][2];
        for (int i = 0; i < times.length; i++) {
            sorted[i][0] = times[i];
            sorted[i][1] = values[i];
        }
        Arrays.sort(sorted, (a, b) -> Long.compare(a[0], b[0]));
        for (int i = 0; i < times.length; i++) {
            times[i] = sorted[i][0];
            values[i] = sorted[i][1];
        }
    }
}
