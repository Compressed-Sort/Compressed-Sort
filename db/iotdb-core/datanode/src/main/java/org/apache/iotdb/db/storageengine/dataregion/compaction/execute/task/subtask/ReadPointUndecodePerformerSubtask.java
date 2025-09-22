package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadPointCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadPointUndecodeCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.reader.IDataBlockReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.writer.AbstractCompactionWriter;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.tsfile.utils.CompressedPageData;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class ReadPointUndecodePerformerSubtask implements Callable<Void> {
    @SuppressWarnings("squid:1068")
    private static final Logger logger =
            LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);

    private final String device;
    private final List<String> measurementList;
    private final FragmentInstanceContext fragmentInstanceContext;
    private final QueryDataSource queryDataSource;
    private final AbstractCompactionWriter compactionWriter;
    private final Map<String, MeasurementSchema> schemaMap;
    private final int taskId;

  public ReadPointUndecodePerformerSubtask(
            String device,
            List < String > measurementList,
            FragmentInstanceContext fragmentInstanceContext,
            QueryDataSource queryDataSource,
            AbstractCompactionWriter compactionWriter,
            Map < String, MeasurementSchema > schemaMap,
    int taskId){
        this.device = device;
        this.measurementList = measurementList;
        this.fragmentInstanceContext = fragmentInstanceContext;
        this.queryDataSource = queryDataSource;
        this.compactionWriter = compactionWriter;
        this.schemaMap = schemaMap;
        this.taskId = taskId;
    }

    @Override
    public Void call () throws Exception {
        for (String measurement : measurementList) {
            List<IMeasurementSchema> measurementSchemas =
                    Collections.singletonList(schemaMap.get(measurement));
            IDataBlockReader dataBlockReader =
                    ReadPointUndecodeCompactionPerformer.constructReader(
                            device,
                            Collections.singletonList(measurement),
                            measurementSchemas,
                            new ArrayList<>(schemaMap.keySet()),
                            fragmentInstanceContext,
                            queryDataSource,
                            false);

            if (dataBlockReader.hasNextUndecodedBatch()) {
                compactionWriter.startMeasurement(measurementSchemas, taskId);
                LinkedList<CompressedPageData> pageData = dataBlockReader.nextUndecodedBatches();
                ReadPointUndecodeCompactionPerformer.writeWithPages(
                        compactionWriter, pageData, device, taskId, false);
                compactionWriter.endMeasurement(taskId);
            }
        }
        return null;
    }
}