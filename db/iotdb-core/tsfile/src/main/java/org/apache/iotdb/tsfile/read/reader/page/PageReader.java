/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.tsfile.read.reader.page;

import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.BatchDataFactory;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.read.reader.series.PaginationController;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.iotdb.tsfile.read.reader.series.PaginationController.UNLIMITED_PAGINATION_CONTROLLER;
import static org.apache.iotdb.tsfile.utils.Preconditions.checkArgument;

public class PageReader implements IPageReader {

  private final PageHeader pageHeader;

  private final TSDataType dataType;

  /** decoder for value column */
  private final Decoder valueDecoder;

  /** decoder for time column */
  private final Decoder timeDecoder;

  /** time column in memory */
  private ByteBuffer timeBuffer;

  /** value column in memory */
  private ByteBuffer valueBuffer;

  private Filter recordFilter;
  private PaginationController paginationController = UNLIMITED_PAGINATION_CONTROLLER;

  /** A list of deleted intervals. */
  private List<TimeRange> deleteIntervalList;

  private int deleteCursor = 0;

  public PageReader(
      ByteBuffer pageData, TSDataType dataType, Decoder valueDecoder, Decoder timeDecoder) {
    this(null, pageData, dataType, valueDecoder, timeDecoder, null);
  }

  public PageReader(
      PageHeader pageHeader,
      ByteBuffer pageData,
      TSDataType dataType,
      Decoder valueDecoder,
      Decoder timeDecoder) {
    this(pageHeader, pageData, dataType, valueDecoder, timeDecoder, null);
  }

  public PageReader(
      PageHeader pageHeader,
      ByteBuffer pageData,
      TSDataType dataType,
      Decoder valueDecoder,
      Decoder timeDecoder,
      Filter recordFilter) {
    this.dataType = dataType;
    this.valueDecoder = valueDecoder;
    this.timeDecoder = timeDecoder;
    this.recordFilter = recordFilter;
    this.pageHeader = pageHeader;
    splitDataToTimeStampAndValue(pageData);
  }

  /**
   * split pageContent into two stream: time and value
   *
   * @param pageData uncompressed bytes size of time column, time column, value column
   */
  private void splitDataToTimeStampAndValue(ByteBuffer pageData) {
    // todo!将长的压缩序列划分到时间列和数值列
    int timeBufferLength = ReadWriteForEncodingUtils.readUnsignedVarInt(pageData);

    timeBuffer = pageData.slice();
    timeBuffer.limit(timeBufferLength);

    valueBuffer = pageData.slice();
    valueBuffer.position(timeBufferLength);
  }

  /** @return the returned BatchData may be empty, but never be null */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Override
  public BatchData getAllSatisfiedPageData(boolean ascending) throws IOException {
    BatchData pageData = BatchDataFactory.createBatchData(dataType, ascending, false);
    boolean allSatisfy = recordFilter == null || recordFilter.allSatisfy(this);
    while (timeDecoder.hasNext(timeBuffer)) {
      long timestamp = timeDecoder.readLong(timeBuffer);
      switch (dataType) {
        case BOOLEAN:
          boolean aBoolean = valueDecoder.readBoolean(valueBuffer);
          if (!isDeleted(timestamp) && (allSatisfy || recordFilter.satisfy(timestamp, aBoolean))) {
            pageData.putBoolean(timestamp, aBoolean);
          }
          break;
        case INT32:
          int anInt = valueDecoder.readInt(valueBuffer);
          if (!isDeleted(timestamp) && (allSatisfy || recordFilter.satisfy(timestamp, anInt))) {
            pageData.putInt(timestamp, anInt);
          }
          break;
        case INT64:
          long aLong = valueDecoder.readLong(valueBuffer);
          if (!isDeleted(timestamp) && (allSatisfy || recordFilter.satisfy(timestamp, aLong))) {
            pageData.putLong(timestamp, aLong);
          }
          break;
        case FLOAT:
          float aFloat = valueDecoder.readFloat(valueBuffer);
          if (!isDeleted(timestamp) && (allSatisfy || recordFilter.satisfy(timestamp, aFloat))) {
            pageData.putFloat(timestamp, aFloat);
          }
          break;
        case DOUBLE:
          double aDouble = valueDecoder.readDouble(valueBuffer);
          if (!isDeleted(timestamp) && (allSatisfy || recordFilter.satisfy(timestamp, aDouble))) {
            pageData.putDouble(timestamp, aDouble);
          }
          break;
        case TEXT:
          Binary aBinary = valueDecoder.readBinary(valueBuffer);
          if (!isDeleted(timestamp) && (allSatisfy || recordFilter.satisfy(timestamp, aBinary))) {
            pageData.putBinary(timestamp, aBinary);
          }
          break;
        default:
          throw new UnSupportedDataTypeException(String.valueOf(dataType));
      }
    }
    return pageData.flip();
  }

  // todo:在这里对编码的数据解压
  @Override
  public TsBlock getAllSatisfiedData() throws IOException {
    TsBlockBuilder builder;
    int initialExpectedEntries = (int) pageHeader.getStatistics().getCount();
    if (paginationController.hasCurLimit()) {
      initialExpectedEntries =
          (int) Math.min(initialExpectedEntries, paginationController.getCurLimit());
    }
    builder = new TsBlockBuilder(initialExpectedEntries, Collections.singletonList(dataType));

    TimeColumnBuilder timeBuilder = builder.getTimeColumnBuilder();
    ColumnBuilder valueBuilder = builder.getColumnBuilder(0);
    boolean allSatisfy = recordFilter == null || recordFilter.allSatisfy(this);
    switch (dataType) {
      case BOOLEAN:
        while (timeDecoder.hasNext(timeBuffer)) {
          long timestamp = timeDecoder.readLong(timeBuffer);
          boolean aBoolean = valueDecoder.readBoolean(valueBuffer);
          if (isDeleted(timestamp) || (!allSatisfy && !recordFilter.satisfy(timestamp, aBoolean))) {
            continue;
          }
          if (paginationController.hasCurOffset()) {
            paginationController.consumeOffset();
            continue;
          }
          if (paginationController.hasCurLimit()) {
            timeBuilder.writeLong(timestamp);
            valueBuilder.writeBoolean(aBoolean);
            builder.declarePosition();
            paginationController.consumeLimit();
          } else {
            break;
          }
        }
        break;
      case INT32:
        while (timeDecoder.hasNext(timeBuffer)) {
          long timestamp = timeDecoder.readLong(timeBuffer);
          int anInt = valueDecoder.readInt(valueBuffer);
          if (isDeleted(timestamp) || (!allSatisfy && !recordFilter.satisfy(timestamp, anInt))) {
            continue;
          }
          if (paginationController.hasCurOffset()) {
            paginationController.consumeOffset();
            continue;
          }
          if (paginationController.hasCurLimit()) {
            timeBuilder.writeLong(timestamp);
            valueBuilder.writeInt(anInt);
            builder.declarePosition();
            paginationController.consumeLimit();
          } else {
            break;
          }
        }
        break;
      case INT64:
        while (timeDecoder.hasNext(timeBuffer)) {
          long timestamp = timeDecoder.readLong(timeBuffer);
          long aLong = valueDecoder.readLong(valueBuffer);
          if (isDeleted(timestamp) || (!allSatisfy && !recordFilter.satisfy(timestamp, aLong))) {
            continue;
          }
          if (paginationController.hasCurOffset()) {
            paginationController.consumeOffset();
            continue;
          }
          if (paginationController.hasCurLimit()) {
            timeBuilder.writeLong(timestamp);
            valueBuilder.writeLong(aLong);
            builder.declarePosition();
            paginationController.consumeLimit();
          } else {
            break;
          }
        }
        break;
      case FLOAT:
        while (timeDecoder.hasNext(timeBuffer)) {
          long timestamp = timeDecoder.readLong(timeBuffer);
          float aFloat = valueDecoder.readFloat(valueBuffer);
          if (isDeleted(timestamp) || (!allSatisfy && !recordFilter.satisfy(timestamp, aFloat))) {
            continue;
          }
          if (paginationController.hasCurOffset()) {
            paginationController.consumeOffset();
            continue;
          }
          if (paginationController.hasCurLimit()) {
            timeBuilder.writeLong(timestamp);
            valueBuilder.writeFloat(aFloat);
            builder.declarePosition();
            paginationController.consumeLimit();
          } else {
            break;
          }
        }
        break;
      case DOUBLE:
        while (timeDecoder.hasNext(timeBuffer)) {
          long timestamp = timeDecoder.readLong(timeBuffer);
          double aDouble = valueDecoder.readDouble(valueBuffer);
          if (isDeleted(timestamp) || (!allSatisfy && !recordFilter.satisfy(timestamp, aDouble))) {
            continue;
          }
          if (paginationController.hasCurOffset()) {
            paginationController.consumeOffset();
            continue;
          }
          if (paginationController.hasCurLimit()) {
            timeBuilder.writeLong(timestamp);
            valueBuilder.writeDouble(aDouble);
            builder.declarePosition();
            paginationController.consumeLimit();
          } else {
            break;
          }
        }
        break;
      case TEXT:
        while (timeDecoder.hasNext(timeBuffer)) {
          long timestamp = timeDecoder.readLong(timeBuffer);
          Binary aBinary = valueDecoder.readBinary(valueBuffer);
          if (isDeleted(timestamp) || (!allSatisfy && !recordFilter.satisfy(timestamp, aBinary))) {
            continue;
          }
          if (paginationController.hasCurOffset()) {
            paginationController.consumeOffset();
            continue;
          }
          if (paginationController.hasCurLimit()) {
            timeBuilder.writeLong(timestamp);
            valueBuilder.writeBinary(aBinary);
            builder.declarePosition();
            paginationController.consumeLimit();
          } else {
            break;
          }
        }
        break;
      default:
        throw new UnSupportedDataTypeException(String.valueOf(dataType));
    }
    return builder.build();
  }

  @Override
  public Statistics<? extends Serializable> getStatistics() {
    return pageHeader.getStatistics();
  }

  @Override
  public Statistics<? extends Serializable> getTimeStatistics() {
    return getStatistics();
  }

  @Override
  public Optional<Statistics<? extends Serializable>> getMeasurementStatistics(
      int measurementIndex) {
    checkArgument(
        measurementIndex == 0,
        "Non-aligned page only has one measurement, but measurementIndex is " + measurementIndex);
    return Optional.ofNullable(getStatistics());
  }

  @Override
  public boolean hasNullValue(int measurementIndex) {
    return false;
  }

  @Override
  public void addRecordFilter(Filter filter) {
    this.recordFilter = FilterFactory.and(recordFilter, filter);
  }

  @Override
  public void setLimitOffset(PaginationController paginationController) {
    this.paginationController = paginationController;
  }

  public void setDeleteIntervalList(List<TimeRange> list) {
    this.deleteIntervalList = list;
  }

  public List<TimeRange> getDeleteIntervalList() {
    return deleteIntervalList;
  }

  @Override
  public boolean isModified() {
    return pageHeader.isModified();
  }

  @Override
  public void initTsBlockBuilder(List<TSDataType> dataTypes) {
    // do nothing
  }

  protected boolean isDeleted(long timestamp) {
    while (deleteIntervalList != null && deleteCursor < deleteIntervalList.size()) {
      if (deleteIntervalList.get(deleteCursor).contains(timestamp)) {
        return true;
      } else if (deleteIntervalList.get(deleteCursor).getMax() < timestamp) {
        deleteCursor++;
      } else {
        return false;
      }
    }
    return false;
  }
}
