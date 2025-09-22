package org.apache.iotdb.tsfile.utils;

import org.apache.iotdb.tsfile.file.header.PageHeader;

import java.nio.ByteBuffer;

public class CompressedPageData {
  TS_DELTA_data timeData;
  TS_DELTA_data valueData;
  long minTime;
  long maxTime;
  int pageTimeLen;
  int pageValueLen;
  boolean isInSorter;
  int count;

  public CompressedPageData(TS_DELTA_data t, TS_DELTA_data v, int totalNum, long minT, long maxT) {
    this.timeData = t;
    this.valueData = v;
    this.count = totalNum;
    this.minTime = minT;
    this.maxTime = maxT;
    this.isInSorter = true;
    this.pageTimeLen = t.getLen();
    this.pageValueLen = v.getLen();
  }

  public CompressedPageData(PageHeader pageHeader, ByteBuffer timeBuffer, ByteBuffer valueBuffer) {
    this.count = (int) pageHeader.getStatistics().getCount();
    this.minTime = pageHeader.getStatistics().getStartTime();
    this.maxTime = pageHeader.getStatistics().getEndTime();
    this.timeData = getDataFromBuffer(timeBuffer);
    this.valueData = getDataFromBuffer(valueBuffer);
    this.isInSorter = true;
    this.pageTimeLen = this.timeData.getLen();
    this.pageValueLen = this.valueData.getLen();
  }

  private TS_DELTA_data getDataFromBuffer(ByteBuffer buffer) {
    byte[] vals = new byte[buffer.limit()-buffer.position()-4-(count+3)/4];
    byte[] lens = new byte[(count+3)/4];
    buffer.get(vals,0,vals.length);
    buffer.get(lens,0,lens.length);
    return new TS_DELTA_data(vals, lens);
  }

  public boolean getIsInSorter() {
    return this.isInSorter;
  }

  public long getMaxTime() {
    return this.maxTime;
  }

  public long getMinTime() {
    return this.minTime;
  }

  public int getPageTimeLen() {
    return this.pageTimeLen;
  }

  public int getPageValueLen() {
    return this.pageValueLen;
  }

  public int getCount() {
    return this.count;
  }

  public TS_DELTA_data getTimeData() {
    return this.timeData;
  }

  public TS_DELTA_data getValueData() {
    return this.valueData;
  }
}
