/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.uniffle.common.netty.protocol;

import java.util.List;
import java.util.Map;

import io.netty.buffer.ByteBuf;

import org.apache.uniffle.common.util.ByteBufUtils;

public class ReportShuffleResultRequest extends RequestMessage {
  private String appId;
  private int shuffleId;
  private long taskAttemptId;
  private int bitmapNum;
  private Map<Integer, List<Long>> partitionToBlockIds;

  public ReportShuffleResultRequest(long requestId, String appId, int shuffleId, long taskAttemptId,
      int bitmapNum, Map<Integer, List<Long>> partitionToBlockIds) {
    super(requestId);
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.taskAttemptId = taskAttemptId;
    this.bitmapNum = bitmapNum;
    this.partitionToBlockIds = partitionToBlockIds;
  }

  @Override
  public Type type() {
    return Type.REPORT_SHUFFLE_RESULT_REQUEST;
  }

  @Override
  public int encodedLength() {
    return REQUEST_ID_ENCODE_LENGTH + ByteBufUtils.encodedLength(appId) + Integer.BYTES + Long.BYTES
        + Integer.BYTES + Encoders.encodeLengthOfPartitionToBlockIds(partitionToBlockIds);
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(getRequestId());
    ByteBufUtils.writeLengthAndString(buf, appId);
    buf.writeInt(shuffleId);
    buf.writeLong(taskAttemptId);
    buf.writeInt(bitmapNum);
    Encoders.encodePartitionToBlockIds(partitionToBlockIds, buf);
  }

  public static ReportShuffleResultRequest decode(ByteBuf byteBuf) {
    long requestId = byteBuf.readLong();
    String appId = ByteBufUtils.readLengthAndString(byteBuf);
    int shuffleId = byteBuf.readInt();
    long taskAttemptId = byteBuf.readLong();
    int bitmapNum = byteBuf.readInt();
    Map<Integer, List<Long>> partitionToBlockIds = Decoders.decodePartitionToBlockIds(byteBuf);
    return new ReportShuffleResultRequest(requestId, appId, shuffleId, taskAttemptId, bitmapNum,
        partitionToBlockIds);
  }

  public String getAppId() {
    return appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public long getTaskAttemptId() {
    return taskAttemptId;
  }

  public int getBitmapNum() {
    return bitmapNum;
  }

  public Map<Integer, List<Long>> getPartitionToBlockIds() {
    return partitionToBlockIds;
  }
}
