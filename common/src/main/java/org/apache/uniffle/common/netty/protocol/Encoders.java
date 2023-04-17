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

import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.util.ByteBufUtils;

public class Encoders {
  public static void encodeShuffleServerInfo(ShuffleServerInfo shuffleServerInfo, ByteBuf byteBuf) {
    ByteBufUtils.writeLengthAndString(byteBuf, shuffleServerInfo.getId());
    ByteBufUtils.writeLengthAndString(byteBuf, shuffleServerInfo.getHost());
    byteBuf.writeInt(shuffleServerInfo.getGrpcPort());
    byteBuf.writeInt(shuffleServerInfo.getNettyPort());
  }

  public static void encodeShuffleBlockInfo(ShuffleBlockInfo shuffleBlockInfo, ByteBuf byteBuf) {
    byteBuf.writeInt(shuffleBlockInfo.getPartitionId());
    byteBuf.writeLong(shuffleBlockInfo.getBlockId());
    byteBuf.writeInt(shuffleBlockInfo.getLength());
    byteBuf.writeInt(shuffleBlockInfo.getShuffleId());
    byteBuf.writeLong(shuffleBlockInfo.getCrc());
    byteBuf.writeLong(shuffleBlockInfo.getTaskAttemptId());
    // todo: avoid copy
    ByteBufUtils.copyByteBuf(shuffleBlockInfo.getData(), byteBuf);
    shuffleBlockInfo.getData().release();
    List<ShuffleServerInfo> shuffleServerInfoList = shuffleBlockInfo.getShuffleServerInfos();
    byteBuf.writeInt(shuffleServerInfoList.size());
    for (ShuffleServerInfo shuffleServerInfo : shuffleServerInfoList) {
      Encoders.encodeShuffleServerInfo(shuffleServerInfo, byteBuf);
    }
    byteBuf.writeInt(shuffleBlockInfo.getUncompressLength());
    byteBuf.writeLong(shuffleBlockInfo.getFreeMemory());
  }

  public static int encodeLengthOfShuffleServerInfo(ShuffleServerInfo shuffleServerInfo) {
    return ByteBufUtils.encodedLength(shuffleServerInfo.getId())
        + ByteBufUtils.encodedLength(shuffleServerInfo.getHost())
        + 2 * Integer.BYTES;
  }

  public static int encodeLengthOfShuffleBlockInfo(ShuffleBlockInfo shuffleBlockInfo) {
    int encodeLength = 4 * Long.BYTES + 4 * Integer.BYTES
        + ByteBufUtils.encodedLength(shuffleBlockInfo.getData()) + Integer.BYTES;
    for (ShuffleServerInfo shuffleServerInfo : shuffleBlockInfo.getShuffleServerInfos()) {
      encodeLength += encodeLengthOfShuffleServerInfo(shuffleServerInfo);
    }
    return encodeLength;
  }


  public static void encodePartitionToBlockIds(Map<Integer, List<Long>> partitionToBlockIds, ByteBuf byteBuf) {
    byteBuf.writeInt(partitionToBlockIds.size());
    for (Map.Entry<Integer, List<Long>> entry : partitionToBlockIds.entrySet()) {
      byteBuf.writeInt(entry.getKey());
      List<Long> blocks = entry.getValue();
      byteBuf.writeInt(blocks.size());
      for (long block : blocks) {
        byteBuf.writeLong(block);
      }
    }
  }

  public static int encodeLengthOfPartitionToBlockIds(Map<Integer, List<Long>> partitionToBlockIds) {
    int encodeLength = Integer.BYTES + 2 * Integer.BYTES * partitionToBlockIds.size();
    for (List<Long> blocks : partitionToBlockIds.values()) {
      encodeLength += blocks.size() * Long.BYTES;
    }
    return encodeLength;
  }

  public static void encodePartitionIds(List<Integer> partitions, ByteBuf byteBuf) {
    byteBuf.writeInt(partitions.size());
    for (Integer partitionId : partitions) {
      byteBuf.writeInt(partitionId);
    }
  }

  public static int encodeLengthOfPartitionIds(List<Integer> partitions) {
    return partitions.size() + partitions.size() * Integer.BYTES;
  }

  public static void encodePartitionRanges(List<PartitionRange> partitionRanges, ByteBuf byteBuf) {
    byteBuf.writeInt(partitionRanges.size());
    for (PartitionRange partitionRange : partitionRanges) {
      byteBuf.writeInt(partitionRange.getStart());
      byteBuf.writeInt(partitionRange.getEnd());
    }
  }

  public static int encodeLengthOfPartitionRanges(List<PartitionRange> partitionRanges) {
    return Integer.BYTES + 2 * Integer.BYTES * partitionRanges.size();
  }

  public static void encodeRemoteStorageInfo(RemoteStorageInfo remoteStorageInfo, ByteBuf byteBuf) {
    ByteBufUtils.writeLengthAndString(byteBuf, remoteStorageInfo.getPath());
    Map<String, String> confItems = remoteStorageInfo.getConfItems();
    byteBuf.writeInt(confItems.size());
    for (Map.Entry<String, String> entry : confItems.entrySet()) {
      ByteBufUtils.writeLengthAndString(byteBuf, entry.getKey());
      ByteBufUtils.writeLengthAndString(byteBuf, entry.getValue());
    }
  }

  public static int encodeLengthOfRemoteStorageInfo(RemoteStorageInfo remoteStorageInfo) {
    int encodeLength = ByteBufUtils.encodedLength(remoteStorageInfo.getPath()) + Integer.BYTES;
    for (Map.Entry<String, String> entry : remoteStorageInfo.getConfItems().entrySet()) {
      encodeLength += ByteBufUtils.encodedLength(entry.getKey()) + ByteBufUtils.encodedLength(entry.getValue());
    }
    return encodeLength;
  }

  public static void encodeBufferSegments(List<BufferSegment> bufferSegments, ByteBuf byteBuf) {
    byteBuf.writeInt(bufferSegments.size());
    for (BufferSegment bufferSegment : bufferSegments) {
      byteBuf.writeLong(bufferSegment.getBlockId());
      byteBuf.writeInt(bufferSegment.getOffset());
      byteBuf.writeInt(bufferSegment.getLength());
      byteBuf.writeInt(bufferSegment.getUncompressLength());
      byteBuf.writeLong(bufferSegment.getCrc());
      byteBuf.writeLong(bufferSegment.getTaskAttemptId());
    }
  }

  public static int encodeLengthOfBufferSegments(List<BufferSegment> bufferSegments) {
    return Integer.BYTES + bufferSegments.size() * (3 * Long.BYTES + 3 * Integer.BYTES);
  }

}
