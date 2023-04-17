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

import io.netty.buffer.ByteBuf;

import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.util.ByteBufUtils;

public class ShuffleRegisterRequest extends RequestMessage {
  private String appId;
  private int shuffleId;
  private List<PartitionRange> partitionRanges;
  private RemoteStorageInfo remoteStorageInfo;
  private String user;
  private ShuffleDataDistributionType shuffleDataDistribution;

  public ShuffleRegisterRequest(long requestId, String appId, int shuffleId, List<PartitionRange> partitionRanges,
      RemoteStorageInfo remoteStorageInfo, String user, ShuffleDataDistributionType shuffleDataDistribution) {
    super(requestId);
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionRanges = partitionRanges;
    this.remoteStorageInfo = remoteStorageInfo;
    this.user = user;
    this.shuffleDataDistribution = shuffleDataDistribution;
  }

  @Override
  public Type type() {
    return Type.SHUFFLE_REGISTER_REQUEST;
  }

  @Override
  public int encodedLength() {
    return REQUEST_ID_ENCODE_LENGTH + ByteBufUtils.encodedLength(appId) + Integer.BYTES
        + Encoders.encodeLengthOfPartitionRanges(partitionRanges)
        + Encoders.encodeLengthOfRemoteStorageInfo(remoteStorageInfo)
        + ByteBufUtils.encodedLength(user) + Byte.BYTES;
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(getRequestId());
    ByteBufUtils.writeLengthAndString(buf, appId);
    buf.writeInt(shuffleId);
    Encoders.encodePartitionRanges(partitionRanges, buf);
    Encoders.encodeRemoteStorageInfo(remoteStorageInfo, buf);
    ByteBufUtils.writeLengthAndString(buf, user);
    shuffleDataDistribution.encode(buf);
  }

  public static ShuffleRegisterRequest decode(ByteBuf byteBuf) {
    long requestId = byteBuf.readLong();
    String appId = ByteBufUtils.readLengthAndString(byteBuf);
    int shuffleId = byteBuf.readInt();
    List<PartitionRange> partitionRanges = Decoders.decodePartitionRanges(byteBuf);
    RemoteStorageInfo remoteStorageInfo = Decoders.decodeRemoteStorageInfo(byteBuf);
    String user = ByteBufUtils.readLengthAndString(byteBuf);
    ShuffleDataDistributionType shuffleDataDistributionType = ShuffleDataDistributionType.decode(byteBuf);
    return new ShuffleRegisterRequest(requestId, appId, shuffleId, partitionRanges, remoteStorageInfo, user,
        shuffleDataDistributionType);
  }

  public String getAppId() {
    return appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public List<PartitionRange> getPartitionRanges() {
    return partitionRanges;
  }

  public RemoteStorageInfo getRemoteStorageInfo() {
    return remoteStorageInfo;
  }

  public String getUser() {
    return user;
  }

  public ShuffleDataDistributionType getShuffleDataDistribution() {
    return shuffleDataDistribution;
  }
}
