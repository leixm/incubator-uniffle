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

import org.apache.uniffle.common.util.ByteBufUtils;

public class RequireBufferRequest extends RequestMessage {
  private int requireSize;
  private String appId;
  private int shuffleId;
  private List<Integer> partitionIds;

  public RequireBufferRequest(long requestId, int requireSize, String appId, int shuffleId,
      List<Integer> partitionIds) {
    super(requestId);
    this.requireSize = requireSize;
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionIds = partitionIds;
  }

  @Override
  public Type type() {
    return Type.REQUIRE_BUFFER_REQUEST;
  }

  @Override
  public int encodedLength() {
    return REQUEST_ID_ENCODE_LENGTH + 2 * Integer.BYTES + ByteBufUtils.encodedLength(appId)
        + Encoders.encodeLengthOfPartitionIds(partitionIds);
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(getRequestId());
    buf.writeInt(requireSize);
    ByteBufUtils.writeLengthAndString(buf, appId);
    buf.writeInt(shuffleId);
    Encoders.encodePartitionIds(partitionIds, buf);
  }

  public static RequireBufferRequest decode(ByteBuf byteBuf) {
    long requestId = byteBuf.readLong();
    int requireSize = byteBuf.readInt();
    String appId = ByteBufUtils.readLengthAndString(byteBuf);
    int shuffleId = byteBuf.readInt();
    List<Integer> partitionIds = Decoders.decodePartitionIds(byteBuf);
    return new RequireBufferRequest(requestId, requireSize, appId, shuffleId, partitionIds);
  }

  public int getRequireSize() {
    return requireSize;
  }

  public String getAppId() {
    return appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public List<Integer> getPartitionIds() {
    return partitionIds;
  }
}
