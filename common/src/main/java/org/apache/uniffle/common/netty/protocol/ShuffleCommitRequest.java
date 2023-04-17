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

import io.netty.buffer.ByteBuf;

import org.apache.uniffle.common.util.ByteBufUtils;

public class ShuffleCommitRequest extends RequestMessage {
  private String appId;
  private int shuffleId;

  public ShuffleCommitRequest(long requestId, String appId, int shuffleId) {
    super(requestId);
    this.appId = appId;
    this.shuffleId = shuffleId;
  }

  @Override
  public Type type() {
    return Type.SHUFFLE_COMMIT_REQUEST;
  }

  @Override
  public int encodedLength() {
    return REQUEST_ID_ENCODE_LENGTH + ByteBufUtils.encodedLength(appId) + Integer.BYTES;
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(getRequestId());
    ByteBufUtils.writeLengthAndString(buf, appId);
    buf.writeInt(shuffleId);
  }

  public static ShuffleCommitRequest decode(ByteBuf byteBuf) {
    long requestId = byteBuf.readLong();
    String appId = ByteBufUtils.readLengthAndString(byteBuf);
    int shuffleId = byteBuf.readInt();
    return new ShuffleCommitRequest(requestId, appId, shuffleId);
  }

  public String getAppId() {
    return appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }
}
