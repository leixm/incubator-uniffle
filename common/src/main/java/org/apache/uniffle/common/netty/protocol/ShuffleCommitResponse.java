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

import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.ByteBufUtils;

public class ShuffleCommitResponse extends RpcResponse {
  private int commitCount;

  public ShuffleCommitResponse(long requestId, StatusCode statusCode, int commitCount) {
    this(requestId, statusCode, null, commitCount);
  }

  public ShuffleCommitResponse(long requestId, StatusCode statusCode, String retMessage, int commitCount) {
    super(requestId, statusCode, retMessage);
    this.commitCount = commitCount;
  }

  @Override
  public int encodedLength() {
    return super.encodedLength() + Integer.BYTES;
  }

  @Override
  public void encode(ByteBuf buf) {
    super.encode(buf);
    buf.writeInt(commitCount);
  }

  public static ShuffleCommitResponse decode(ByteBuf byteBuf) {
    long requestId = byteBuf.readLong();
    StatusCode statusCode = StatusCode.fromCode(byteBuf.readInt());
    String retMessage = ByteBufUtils.readLengthAndString(byteBuf);
    int commitCount = byteBuf.readInt();
    return new ShuffleCommitResponse(requestId, statusCode, retMessage, commitCount);
  }

  @Override
  public Type type() {
    return Type.SHUFFLE_COMMIT_RESPONSE;
  }

  public int getCommitCount() {
    return commitCount;
  }
}
