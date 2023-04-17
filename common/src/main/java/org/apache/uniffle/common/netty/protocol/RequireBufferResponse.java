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

public class RequireBufferResponse extends RpcResponse {
  private long requireBufferId;

  public RequireBufferResponse(long requestId, StatusCode statusCode, long requireBufferId) {
    this(requestId, statusCode, null, requireBufferId);
  }

  public RequireBufferResponse(long requestId, StatusCode statusCode, String retMessage, long requireBufferId) {
    super(requestId, statusCode, retMessage);
    this.requireBufferId = requireBufferId;
  }

  @Override
  public int encodedLength() {
    return super.encodedLength() + Long.BYTES;
  }

  @Override
  public void encode(ByteBuf buf) {
    super.encode(buf);
    buf.writeLong(requireBufferId);
  }

  public static RequireBufferResponse decode(ByteBuf byteBuf) {
    long requestId = byteBuf.readLong();
    StatusCode statusCode = StatusCode.fromCode(byteBuf.readInt());
    String retMessage = ByteBufUtils.readLengthAndString(byteBuf);
    long requireId = byteBuf.readLong();
    return new RequireBufferResponse(requestId, statusCode, retMessage, requireId);
  }

  @Override
  public Type type() {
    return Type.REQUIRE_BUFFER_RESPONSE;
  }

  public long getRequireBufferId() {
    return requireBufferId;
  }
}
