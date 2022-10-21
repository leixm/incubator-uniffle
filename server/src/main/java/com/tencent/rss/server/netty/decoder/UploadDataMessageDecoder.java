/*
 * Tencent is pleased to support the open source community by making
 * Firestorm-Spark remote shuffle server available.
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.tencent.rss.server.netty.decoder;

import java.nio.ByteBuffer;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.rss.common.util.MessageConstants;
import com.tencent.rss.server.netty.message.UploadDataMessage;
import com.tencent.rss.server.netty.util.NettyUtils;

public class UploadDataMessageDecoder extends ByteToMessageDecoder {

  private static final Logger LOG = LoggerFactory.getLogger(UploadDataMessageDecoder.class);

  private State state = State.READ_MAGIC_BYTE;
  private int requiredBytes = 0;
  private int partitionId;
  private long blockId;
  private long crc;
  private int uncompressLength;
  private int dataLength;
  private ByteBuffer dataBuffer = null;
  private UploadDataMessage uploadDataMessage = new UploadDataMessage();

  public UploadDataMessageDecoder() {
    super();
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    super.channelInactive(ctx);
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
    if (in.readableBytes() == 0) {
      return;
    }

    switch (state) {
      case READ_MAGIC_BYTE:
        if (in.readableBytes() < Byte.BYTES) {
          return;
        }
        byte magicByte = in.readByte();
        switch (magicByte) {
          case 1:
            // start to process data upload
            state = State.READ_TASK_APPID_LEN;
            return;
          default:
            String clientInfo = NettyUtils.getServerConnectionInfo(ctx);
            LOG.warn(
                "Invalid magic byte {} from client {}",
                magicByte, clientInfo);
            ctx.close();
            LOG.debug("Closed connection to client {}", clientInfo);
            return;
        }
      case READ_TASK_APPID_LEN:
        if (in.readableBytes() < Integer.BYTES) {
          return;
        }
        // read length of appId
        requiredBytes = in.readInt();
        if (requiredBytes < 0) {
          String clientInfo = NettyUtils.getServerConnectionInfo(ctx);
          LOG.warn(
              "Invalid length of applicationId {} from client {}",
              requiredBytes, clientInfo);
          ctx.close();
          LOG.debug("Closed connection to client {}", clientInfo);
          return;
        }
        state = State.READ_TASK_UPLOAD_INFO;
        return;
      case READ_TASK_UPLOAD_INFO:
        // appId + shuffleId + requireBufferId
        if (in.readableBytes() < requiredBytes + Integer.BYTES + Long.BYTES) {
          return;
        }
        uploadDataMessage.readMessageInfo(in, requiredBytes);
        state = State.READ_PARTITION_ID;
        return;
      case READ_PARTITION_ID:
        if (in.readableBytes() < Integer.BYTES) {
          return;
        }
        int tmpPartitionId = in.readInt();
        // check if there has no more data
        if (tmpPartitionId == MessageConstants.MESSAGE_UPLOAD_DATA_END) {
          // add message to process
          out.add(uploadDataMessage);
        } else {
          partitionId = tmpPartitionId;
          dataBuffer = null;
          state = State.READ_BLOCK_INFO_START;
        }
        return;
      case READ_BLOCK_INFO_START:
        if (in.readableBytes() < Byte.BYTES) {
          return;
        }
        byte statusFlg = in.readByte();
        // check if there has no more data for current partition
        if (statusFlg == MessageConstants.MESSAGE_UPLOAD_DATA_PARTITION_END) {
          state = State.READ_PARTITION_ID;
        } else if (statusFlg == MessageConstants.MESSAGE_UPLOAD_DATA_PARTITION_CONTINUE) {
          state = State.READ_BLOCK_INFO;
        } else {
          throw new RuntimeException("Unexpected flag[" + statusFlg + "] in READ_BLOCK_INFO_START status");
        }
        // there has no data when come here for new partition
        if (dataBuffer != null) {
          // add block to message
          uploadDataMessage.addBlockData(partitionId, blockId, crc, uncompressLength, dataLength, dataBuffer);
        }
        return;
      case READ_BLOCK_INFO:
        // blockId + crc + uncompressLength + length
        if (in.readableBytes() < Long.BYTES + Long.BYTES + Integer.BYTES + Integer.BYTES) {
          return;
        }
        blockId = in.readLong();
        crc = in.readLong();
        uncompressLength = in.readInt();
        dataLength = in.readInt();

        if (dataLength == 0) {
          throw new RuntimeException("Unexpected 0 length for data block[" + blockId + "]");
        } else {
          // create ByteBuffer for new data block
          dataBuffer = ByteBuffer.allocateDirect(dataLength);
          requiredBytes = dataLength;
          state = State.READ_BLOCK_DATA;
        }
        return;
      case READ_BLOCK_DATA:
        // read data to ByteBuffer
        if (in.readableBytes() < requiredBytes) {
          int count = in.readableBytes();
          while (in.readableBytes() > 0) {
            dataBuffer.put(in.readByte());
          }
          requiredBytes -= count;
        } else {
          in.readBytes(dataBuffer);
          requiredBytes = 0;
          state = State.READ_BLOCK_INFO_START;
        }
        return;
      default:
        throw new RuntimeException(String.format(
            "Should not get incoming data in state %s, client %s",
            state, NettyUtils.getServerConnectionInfo(ctx)));
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    String connectionInfo = NettyUtils.getServerConnectionInfo(ctx);
    String msg = "Got exception " + connectionInfo;
    LOG.warn(msg, cause);

    ctx.close();
  }

  private enum State {
    READ_MAGIC_BYTE,
    READ_TASK_APPID_LEN,
    READ_TASK_UPLOAD_INFO,
    READ_PARTITION_ID,
    READ_BLOCK_INFO_START,
    READ_BLOCK_INFO,
    READ_BLOCK_DATA
  }
}
