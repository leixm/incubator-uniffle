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

package com.tencent.rss.client.impl.grpc;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.rss.client.request.RssGetInMemoryShuffleDataRequest;
import com.tencent.rss.client.request.RssGetShuffleDataRequest;
import com.tencent.rss.client.response.ResponseStatusCode;
import com.tencent.rss.client.response.RssGetInMemoryShuffleDataResponse;
import com.tencent.rss.client.response.RssGetShuffleDataResponse;
import com.tencent.rss.common.BufferSegment;
import com.tencent.rss.common.util.MessageConstants;

public class ShuffleServerGrpcNettyClient extends ShuffleServerGrpcClient {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleServerGrpcNettyClient.class);

  protected String connectionInfo = "";
  private int nettyPort;

  public ShuffleServerGrpcNettyClient(String host, int grpcPort, int nettyPort) {
    super(host, grpcPort);
    this.nettyPort = nettyPort;
  }

  private void closeSocket(Socket socket, InputStream inputStream, OutputStream outputStream) {
    if (socket == null) {
      return;
    }

    try {
      if (outputStream != null) {
        outputStream.flush();
      }
    } catch (Throwable e) {
      LOG.warn("Hit exception when flushing output stream: " + connectionInfo, e);
    }

    try {
      if (outputStream != null) {
        outputStream.close();
      }
    } catch (Throwable e) {
      LOG.warn("Hit exception when closing output stream: " + connectionInfo, e);
    }

    try {
      if (inputStream != null) {
        inputStream.close();
      }
    } catch (Throwable e) {
      LOG.warn("Hit exception when closing input stream: " + connectionInfo, e);
    }

    try {
      socket.close();
    } catch (Throwable e) {
      LOG.warn("Hit exception when closing socket: " + connectionInfo, e);
    }

    socket = null;
  }

  @Override
  public RssGetShuffleDataResponse getShuffleData(RssGetShuffleDataRequest request) {
    Socket socket = null;
    InputStream inputStream = null;
    OutputStream outputStream = null;
    try {
      long start = System.currentTimeMillis();
      LOG.debug(String.format("Connecting to server: %s", connectionInfo));
      // connect shuffle server
      socket = connectSocket(10000);
      inputStream = socket.getInputStream();
      outputStream = socket.getOutputStream();
      // write flag for fetch shuffle data
      write(outputStream, new byte[]{MessageConstants.GET_LOCAL_SHUFFLE_DATA_MAGIC_BYTE});
      String appId = request.getAppId();
      byte[] appIdBytes = appId.getBytes();
      ByteBuf bufAppId = Unpooled.buffer(Integer.BYTES + appIdBytes.length);
      // write length of appId and appId
      bufAppId.writeInt(appId.length());
      bufAppId.writeBytes(appIdBytes);
      write(outputStream, bufAppId.array());
      bufAppId.release();
      // buf for shuffleId + partitionId + partitionNumPerRange + PartitionNum + ReadBufferSize
      ByteBuf bufInt = Unpooled.buffer(6 * Integer.BYTES + Long.BYTES);
      bufInt.writeInt(request.getShuffleId());
      bufInt.writeInt(request.getPartitionId());
      bufInt.writeInt(request.getPartitionNumPerRange());
      bufInt.writeInt(request.getPartitionNum());
      bufInt.writeInt(request.getLength());
      bufInt.writeLong(request.getOffset());
      write(outputStream, bufInt.array());
      bufInt.release();

      RssGetShuffleDataResponse response = null;
      if (readStatus(inputStream) == MessageConstants.RESPONSE_STATUS_OK) {
        byte[] shuffleData = readBytes(inputStream, request.getLength());
        response = new RssGetShuffleDataResponse(ResponseStatusCode.SUCCESS, shuffleData);
      } else {
        throw new RuntimeException("Can't get shuffle data with " + connectionInfo
            + " for appId[" + appId + "], shuffleId[" + request.getShuffleId() + "], partitionId["
            + request.getPartitionId() + "]");
      }
      return response;
    } catch (Exception e) {
      throw new RuntimeException("Error happen when get shuffle data from shuffle server", e);
    } finally {
      closeSocket(socket, inputStream, outputStream);
    }
  }

  @Override
  public RssGetInMemoryShuffleDataResponse getInMemoryShuffleData(
      RssGetInMemoryShuffleDataRequest request) {
    Socket socket = null;
    InputStream inputStream = null;
    OutputStream outputStream = null;
    try {
      long start = System.currentTimeMillis();
      LOG.debug(String.format("Connecting to server: %s", connectionInfo));
      // connect shuffle server
      socket = connectSocket(10000);
      inputStream = socket.getInputStream();
      outputStream = socket.getOutputStream();
      // write flag for fetch shuffle data
      write(outputStream, new byte[]{MessageConstants.GET_IN_MEMORY_SHUFFLE_DATA_MAGIC_BYTE});
      String appId = request.getAppId();
      byte[] appIdBytes = appId.getBytes();
      ByteBuf bufAppId = Unpooled.buffer(Integer.BYTES + appIdBytes.length);
      // write length of appId and appId
      bufAppId.writeInt(appId.length());
      bufAppId.writeBytes(appIdBytes);
      write(outputStream, bufAppId.array());
      bufAppId.release();
      // buf for shuffleId + partitionId + partitionNumPerRange + PartitionNum + ReadBufferSize
      ByteBuf bufInt = Unpooled.buffer(3 * Integer.BYTES + Long.BYTES);
      bufInt.writeInt(request.getShuffleId());
      bufInt.writeInt(request.getPartitionId());
      bufInt.writeInt(request.getReadBufferSize());
      bufInt.writeLong(request.getLastBlockId());
      write(outputStream, bufInt.array());
      bufInt.release();

      RssGetInMemoryShuffleDataResponse response = null;
      if (readStatus(inputStream) == MessageConstants.RESPONSE_STATUS_OK) {
        List<BufferSegment> bufferSegments = Lists.newArrayList();
        int dataLength = readBufferSegments(inputStream, bufferSegments);
        byte[] shuffleData = readBytes(inputStream, dataLength);
        response = new RssGetInMemoryShuffleDataResponse(
            ResponseStatusCode.SUCCESS, shuffleData, bufferSegments);
        LOG.info("Successfully read shuffle data from memory with {} bytes", dataLength);
      } else {
        throw new RuntimeException("Can't get shuffle data with " + connectionInfo
            + " for appId[" + appId + "], shuffleId[" + request.getShuffleId() + "], partitionId["
            + request.getPartitionId() + "]");
      }
      return response;
    } catch (Exception e) {
      throw new RuntimeException("Error happen when get shuffle data from shuffle server", e);
    } finally {
      closeSocket(socket, inputStream, outputStream);
    }
  }

  //  @Override
  //  public RssSendShuffleDataResponse sendShuffleData(RssSendShuffleDataRequest request) {
  //    Socket socket = null;
  //    InputStream inputStream = null;
  //    OutputStream outputStream = null;
  //    try {
  //      long start = System.currentTimeMillis();
  //      LOG.debug(String.format("Connecting to server: %s", connectionInfo));
  //      // connect shuffle server
  //      socket = connectSocket(request.getSendDataTimeout());
  //      inputStream = socket.getInputStream();
  //      outputStream = socket.getOutputStream();
  //      // write flag for upload shuffle data
  //      write(outputStream, new byte[]{MessageConstants.UPLOAD_MAGIC_BYTE});
  //      String appId = request.getAppId();
  //      byte[] appIdBytes = appId.getBytes();
  //      ByteBuf bufAppId = Unpooled.buffer(Integer.BYTES + appIdBytes.length);
  //      // write length of appId and appId
  //      bufAppId.writeInt(appId.length());
  //      bufAppId.writeBytes(appIdBytes);
  //      write(outputStream, bufAppId.array());
  //      bufAppId.release();
  //      Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleIdToBlocks = request.getShuffleIdToBlocks();
  //      boolean isSuccessful = true;
  //      // buf for shuffleId + requireBufferId
  //      ByteBuf bufShuffleInfo = Unpooled.buffer(Integer.BYTES + Long.BYTES);
  //      // prepare rpc request based on shuffleId -> partitionId -> blocks
  //      for (Map.Entry<Integer, Map<Integer, List<ShuffleBlockInfo>>> stb : shuffleIdToBlocks.entrySet()) {
  //        int shuffleId = stb.getKey();
  //        int size = 0;
  //        int blockNum = 0;
  //        for (Map.Entry<Integer, List<ShuffleBlockInfo>> ptb : stb.getValue().entrySet()) {
  //          for (ShuffleBlockInfo sbi : ptb.getValue()) {
  //            size += sbi.getLength();
  //            blockNum++;
  //          }
  //        }
  //
  //        long requireId = requirePreAllocation(size, request.getRetryMax(), request.getRetryIntervalMax());
  //        if (requireId != FAILED_REQUIRE_ID) {
  //          // write shuffleId, requireId
  //          bufShuffleInfo.writeInt(shuffleId);
  //          bufShuffleInfo.writeLong(requireId);
  //          write(outputStream, bufShuffleInfo.array());
  //          bufShuffleInfo.resetReaderIndex();
  //          bufShuffleInfo.resetWriterIndex();
  //          isSuccessful = uploadData(outputStream, inputStream, stb.getValue());
  //          LOG.info("Do sendShuffleData with netty cost:" + (System.currentTimeMillis() - start)
  //              + " ms for " + size + " bytes with " + blockNum + " blocks");
  //        } else {
  //          isSuccessful = false;
  //          break;
  //        }
  //      }
  //      bufShuffleInfo.release();
  //
  //      RssSendShuffleDataResponse response;
  //      if (isSuccessful) {
  //        response = new RssSendShuffleDataResponse(ResponseStatusCode.SUCCESS);
  //      } else {
  //        response = new RssSendShuffleDataResponse(ResponseStatusCode.INTERNAL_ERROR);
  //      }
  //      return response;
  //    } catch (Exception e) {
  //      throw new RuntimeException("Error happen when send data to shuffle server", e);
  //    } finally {
  //      closeSocket(socket, inputStream, outputStream);
  //    }
  //  }

  //  private boolean uploadData(
  //      OutputStream outputStream,
  //      InputStream inputStream,
  //      Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks) {
  //    // buf for int data
  //    ByteBuf bufInt = Unpooled.buffer(Integer.BYTES);
  //    // buf for blockId + crc + uncompressLength + length
  //    ByteBuf bufBlockInfo = Unpooled.buffer(Long.BYTES + Long.BYTES + Integer.BYTES + Integer.BYTES);
  //    for (Map.Entry<Integer, List<ShuffleBlockInfo>> entry : partitionToBlocks.entrySet()) {
  //      // write partitionId
  //      bufInt.writeInt(entry.getKey());
  //      write(outputStream, bufInt.array());
  //      bufInt.resetReaderIndex();
  //      bufInt.resetWriterIndex();
  //      for (ShuffleBlockInfo sbi : entry.getValue()) {
  //        // write flag which means new block is coming
  //        write(outputStream, new byte[]{MessageConstants.MESSAGE_UPLOAD_DATA_PARTITION_CONTINUE});
  //        // write blockInfo include blockId, crc, uncompressLength, length
  //        bufBlockInfo.writeLong(sbi.getBlockId());
  //        bufBlockInfo.writeLong(sbi.getCrc());
  //        bufBlockInfo.writeInt(sbi.getUncompressLength());
  //        bufBlockInfo.writeInt(sbi.getLength());
  //        write(outputStream, bufBlockInfo.array());
  //        bufBlockInfo.resetWriterIndex();
  //        bufBlockInfo.resetReaderIndex();
  //        // write block data
  //        write(outputStream, sbi.getData());
  //      }
  //      // write flag which means no block for current partitionId
  //      write(outputStream, new byte[]{MessageConstants.MESSAGE_UPLOAD_DATA_PARTITION_END});
  //    }
  //    // write flag which means no block for current shuffleId
  //    bufInt.writeInt(MessageConstants.MESSAGE_UPLOAD_DATA_END);
  //    write(outputStream, bufInt.array());
  //    bufInt.resetReaderIndex();
  //    bufInt.resetWriterIndex();
  //    bufInt.release();
  //    bufBlockInfo.release();
  //    return readStatus(inputStream) == MessageConstants.RESPONSE_STATUS_OK;
  //  }

  private int readBufferSegments(InputStream inputStream, List<BufferSegment> segments) {
    int dataLength = 0;
    int segmentNum = readInt(inputStream);
    for (int i = 0; i < segmentNum; i++) {
      long blockId = readLong(inputStream);
      long offset = readLong(inputStream);
      int length = readInt(inputStream);
      int uncompressLength = readInt(inputStream);
      long crc = readLong(inputStream);
      long taskAttemptId = readLong(inputStream);
      segments.add(new BufferSegment(
          blockId, offset, length, uncompressLength, crc, taskAttemptId));
      dataLength += length;
    }
    return dataLength;
  }

  protected Socket connectSocket(int timeout) {
    Socket socket = null;
    long startTime = System.currentTimeMillis();
    int triedTimes = 0;
    try {
      Throwable lastException = null;
      while (System.currentTimeMillis() - startTime <= timeout) {
        if (triedTimes >= 1) {
          LOG.info(String
              .format("Retrying connect to %s:%s, total retrying times: %s, elapsed milliseconds: %s", host,
                  nettyPort,
                  triedTimes, System.currentTimeMillis() - startTime));
        }
        triedTimes++;
        try {
          socket = new Socket();
          socket.setSoTimeout(timeout);
          socket.setTcpNoDelay(true);
          socket.connect(new InetSocketAddress(host, nettyPort), timeout);
          break;
        } catch (Exception socketException) {
          socket = null;
          lastException = socketException;
          LOG.warn(String.format("Failed to connect to %s:%s", host, nettyPort), socketException);
          Thread.sleep(500);
        }
      }

      if (socket == null) {
        if (lastException != null) {
          throw lastException;
        } else {
          throw new IOException(String.format("Failed to connect to %s:%s", host, nettyPort));
        }
      }

      connectionInfo = String.format("[%s -> %s (%s)]",
          socket.getLocalSocketAddress(),
          socket.getRemoteSocketAddress(),
          host);
    } catch (Throwable e) {
      long elapsedTime = System.currentTimeMillis() - startTime;
      String msg = String
          .format("connectSocket failed after trying %s times for %s milliseconds (timeout set to %s): %s",
              triedTimes, elapsedTime, timeout, connectionInfo);
      LOG.error(msg, e);
      throw new RuntimeException(msg, e);
    }
    return socket;
  }

  protected void write(OutputStream outputStream, byte[] bytes) {
    try {
      outputStream.write(bytes);
    } catch (IOException e) {
      String logMsg = String.format("write failed: %s", connectionInfo);
      LOG.error(logMsg, e);
      throw new RuntimeException(logMsg, e);
    }
  }

  protected int readStatus(InputStream inputStream) {
    try {
      return inputStream.read();
    } catch (IOException e) {
      String logMsg = String.format("read status failed: %s", connectionInfo);
      LOG.error(logMsg, e);
      throw new RuntimeException(logMsg, e);
    }
  }

  private int readInt(InputStream stream) {
    byte[] bytes = readBytes(stream, Integer.BYTES);
    ByteBuf buf = Unpooled.wrappedBuffer(bytes);
    try {
      int value = buf.readInt();
      return value;
    } finally {
      buf.release();
    }
  }

  private long readLong(InputStream stream) {
    byte[] bytes = readBytes(stream, Long.BYTES);
    ByteBuf buf = Unpooled.wrappedBuffer(bytes);
    try {
      long value = buf.readLong();
      return value;
    } finally {
      buf.release();
    }
  }

  private byte[] readBytes(InputStream stream, int numBytes) {
    if (numBytes == 0) {
      return new byte[0];
    }

    byte[] result = new byte[numBytes];
    int readBytes = 0;
    while (readBytes < numBytes) {
      try {
        int numBytesToRead = numBytes - readBytes;
        int count = stream.read(result, readBytes, numBytesToRead);

        if (count == -1) {
          throw new RuntimeException(
              "Failed to read data bytes due to end of stream: "
                  + numBytesToRead);
        }

        readBytes += count;
      } catch (IOException e) {
        throw new RuntimeException("Failed to read data", e);
      }
    }

    return result;
  }
}
