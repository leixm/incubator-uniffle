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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.common.rpc.StatusCode;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NettyProtocolTest {
  @Test
  public void testSendShuffleDataRequest() {
    String appId = "test_app";
    byte[] data = new byte[]{1, 2, 3};
    List<ShuffleServerInfo> shuffleServerInfoList = Arrays.asList(new ShuffleServerInfo("aaa", 1),
        new ShuffleServerInfo("bbb", 2));
    List<ShuffleBlockInfo> shuffleBlockInfoList1 =
        Arrays.asList(new ShuffleBlockInfo(1, 1, 1, 10, 123,
                Unpooled.wrappedBuffer(data).retain(), shuffleServerInfoList, 5, 0, 1),
            new ShuffleBlockInfo(1, 1, 1, 10, 123,
                Unpooled.wrappedBuffer(data).retain(), shuffleServerInfoList, 5, 0, 1));
    List<ShuffleBlockInfo> shuffleBlockInfoList2 =
        Arrays.asList(new ShuffleBlockInfo(1, 2, 1, 10, 123,
                Unpooled.wrappedBuffer(data).retain(), shuffleServerInfoList, 5, 0, 1),
            new ShuffleBlockInfo(1, 1, 2, 10, 123,
                Unpooled.wrappedBuffer(data).retain(), shuffleServerInfoList, 5, 0, 1));
    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(1, shuffleBlockInfoList1);
    partitionToBlocks.put(2, shuffleBlockInfoList2);
    SendShuffleDataRequest sendShuffleDataRequest =
        new SendShuffleDataRequest(1L, appId, 1, 1, partitionToBlocks, 12345);
    int encodeLength = sendShuffleDataRequest.encodedLength();

    ByteBuf byteBuf = Unpooled.buffer(sendShuffleDataRequest.encodedLength());
    sendShuffleDataRequest.encode(byteBuf);
    assertEquals(byteBuf.readableBytes(), encodeLength);
    SendShuffleDataRequest sendShuffleDataRequest1 = sendShuffleDataRequest.decode(byteBuf);
    assertTrue(NettyProtocolTestUtils.compareSendShuffleDataRequest(sendShuffleDataRequest, sendShuffleDataRequest1));
    assertEquals(encodeLength, sendShuffleDataRequest1.encodedLength());
    byteBuf.release();
    for (ShuffleBlockInfo shuffleBlockInfo : sendShuffleDataRequest1.getPartitionToBlocks().get(1)) {
      shuffleBlockInfo.getData().release();
    }
    for (ShuffleBlockInfo shuffleBlockInfo : sendShuffleDataRequest1.getPartitionToBlocks().get(2)) {
      shuffleBlockInfo.getData().release();
    }
    assertEquals(0, byteBuf.refCnt());
  }

  @Test
  public void testRpcResponse() {
    RpcResponse rpcResponse = new RpcResponse(1, StatusCode.SUCCESS, "test_message");
    int encodeLength = rpcResponse.encodedLength();
    ByteBuf byteBuf = Unpooled.buffer(encodeLength);
    rpcResponse.encode(byteBuf);
    assertEquals(byteBuf.readableBytes(), encodeLength);
    RpcResponse rpcResponse1 = RpcResponse.decode(byteBuf);
    assertTrue(rpcResponse.equals(rpcResponse1));
    assertEquals(rpcResponse.encodedLength(), rpcResponse1.encodedLength());
    byteBuf.release();
  }

  @Test
  public void testAppHeartBeatRequest() {
    AppHeartBeatRequest appHeartBeatRequest = new AppHeartBeatRequest(1, "test_app");
    int encodeLength = appHeartBeatRequest.encodedLength();
    ByteBuf byteBuf = Unpooled.buffer(encodeLength, encodeLength);
    appHeartBeatRequest.encode(byteBuf);
    AppHeartBeatRequest appHeartBeatRequest1 = AppHeartBeatRequest.decode(byteBuf);

    assertEquals(appHeartBeatRequest.getRequestId(), appHeartBeatRequest1.getRequestId());
    assertEquals(appHeartBeatRequest.getAppId(), appHeartBeatRequest1.getAppId());
  }

  @Test
  public void testFinishShuffleRequest() {
    FinishShuffleRequest finishShuffleRequest = new FinishShuffleRequest(1, "test_app", 1);
    int encodeLength = finishShuffleRequest.encodedLength();
    ByteBuf byteBuf = Unpooled.buffer(encodeLength, encodeLength);
    finishShuffleRequest.encode(byteBuf);
    FinishShuffleRequest finishShuffleRequest1 = FinishShuffleRequest.decode(byteBuf);

    assertEquals(finishShuffleRequest.getRequestId(), finishShuffleRequest1.getRequestId());
    assertEquals(finishShuffleRequest.getAppId(), finishShuffleRequest1.getAppId());
    assertEquals(finishShuffleRequest.getShuffleId(), finishShuffleRequest1.getShuffleId());
  }

  @Test
  public void GetLocalShuffleDataRequest() {
    GetLocalShuffleDataRequest getLocalShuffleDataRequest = new GetLocalShuffleDataRequest(1, "test_app",
        1, 1, 1, 100, 0, 200, System.currentTimeMillis());
    int encodeLength = getLocalShuffleDataRequest.encodedLength();
    ByteBuf byteBuf = Unpooled.buffer(encodeLength, encodeLength);
    getLocalShuffleDataRequest.encode(byteBuf);
    GetLocalShuffleDataRequest getLocalShuffleDataRequest1 = GetLocalShuffleDataRequest.decode(byteBuf);

    assertEquals(getLocalShuffleDataRequest.getRequestId(), getLocalShuffleDataRequest1.getRequestId());
    assertEquals(getLocalShuffleDataRequest.getAppId(), getLocalShuffleDataRequest1.getAppId());
    assertEquals(getLocalShuffleDataRequest.getShuffleId(), getLocalShuffleDataRequest1.getShuffleId());
    assertEquals(getLocalShuffleDataRequest.getPartitionId(), getLocalShuffleDataRequest1.getPartitionId());
    assertEquals(getLocalShuffleDataRequest.getPartitionNumPerRange(),
        getLocalShuffleDataRequest1.getPartitionNumPerRange());
    assertEquals(getLocalShuffleDataRequest.getPartitionNum(), getLocalShuffleDataRequest1.getPartitionNum());
    assertEquals(getLocalShuffleDataRequest.getOffset(), getLocalShuffleDataRequest1.getOffset());
    assertEquals(getLocalShuffleDataRequest.getLength(), getLocalShuffleDataRequest1.getLength());
    assertEquals(getLocalShuffleDataRequest.getTimestamp(), getLocalShuffleDataRequest1.getTimestamp());
  }

  @Test
  public void testGetLocalShuffleDataResponse() {
    byte[] data = new byte[]{1, 2, 3};
    GetLocalShuffleDataResponse getLocalShuffleDataResponse =
        new GetLocalShuffleDataResponse(1, StatusCode.SUCCESS, data);
    int encodeLength = getLocalShuffleDataResponse.encodedLength();
    ByteBuf byteBuf = Unpooled.buffer(encodeLength, encodeLength);
    getLocalShuffleDataResponse.encode(byteBuf);
    GetLocalShuffleDataResponse getLocalShuffleDataResponse1 = GetLocalShuffleDataResponse.decode(byteBuf);

    assertEquals(getLocalShuffleDataResponse.getRequestId(), getLocalShuffleDataResponse1.getRequestId());
    assertEquals(getLocalShuffleDataResponse.getRetMessage(), getLocalShuffleDataResponse1.getRetMessage());
    assertEquals(getLocalShuffleDataResponse.getStatusCode(), getLocalShuffleDataResponse1.getStatusCode());
    assertEquals(getLocalShuffleDataResponse.getData(), getLocalShuffleDataResponse1.getData());
  }

  @Test
  public void testGetLocalShuffleIndexRequest() {
    GetLocalShuffleIndexRequest getLocalShuffleIndexRequest =
        new GetLocalShuffleIndexRequest(1, "test_app", 1,
            1, 1, 100);
    int encodeLength = getLocalShuffleIndexRequest.encodedLength();
    ByteBuf byteBuf = Unpooled.buffer(encodeLength, encodeLength);
    getLocalShuffleIndexRequest.encode(byteBuf);
    GetLocalShuffleIndexRequest getLocalShuffleIndexRequest1 = GetLocalShuffleIndexRequest.decode(byteBuf);

    assertEquals(getLocalShuffleIndexRequest.getRequestId(), getLocalShuffleIndexRequest1.getRequestId());
    assertEquals(getLocalShuffleIndexRequest.getAppId(), getLocalShuffleIndexRequest1.getAppId());
    assertEquals(getLocalShuffleIndexRequest.getShuffleId(), getLocalShuffleIndexRequest1.getShuffleId());
    assertEquals(getLocalShuffleIndexRequest.getPartitionId(), getLocalShuffleIndexRequest1.getPartitionId());
    assertEquals(getLocalShuffleIndexRequest.getPartitionNumPerRange(),
        getLocalShuffleIndexRequest1.getPartitionNumPerRange());
    assertEquals(getLocalShuffleIndexRequest.getPartitionNum(), getLocalShuffleIndexRequest1.getPartitionNum());
  }

  @Test
  public void testGetLocalShuffleIndexResponse() {
    byte[] indexData = new byte[]{1, 2, 3};
    GetLocalShuffleIndexResponse getLocalShuffleIndexResponse =
        new GetLocalShuffleIndexResponse(1, StatusCode.SUCCESS, "", indexData, 23);
    int encodeLength = getLocalShuffleIndexResponse.encodedLength();
    ByteBuf byteBuf = Unpooled.buffer(encodeLength, encodeLength);
    getLocalShuffleIndexResponse.encode(byteBuf);
    GetLocalShuffleIndexResponse getLocalShuffleIndexResponse1 = GetLocalShuffleIndexResponse.decode(byteBuf);

    assertEquals(getLocalShuffleIndexResponse.getRequestId(), getLocalShuffleIndexResponse1.getRequestId());
    assertEquals(getLocalShuffleIndexResponse.getStatusCode(), getLocalShuffleIndexResponse1.getStatusCode());
    assertEquals(getLocalShuffleIndexResponse.getRetMessage(), getLocalShuffleIndexResponse1.getRetMessage());
    assertEquals(getLocalShuffleIndexResponse.getFileLength(), getLocalShuffleIndexResponse1.getFileLength());
    assertEquals(getLocalShuffleIndexResponse.getIndexData(), getLocalShuffleIndexResponse1.getIndexData());
  }

  @Test
  public void testGetMemoryShuffleDataRequest() {
    Roaring64NavigableMap expectedTaskIdsBitmap = Roaring64NavigableMap.bitmapOf(1, 2, 3, 4, 5);
    GetMemoryShuffleDataRequest getMemoryShuffleDataRequest = new GetMemoryShuffleDataRequest(1, "test_app",
        1, 1, 1, 64, System.currentTimeMillis(), expectedTaskIdsBitmap);
    int encodeLength = getMemoryShuffleDataRequest.encodedLength();
    ByteBuf byteBuf = Unpooled.buffer(encodeLength, encodeLength);
    getMemoryShuffleDataRequest.encode(byteBuf);
    GetMemoryShuffleDataRequest getMemoryShuffleDataRequest1 = GetMemoryShuffleDataRequest.decode(byteBuf);

    assertEquals(getMemoryShuffleDataRequest.getRequestId(), getMemoryShuffleDataRequest1.getRequestId());
    assertEquals(getMemoryShuffleDataRequest.getAppId(), getMemoryShuffleDataRequest1.getAppId());
    assertEquals(getMemoryShuffleDataRequest.getShuffleId(), getMemoryShuffleDataRequest1.getShuffleId());
    assertEquals(getMemoryShuffleDataRequest.getPartitionId(), getMemoryShuffleDataRequest1.getPartitionId());
    assertEquals(getMemoryShuffleDataRequest.getLastBlockId(), getMemoryShuffleDataRequest1.getLastBlockId());
    assertEquals(getMemoryShuffleDataRequest.getReadBufferSize(), getMemoryShuffleDataRequest1.getReadBufferSize());
    assertEquals(getMemoryShuffleDataRequest.getTimestamp(), getMemoryShuffleDataRequest1.getTimestamp());
    assertEquals(getMemoryShuffleDataRequest.getExpectedTaskIdsBitmap().getLongCardinality(),
        getMemoryShuffleDataRequest1.getExpectedTaskIdsBitmap().getLongCardinality());
  }

  @Test
  public void testGetMemoryShuffleDataResponse() {
    byte[] data = new byte[]{1, 2, 3, 4, 5};
    List<BufferSegment> bufferSegments = Lists.newArrayList(
        new BufferSegment(1, 0, 5, 10, 123, 1),
        new BufferSegment(1, 0, 5, 10, 345, 1));
    GetMemoryShuffleDataResponse getMemoryShuffleDataResponse =
        new GetMemoryShuffleDataResponse(1, StatusCode.SUCCESS, "", bufferSegments, data);
    int encodeLength = getMemoryShuffleDataResponse.encodedLength();
    ByteBuf byteBuf = Unpooled.buffer(encodeLength, encodeLength);
    getMemoryShuffleDataResponse.encode(byteBuf);
    GetMemoryShuffleDataResponse getMemoryShuffleDataResponse1 = GetMemoryShuffleDataResponse.decode(byteBuf);

    assertEquals(getMemoryShuffleDataResponse.getRequestId(), getMemoryShuffleDataResponse1.getRequestId());
    assertEquals(getMemoryShuffleDataResponse.getStatusCode(), getMemoryShuffleDataResponse1.getStatusCode());
    assertTrue(getMemoryShuffleDataResponse.getData().equals(getMemoryShuffleDataResponse1.getData()));

    for (int i = 0; i < 2; i++) {
      assertEquals(getMemoryShuffleDataResponse.getBufferSegments().get(i),
          getMemoryShuffleDataResponse1.getBufferSegments().get(i));
    }
  }

  @Test
  public void testGetShuffleResultRequest() {
    GetShuffleResultRequest getShuffleResultRequest = new GetShuffleResultRequest(1, "test_app", 1, 1);
    int encodeLength = getShuffleResultRequest.encodedLength();
    ByteBuf byteBuf = Unpooled.buffer(encodeLength, encodeLength);
    getShuffleResultRequest.encode(byteBuf);
    GetShuffleResultRequest getShuffleResultRequest1 = GetShuffleResultRequest.decode(byteBuf);

    assertEquals(getShuffleResultRequest.getRequestId(), getShuffleResultRequest1.getRequestId());
    assertEquals(getShuffleResultRequest.getAppId(), getShuffleResultRequest1.getAppId());
    assertEquals(getShuffleResultRequest.getShuffleId(), getShuffleResultRequest1.getShuffleId());
    assertEquals(getShuffleResultRequest.getPartitionId(), getShuffleResultRequest1.getPartitionId());
  }

  @Test
  public void testGetShuffleResultResponse() throws IOException {
    Roaring64NavigableMap bitmap = Roaring64NavigableMap.bitmapOf(1, 3, 6, 9);
    GetShuffleResultResponse getShuffleResultResponse =
        new GetShuffleResultResponse(1, StatusCode.SUCCESS, "retMessage", RssUtils.serializeBitMap(bitmap));
    int encodeLength = getShuffleResultResponse.encodedLength();
    ByteBuf byteBuf = Unpooled.buffer(encodeLength, encodeLength);
    getShuffleResultResponse.encode(byteBuf);
    GetShuffleResultResponse getShuffleResultResponse1 = GetShuffleResultResponse.decode(byteBuf);

    assertEquals(getShuffleResultResponse.getRequestId(), getShuffleResultResponse1.getRequestId());
    assertEquals(getShuffleResultResponse.getRetMessage(), getShuffleResultResponse1.getRetMessage());
    assertEquals(getShuffleResultResponse.getStatusCode(), getShuffleResultResponse1.getStatusCode());
    assertArrayEquals(getShuffleResultResponse.getSerializedBitmap(),
        getShuffleResultResponse1.getSerializedBitmap());
  }

  @Test
  public void testReportShuffleResultRequest() {
    Map<Integer, List<Long>> partitionToBlockIds = Maps.newHashMap();
    partitionToBlockIds.put(1, Lists.newArrayList(1L, 2L, 3L));
    partitionToBlockIds.put(2, Lists.newArrayList(4L, 5L, 6L));
    ReportShuffleResultRequest reportShuffleResultRequest =
        new ReportShuffleResultRequest(1, "test_app", 1, 1, 2, partitionToBlockIds);
    int encodeLength = reportShuffleResultRequest.encodedLength();
    ByteBuf byteBuf = Unpooled.buffer(encodeLength, encodeLength);
    reportShuffleResultRequest.encode(byteBuf);
    ReportShuffleResultRequest reportShuffleResultRequest1 = ReportShuffleResultRequest.decode(byteBuf);

    assertEquals(reportShuffleResultRequest.getRequestId(), reportShuffleResultRequest1.getRequestId());
    assertEquals(reportShuffleResultRequest.getAppId(), reportShuffleResultRequest1.getAppId());
    assertEquals(reportShuffleResultRequest.getBitmapNum(), reportShuffleResultRequest1.getBitmapNum());
    assertEquals(reportShuffleResultRequest.getShuffleId(), reportShuffleResultRequest1.getShuffleId());
    assertEquals(reportShuffleResultRequest.getTaskAttemptId(), reportShuffleResultRequest1.getTaskAttemptId());
    assertEquals(reportShuffleResultRequest.getPartitionToBlockIds(), reportShuffleResultRequest1.getPartitionToBlockIds());
  }

  @Test
  public void testRequireBufferRequest() {
    List<Integer> partitionIds = Lists.newArrayList(1, 2, 3, 4);
    RequireBufferRequest requireBufferRequest = new RequireBufferRequest(1, 10, "test_app", 1, partitionIds);
    int encodeLength = requireBufferRequest.encodedLength();
    ByteBuf byteBuf = Unpooled.buffer(encodeLength, encodeLength);
    requireBufferRequest.encode(byteBuf);
    RequireBufferRequest requireBufferRequest1 = RequireBufferRequest.decode(byteBuf);

    assertEquals(requireBufferRequest.getRequestId(), requireBufferRequest1.getRequestId());
    assertEquals(requireBufferRequest.getAppId(), requireBufferRequest1.getAppId());
    assertEquals(requireBufferRequest.getShuffleId(), requireBufferRequest1.getShuffleId());
    assertEquals(requireBufferRequest.getRequireSize(), requireBufferRequest1.getRequireSize());
    assertEquals(requireBufferRequest.getPartitionIds(), requireBufferRequest1.getPartitionIds());
  }

  @Test
  public void testRequireBufferResponse() {
    RequireBufferResponse requireBufferResponse = new RequireBufferResponse(1, StatusCode.SUCCESS, 1213);
    int encodeLength = requireBufferResponse.encodedLength();
    ByteBuf byteBuf = Unpooled.buffer(encodeLength, encodeLength);
    requireBufferResponse.encode(byteBuf);
    RequireBufferResponse requireBufferResponse1 = RequireBufferResponse.decode(byteBuf);

    assertEquals(requireBufferResponse.getRequestId(), requireBufferResponse1.getRequestId());
    assertEquals(requireBufferResponse.getStatusCode(), requireBufferResponse1.getStatusCode());
    assertEquals(requireBufferResponse.getRequireBufferId(), requireBufferResponse1.getRequireBufferId());
  }

  @Test
  public void testShuffleCommitRequest() {
    ShuffleCommitRequest shuffleCommitRequest = new ShuffleCommitRequest(1, "test_app", 1);
    int encodeLength = shuffleCommitRequest.encodedLength();
    ByteBuf byteBuf = Unpooled.buffer(encodeLength, encodeLength);
    shuffleCommitRequest.encode(byteBuf);
    ShuffleCommitRequest shuffleCommitRequest1 = ShuffleCommitRequest.decode(byteBuf);

    assertEquals(shuffleCommitRequest.getRequestId(), shuffleCommitRequest1.getRequestId());
    assertEquals(shuffleCommitRequest.getAppId(), shuffleCommitRequest1.getAppId());
    assertEquals(shuffleCommitRequest.getShuffleId(), shuffleCommitRequest1.getShuffleId());
  }

  @Test
  public void testShuffleCommitResponse() {
    ShuffleCommitResponse shuffleCommitResponse = new ShuffleCommitResponse(1, StatusCode.SUCCESS, 10);
    int encodeLength = shuffleCommitResponse.encodedLength();
    ByteBuf byteBuf = Unpooled.buffer(encodeLength, encodeLength);
    shuffleCommitResponse.encode(byteBuf);
    ShuffleCommitResponse shuffleCommitResponse1 = ShuffleCommitResponse.decode(byteBuf);
    assertEquals(shuffleCommitResponse.getRequestId(), shuffleCommitResponse1.getRequestId());
    assertEquals(shuffleCommitResponse.getStatusCode(), shuffleCommitResponse1.getStatusCode());
    assertEquals(shuffleCommitResponse.getCommitCount(), shuffleCommitResponse1.getCommitCount());
  }

  @Test
  public void testShuffleRegisterRequest() {
    List<PartitionRange> partitionRanges = Lists.newArrayList(new PartitionRange(1, 2), new PartitionRange(3, 4));
    Map<String, String> confItems = Maps.newHashMap();
    confItems.put("key1", "value1");
    confItems.put("key2", "value2");
    RemoteStorageInfo remoteStorageInfo = new RemoteStorageInfo("test_path", confItems);
    ShuffleRegisterRequest shuffleRegisterRequest = new ShuffleRegisterRequest(1, "test_app", 1,
        partitionRanges, remoteStorageInfo, "test_user", ShuffleDataDistributionType.LOCAL_ORDER);
    int encodeLength = shuffleRegisterRequest.encodedLength();
    ByteBuf byteBuf = Unpooled.buffer(encodeLength, encodeLength);
    shuffleRegisterRequest.encode(byteBuf);
    ShuffleRegisterRequest shuffleRegisterRequest1 = ShuffleRegisterRequest.decode(byteBuf);

    assertEquals(shuffleRegisterRequest.getRequestId(), shuffleRegisterRequest1.getRequestId());
    assertEquals(shuffleRegisterRequest.getAppId(), shuffleRegisterRequest1.getAppId());
    assertEquals(shuffleRegisterRequest.getPartitionRanges(), shuffleRegisterRequest1.getPartitionRanges());
    assertEquals(shuffleRegisterRequest.getRemoteStorageInfo(), shuffleRegisterRequest1.getRemoteStorageInfo());
    assertEquals(shuffleRegisterRequest.getShuffleDataDistribution(),
        shuffleRegisterRequest1.getShuffleDataDistribution());
    assertEquals(shuffleRegisterRequest.getShuffleId(), shuffleRegisterRequest1.getShuffleId());
    assertEquals(shuffleRegisterRequest.getUser(), shuffleRegisterRequest1.getUser());
  }
}
