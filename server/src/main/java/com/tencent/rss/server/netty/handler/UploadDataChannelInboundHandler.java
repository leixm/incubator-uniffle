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

package com.tencent.rss.server.netty.handler;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.util.MessageConstants;
import com.tencent.rss.server.ShuffleServer;
import com.tencent.rss.server.ShuffleServerConf;
import com.tencent.rss.server.netty.message.UploadDataMessage;
import com.tencent.rss.server.netty.util.NettyUtils;

public class UploadDataChannelInboundHandler extends ChannelInboundHandlerAdapter {

  private static final Logger LOG = LoggerFactory.getLogger(UploadDataChannelInboundHandler.class);

  private final long idleTimeoutMillis;

  private String connectionInfo = "";

  private ShuffleServer shuffleServer;
  private IdleCheck idleCheck;

  public UploadDataChannelInboundHandler(
      ShuffleServer shuffleServer) {
    this.idleTimeoutMillis = shuffleServer.getShuffleServerConf().getLong(
        ShuffleServerConf.SERVER_NETTY_HANDLER_IDLE_TIMEOUT);
    this.shuffleServer = shuffleServer;
  }

  private static void schedule(ChannelHandlerContext ctx, Runnable task, long delayMillis) {
    ctx.executor().schedule(task, delayMillis, TimeUnit.MILLISECONDS);
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    super.channelActive(ctx);
    processChannelActive(ctx);
  }

  public void processChannelActive(final ChannelHandlerContext ctx) {
    // colinmjj: add metrics for connection
    connectionInfo = NettyUtils.getServerConnectionInfo(ctx);

    idleCheck = new IdleCheck(ctx, idleTimeoutMillis);
    schedule(ctx, idleCheck, idleTimeoutMillis);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    super.channelInactive(ctx);
    LOG.debug("Channel inactive: {}", connectionInfo);

    if (idleCheck != null) {
      idleCheck.cancel();
    }
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    ByteBuf resultBuf = ctx.alloc().buffer(1);
    if (idleCheck != null) {
      idleCheck.updateLastReadTime();
    }

    if (msg instanceof UploadDataMessage) {
      UploadDataMessage uploadDataMessage = (UploadDataMessage) msg;
      String appId = uploadDataMessage.getAppId();
      long requireBufferId = uploadDataMessage.getRequireBufferId();
      int shuffleId = uploadDataMessage.getShuffleId();
      Map<Integer, List<ShufflePartitionedBlock>> shuffleData = uploadDataMessage.getShuffleData();
      try {
        boolean isPreAllocated =
            shuffleServer.getShuffleTaskManager().isPreAllocated(requireBufferId);
        if (!isPreAllocated) {
          LOG.warn("Can't find requireBufferId[" + requireBufferId + "] for appId[" + appId
              + "], shuffleId[" + shuffleId + "]");
        }
        //        for (Map.Entry<Integer, List<ShufflePartitionedBlock>> entry : shuffleData.entrySet()) {
        //          String shuffleDataInfo = "appId[" + appId + "], shuffleId[" + shuffleId
        //              + "], partitionId[" + entry.getKey() + "]";
        //          ShufflePartitionedData spd = new ShufflePartitionedData(entry.getKey(), entry.getValue());
        //          StatusCode ret = shuffleServer
        //              .getShuffleTaskManager()
        //              .cacheShuffleData(appId, shuffleId, isPreAllocated, spd);
        //          if (ret != StatusCode.SUCCESS) {
        //            String errorMsg = "Error happened when add shuffle data for "
        //                + shuffleDataInfo + ", statusCode=" + ret;
        //            LOG.error(errorMsg);
        //            break;
        //          } else {
        //            shuffleServer.getShuffleTaskManager().updateCachedBlockCount(
        //                appId, shuffleId, entry.getValue().size());
        //          }
        //        }
        shuffleServer
            .getShuffleTaskManager().removeRequireBufferId(requireBufferId);
        resultBuf.writeByte(MessageConstants.RESPONSE_STATUS_OK);
        ctx.writeAndFlush(resultBuf).addListener(ChannelFutureListener.CLOSE);
      } catch (Exception e) {
        LOG.warn(
            "Error happened when add shuffle data for appId[" + appId
                + "], shuffleId[" + shuffleId + "] from " + connectionInfo, e);
        resultBuf.writeByte(MessageConstants.RESPONSE_STATUS_ERROR);
        ctx.writeAndFlush(resultBuf).addListener(ChannelFutureListener.CLOSE);
        return;
      }
    } else {
      throw new RuntimeException(String.format("Unsupported message: %s, %s", msg, connectionInfo));
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    String msg = "Got exception " + connectionInfo;
    LOG.warn(msg, cause);
    ctx.close();
  }

  private static class IdleCheck implements Runnable {

    private final ChannelHandlerContext ctx;
    private final long idleTimeoutMillis;

    private volatile long lastReadTime = System.currentTimeMillis();
    private volatile boolean canceled = false;

    IdleCheck(ChannelHandlerContext ctx, long idleTimeoutMillis) {
      this.ctx = ctx;
      this.idleTimeoutMillis = idleTimeoutMillis;
    }

    @Override
    public void run() {
      try {
        if (canceled) {
          return;
        }

        if (!ctx.channel().isOpen()) {
          return;
        }

        checkIdle(ctx);
      } catch (Throwable ex) {
        LOG.warn(String.format("Failed to run idle check, %s",
            NettyUtils.getServerConnectionInfo(ctx)), ex);
      }
    }

    public void updateLastReadTime() {
      lastReadTime = System.currentTimeMillis();
    }

    public void cancel() {
      canceled = true;
    }

    private void checkIdle(ChannelHandlerContext ctx) {
      if (System.currentTimeMillis() - lastReadTime >= idleTimeoutMillis) {
        // colinmjj: add metrics
        LOG.info("Closing idle connection {}", NettyUtils.getServerConnectionInfo(ctx));
        ctx.close();
        return;
      }

      schedule(ctx, this, idleTimeoutMillis);
    }
  }
}
