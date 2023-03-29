package org.apache.uniffle.server;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Queues;

import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.ThreadUtils;
import org.apache.uniffle.server.storage.StorageManager;
import org.apache.uniffle.storage.common.HdfsStorage;
import org.apache.uniffle.storage.common.LocalStorage;
import org.apache.uniffle.storage.common.Storage;
import org.apache.uniffle.storage.util.StorageType;

public class StorageTypeFlushEventHandler implements FlushEventHandler {
  private final ShuffleServerConf shuffleServerConf;
  private final ShuffleFlushManager shuffleFlushManager;
  private final StorageManager storageManager;
  private Executor localFileThreadPoolExecutor;
  private Executor hdfsThreadPoolExecutor;
  private final StorageType storageType;

  public StorageTypeFlushEventHandler(ShuffleServerConf conf, ShuffleFlushManager shuffleFlushManager,
      StorageManager storageManager) {
    this.shuffleServerConf = conf;
    this.storageType = StorageType.valueOf(shuffleServerConf.get(RssBaseConf.RSS_STORAGE_TYPE));
    this.shuffleFlushManager = shuffleFlushManager;
    this.storageManager = storageManager;
    initFlushEventExecutor();
  }

  @Override
  public void handle(ShuffleDataFlushEvent event) {
    Storage storage = storageManager.selectStorage(event);
    if (storage instanceof HdfsStorage) {
      hdfsThreadPoolExecutor.execute(() -> handleEventAndUpdateMetrics(event, false));
    } else if (storage instanceof LocalStorage) {
      localFileThreadPoolExecutor.execute(() -> handleEventAndUpdateMetrics(event, true));
    } else {
      throw new RssException("Unexpected storage type!");
    }
  }

  private void handleEventAndUpdateMetrics(ShuffleDataFlushEvent event, boolean isLocalFile) {
    try {
      shuffleFlushManager.processEvent(event);
    } finally {
      if (isLocalFile) {
        ShuffleServerMetrics.counterLocalFileEventFlush.inc();
      } else {
        ShuffleServerMetrics.counterHdfsEventFlush.inc();
      }
    }
  }

  protected void initFlushEventExecutor() {
    if (StorageType.withLocalfile(storageType)) {
      int poolSize = shuffleServerConf.getInteger(ShuffleServerConf.SERVER_FLUSH_LOCALFILE_THREAD_POOL_SIZE);
      localFileThreadPoolExecutor = createFlushEventExecutor(poolSize, "LocalFileFlushEventThreadPool");
    }
    if (StorageType.withHDFS(storageType)) {
      int poolSize = shuffleServerConf.getInteger(ShuffleServerConf.SERVER_FLUSH_HDFS_THREAD_POOL_SIZE);
      hdfsThreadPoolExecutor = createFlushEventExecutor(poolSize, "HdfsFlushEventThreadPool");
    }
  }

  protected Executor createFlushEventExecutor(int poolSize, String threadFactoryName) {
    int waitQueueSize = shuffleServerConf.getInteger(
        ShuffleServerConf.SERVER_FLUSH_THREAD_POOL_QUEUE_SIZE);
    BlockingQueue<Runnable> waitQueue = Queues.newLinkedBlockingQueue(waitQueueSize);
    long keepAliveTime = shuffleServerConf.getLong(ShuffleServerConf.SERVER_FLUSH_THREAD_ALIVE);
    return new ThreadPoolExecutor(poolSize, poolSize, keepAliveTime, TimeUnit.SECONDS, waitQueue,
        ThreadUtils.getThreadFactory(threadFactoryName));
  }
}
