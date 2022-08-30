package org.apache.uniffle.test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Maps;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.shuffle.RssShuffleHandle;
import org.apache.spark.shuffle.RssShuffleManager;
import org.apache.spark.shuffle.ShuffleHandle;
import org.apache.spark.shuffle.ShuffleReadMetricsReporter;
import org.apache.spark.shuffle.ShuffleReader;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.common.ShuffleServerInfo;

public class MockRssShuffleManager extends RssShuffleManager {

  // shuffleId -> partShouldRequestNum
  Map<Integer, AtomicInteger> shuffleToPartShouldRequestNum = Maps.newConcurrentMap();

  public MockRssShuffleManager(SparkConf conf, boolean isDriver) {
    super(conf, isDriver);
  }

  @Override
  public <K, C> ShuffleReader<K, C> getReaderImpl(
      ShuffleHandle handle,
      int startMapIndex,
      int endMapIndex,
      int startPartition,
      int endPartition,
      TaskContext context,
      ShuffleReadMetricsReporter metrics,
      Roaring64NavigableMap taskIdBitmap) {
    int shuffleId = handle.shuffleId();
    RssShuffleHandle rssShuffleHandle = (RssShuffleHandle) handle;
    Map<Integer, List<ShuffleServerInfo>> allPartitionToServers = rssShuffleHandle.getPartitionToServers();
    int partitionNum = (int) allPartitionToServers.entrySet().stream()
        .filter(x -> x.getKey() >= startPartition && x.getKey() <= endPartition).count();
    AtomicInteger partShouldRequestNum = shuffleToPartShouldRequestNum.computeIfAbsent(shuffleId,
        x -> new AtomicInteger(0));
    partShouldRequestNum.addAndGet(partitionNum);
    return super.getReaderImpl(handle, startMapIndex, endMapIndex, startPartition, endPartition,
        context, metrics, taskIdBitmap);
  }

  public Map<Integer, AtomicInteger> getShuffleIdToPartitionNum() {
    return shuffleToPartShouldRequestNum;
  }
}
