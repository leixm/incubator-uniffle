package org.apache.uniffle.client.request;

import java.util.Set;

public class RssGetShuffleResultForMultiPartRequest {
  private String appId;
  private int shuffleId;
  private Set<Integer> partitions;

  public RssGetShuffleResultForMultiPartRequest(String appId, int shuffleId, Set<Integer> partitions) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitions = partitions;
  }

  public String getAppId() {
    return appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public Set<Integer> getPartitions() {
    return partitions;
  }
}
