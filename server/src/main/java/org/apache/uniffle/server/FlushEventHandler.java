package org.apache.uniffle.server;

public interface FlushEventHandler {
  void handle(ShuffleDataFlushEvent event);
}
