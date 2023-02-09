package org.apache.uniffle.common.exception;

public class NotRetryException extends RssException {

  public NotRetryException(String message) {
    super(message);
  }

  public NotRetryException(String message, Throwable e) {
    super(message, e);
  }
}