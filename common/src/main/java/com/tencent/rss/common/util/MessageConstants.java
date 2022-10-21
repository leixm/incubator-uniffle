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

package com.tencent.rss.common.util;

public class MessageConstants {

  public static final byte UPLOAD_DATA_MAGIC_BYTE = 'u';
  public static final byte MESSAGE_UPLOAD_DATA_PARTITION_END = 0;
  public static final byte MESSAGE_UPLOAD_DATA_PARTITION_CONTINUE = 1;
  public static final byte RESPONSE_STATUS_OK = 20;
  public static final byte RESPONSE_STATUS_ERROR = 21;
  public static final int DEFAULT_SHUFFLE_DATA_MESSAGE_SIZE = 32 * 1024;
  public static final int MESSAGE_UPLOAD_DATA_END = -1;
  public static final int MESSAGE_TYPE_UPLOAD_DATA = 1;

  public static final byte GET_LOCAL_SHUFFLE_DATA_MAGIC_BYTE = 'f';
  public static final byte GET_IN_MEMORY_SHUFFLE_DATA_MAGIC_BYTE = 'i';
}
