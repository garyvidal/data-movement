/*
 * Copyright 2015 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.marklogic.datamovement.impl;

import com.marklogic.client.DatabaseClient;
import com.marklogic.datamovement.HostBatcher;

public class HostBatcherImpl<T extends HostBatcher<T>> implements HostBatcher<T> {
  private String jobName;
  private int batchSize;
  private int threadCount;
  private DatabaseClient client;

  @SuppressWarnings("unchecked")
  public T withJobName(String jobName) {
    this.jobName = jobName;
    return (T) this;
  }

  public String getJobName() {
    return jobName;
  }

  @SuppressWarnings("unchecked")
  public T withBatchSize(int batchSize) {
    this.batchSize = batchSize;
    return (T) this;
  }

  public int getBatchSize() {
    return batchSize;
  }

  @SuppressWarnings("unchecked")
  public T withThreadCount(int threadCount) {
    this.threadCount = threadCount;
    return (T) this;
  }

  public int getThreadCount() {
    return threadCount;
  }

  public synchronized void setClient(DatabaseClient client) {
    if ( client == null ) {
      throw new IllegalStateException("client must not be null");
    }
    if ( this.client != null ) {
      throw new IllegalStateException("You can only call setClient once per ImportHostBatcher instance");
    }
    this.client = client;
  }

  public DatabaseClient getClient() {
    return client;
  }

}
