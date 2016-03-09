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
package com.marklogic.datamovement;

import com.marklogic.client.document.ServerTransform;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.client.io.marker.DocumentMetadataWriteHandle;

public interface ImportHostBatcher extends HostBatcher<ImportHostBatcher> {
  public ImportHostBatcher add(String uri, AbstractWriteHandle contentHandle);
  public ImportHostBatcher addAs(String uri, Object content);
  public ImportHostBatcher add(String uri, DocumentMetadataWriteHandle metadataHandle,
      AbstractWriteHandle contentHandle);
  public ImportHostBatcher addAs(String uri, DocumentMetadataWriteHandle metadataHandle,
      Object content);
  public ImportHostBatcher onBatchSuccess(BatchListener<WriteEvent> listener);
  public ImportHostBatcher onBatchFailure(BatchFailureListener<WriteEvent> listener);

  /** Treat any remaining batches as if they're full and send them */
  public void flush();
  public void finalize(); // calls flush()

  /** Flush every <interval> milliseconds */
  public ImportHostBatcher withAutoFlushInterval(long interval);
  public long getAutoFlushInterval();

  public ImportHostBatcher withTransactionSize(int transactionSize);
  public int getTransactionSize();

  public ImportHostBatcher withTemporalCollection(String collection);
  public String getTemporalCollection();

  public ImportHostBatcher withTransform(ServerTransform transform);
  public ServerTransform getTransform();

  public ImportHostBatcher withForestConfig(ForestConfiguration forestConfig);
  public ForestConfiguration getForestConfig();
}
