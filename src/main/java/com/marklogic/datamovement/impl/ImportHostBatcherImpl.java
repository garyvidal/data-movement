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

import java.util.HashMap;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.Transaction;
import com.marklogic.client.document.DocumentManager;
import com.marklogic.client.document.ServerTransform;
import com.marklogic.client.impl.Utilities;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.client.io.marker.ContentHandle;
import com.marklogic.client.io.marker.DocumentMetadataWriteHandle;
//import com.marklogic.client.io.InputFormatHandle;
import com.marklogic.datamovement.BatchFailureListener;
import com.marklogic.datamovement.BatchListener;
import com.marklogic.datamovement.Forest;
import com.marklogic.datamovement.ForestConfiguration;
import com.marklogic.datamovement.ImportEvent;
import com.marklogic.datamovement.ImportHostBatcher;

public class ImportHostBatcherImpl
  extends HostBatcherImpl<ImportHostBatcher>
  implements ImportHostBatcher
{
  private long autoFlushInterval;
  private int transactionSize;
  private String temporalCollection;
  private ServerTransform transform;
  private ForestConfiguration forestConfig;
  private DatabaseClient client;
  private DocumentManager<?,?> docMgr;
  private HashMap<Forest, WriteBatch> batches = new HashMap<>();

  public ImportHostBatcherImpl(ForestConfiguration forestConfig) {
    super();
    this.forestConfig = forestConfig;
  }

  public synchronized void setClient(DatabaseClient client) {
    if ( client == null ) {
      throw new IllegalStateException("client must not be null");
    }
    if ( this.client != null ) {
      throw new IllegalStateException("You can only call setClient once per ImportHostBatcher instance");
    }
    this.client = client;
    this.docMgr = client.newDocumentManager();
  }

  public ImportHostBatcher add(String uri, AbstractWriteHandle contentHandle) {
    add(uri, null, contentHandle);
    return this;
  }

  public ImportHostBatcher addAs(String uri, Object content) {
    return addAs(uri, null, content);
  }

  public ImportHostBatcher add(String uri, DocumentMetadataWriteHandle metadataHandle,
      AbstractWriteHandle contentHandle)
  {
    Forest forest = assign(uri);
    synchronized(batches) {
      WriteBatch writeBatch = getBatch(forest);
      writeBatch.getWriteSet().add(uri, metadataHandle, contentHandle);
      if ( writeBatch.getWriteSet().size() >= getBatchSize() ) {
        // TODO: kick this off in another thread to reduce time spent in this synchronized block
        flushBatch(writeBatch);
      }
    }
    return this;
  }

  public ImportHostBatcher addAs(String uri, DocumentMetadataWriteHandle metadataHandle,
      Object content) {
    if (content == null) throw new IllegalArgumentException("content must not be null");

    Class<?> as = content.getClass();
    ContentHandle<?> handle = DatabaseClientFactory.getHandleRegistry().makeHandle(as);
    Utilities.setHandleContent(handle, content);
    return add(uri, metadataHandle, handle);
  }

  private Forest assign(String uri) {
    // TODO: actually get host or forest assignments
    return forestConfig.assign("default");
  }

  private WriteBatch getBatch(Forest forest) {
    WriteBatch batch = batches.get(forest);
    if ( batch == null ) {
      batch = initBatch(forest);
    }
    return batch;
  }

  public ImportHostBatcher onBatchSuccess(BatchListener<ImportEvent> listener) {
    // TODO: implement
    return this;
  }
  public ImportHostBatcher onBatchFailure(BatchFailureListener<ImportEvent> listener) {
    // TODO: implement
    return this;
  }

  /* flush every <interval> milliseconds */
  public ImportHostBatcher withAutoFlushInterval(long interval) {
    // TODO: implement triggering flush() at the specified intervals
    this.autoFlushInterval = interval;
    return this;
  }

  public long getAutoFlushInterval() {
    return autoFlushInterval;
  }

  /* treat any remaining batches as if they're full and send them to
   * ImportHostBatchFullListener
   */
  public void flush() {
    // TODO: write all batches as there will not always be just one
    for ( WriteBatch batch : batches.values() ) {
      flushBatch(batch);
    }
  }

  public void flushBatch(WriteBatch batch) {
    docMgr.write(batch.getWriteSet(), getTransform(), batch.getTransaction());
    int batchNumberInTransaction = batch.getBatchNumberInTransaction();
    if ( batchNumberInTransaction < transactionSize ) {
      Transaction transaction = batch.getTransaction();
      Forest forest = batch.getForest();
      synchronized(batches) {
        batches.put(forest, new WriteBatch(batchNumberInTransaction++, docMgr, transaction, forest));
      }
    }
  }

  public WriteBatch initBatch(Forest forest) {
    Transaction transaction = null;
    if ( transactionSize > 1 ) {
       transaction = client.openTransaction();
    }
    synchronized(batches) {
      WriteBatch batch = batches.get(forest);
      if ( batch == null ) {
        batch = new WriteBatch(1, docMgr, transaction, forest);
        batches.put(forest, batch);
      }
      return batch;
    }
  }

  public void finalize() {
    flush();
  }

  public ImportHostBatcher withTransactionSize(int transactionSize) {
    this.transactionSize = transactionSize;
    return this;
  }

  public int getTransactionSize() {
    return transactionSize;
  }

  public ImportHostBatcher withTemporalCollection(String collection) {
    this.temporalCollection = collection;
    return this;
  }

  public String getTemporalCollection() {
    return temporalCollection;
  }

  public ImportHostBatcher withTransform(ServerTransform transform) {
    this.transform = transform;
    return this;
  }

  public ServerTransform getTransform() {
    return transform;
  }

  public synchronized ImportHostBatcher withForestConfig(ForestConfiguration forestConfig) {
    this.forestConfig = forestConfig;
    return this;
  }

  public ForestConfiguration getForestConfig() {
    return forestConfig;
  }
}
