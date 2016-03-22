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

import java.util.ArrayList;
import java.util.HashMap;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.Transaction;
import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.document.ServerTransform;
import com.marklogic.client.impl.Utilities;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.client.io.marker.ContentHandle;
import com.marklogic.client.io.marker.DocumentMetadataWriteHandle;
import com.marklogic.datamovement.Batch;
import com.marklogic.datamovement.BatchFailureListener;
import com.marklogic.datamovement.BatchListener;
import com.marklogic.datamovement.Forest;
import com.marklogic.datamovement.ForestConfiguration;
import com.marklogic.datamovement.WriteHostBatcher;
import com.marklogic.datamovement.WriteEvent;

public class WriteHostBatcherImpl
  extends HostBatcherImpl<WriteHostBatcher>
  implements WriteHostBatcher
{
  private long autoFlushInterval;
  private int transactionSize;
  private String temporalCollection;
  private ServerTransform transform;
  private ForestConfiguration forestConfig;
  private XMLDocumentManager docMgr;
  private HashMap<Forest, BatchWriteSet> writeSets = new HashMap<>();
  private ArrayList<BatchListener<WriteEvent>> successListeners = new ArrayList<>();
  private ArrayList<BatchFailureListener<WriteEvent>> failureListeners = new ArrayList<>();

  public WriteHostBatcherImpl(ForestConfiguration forestConfig) {
    super();
    this.forestConfig = forestConfig;
  }

  public synchronized void setClient(DatabaseClient client) {
    super.setClient(client);
    // use XMLDocumentManager because it can use temporalCollection
    this.docMgr = getClient().newXMLDocumentManager();
  }

  public WriteHostBatcher add(String uri, AbstractWriteHandle contentHandle) {
    add(uri, null, contentHandle);
    return this;
  }

  public WriteHostBatcher addAs(String uri, Object content) {
    return addAs(uri, null, content);
  }

  public WriteHostBatcher add(String uri, DocumentMetadataWriteHandle metadataHandle,
      AbstractWriteHandle contentHandle)
  {
    Forest forest = assign(uri);
    synchronized(writeSets) {
      BatchWriteSet writeSet = getBatch(forest);
      writeSet.getWriteSet().add(uri, metadataHandle, contentHandle);
      if ( writeSet.getWriteSet().size() >= getBatchSize() ) {
        // TODO: kick this off in another thread to reduce time spent in this synchronized block
        flushBatch(writeSet);
      }
    }
    return this;
  }

  public WriteHostBatcher addAs(String uri, DocumentMetadataWriteHandle metadataHandle,
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

  private BatchWriteSet getBatch(Forest forest) {
    BatchWriteSet writeSet = writeSets.get(forest);
    if ( writeSet == null ) {
      writeSet = initBatch(forest);
    }
    return writeSet;
  }

  public WriteHostBatcher onBatchSuccess(BatchListener<WriteEvent> listener) {
    successListeners.add(listener);
    return this;
  }
  public WriteHostBatcher onBatchFailure(BatchFailureListener<WriteEvent> listener) {
    failureListeners.add(listener);
    return this;
  }

  /* flush every <interval> milliseconds */
  public WriteHostBatcher withAutoFlushInterval(long interval) {
    // TODO: implement triggering flush() at the specified intervals
    this.autoFlushInterval = interval;
    return this;
  }

  public long getAutoFlushInterval() {
    return autoFlushInterval;
  }

  /* treat any remaining writeSets as if they're full and send them to
   * ImportHostBatchFullListener
   */
  public void flush() {
    for ( BatchWriteSet writeSet : writeSets.values() ) {
System.out.println("DEBUG: [WriteHostBatcherImpl] writeSet.getWriteSet().size()=[" + writeSet.getWriteSet().size() + "]");
      flushBatch(writeSet);
    }
  }

  public void flushBatch(BatchWriteSet writeSet) {
    Transaction transaction = writeSet.getTransaction();
    try {
      docMgr.write(writeSet.getWriteSet(), getTransform(), transaction, getTemporalCollection());
      int batchNumberInTransaction = writeSet.getBatchNumberInTransaction();
      Forest forest = writeSet.getForest();
      if ( batchNumberInTransaction >= getTransactionSize() ) {
        synchronized(writeSets) {
          writeSets.remove(forest);
        }
        if ( transaction != null ) transaction.commit();
        for ( BatchListener<WriteEvent> successListener : successListeners ) {
          successListener.processEvent(getClient(), writeSet.getBatchOfWriteEvents());
        }
      } else {
        synchronized(writeSets) {
          writeSets.put(forest, new BatchWriteSet(batchNumberInTransaction++, docMgr, transaction, forest));
        }
      }
    } catch (Throwable t) {
      Batch<WriteEvent> batch = writeSet.getBatchOfWriteEvents();
      for ( BatchFailureListener<WriteEvent> failureListener : failureListeners ) {
        failureListener.processEvent(getClient(), batch, t);
      }
    }
  }

  public BatchWriteSet initBatch(Forest forest) {
    Transaction transaction = null;
    if ( transactionSize > 1 ) {
       transaction = getClient().openTransaction();
    }
    synchronized(writeSets) {
      BatchWriteSet writeSet = writeSets.get(forest);
      if ( writeSet == null ) {
        writeSet = new BatchWriteSet(1, docMgr, transaction, forest);
        writeSets.put(forest, writeSet);
      }
      return writeSet;
    }
  }

  public void finalize() {
    flush();
  }

  public WriteHostBatcher withTransactionSize(int transactionSize) {
    this.transactionSize = transactionSize;
    return this;
  }

  public int getTransactionSize() {
    return transactionSize;
  }

  public WriteHostBatcher withTemporalCollection(String collection) {
    this.temporalCollection = collection;
    return this;
  }

  public String getTemporalCollection() {
    return temporalCollection;
  }

  public WriteHostBatcher withTransform(ServerTransform transform) {
    this.transform = transform;
    return this;
  }

  public ServerTransform getTransform() {
    return transform;
  }

  public synchronized WriteHostBatcher withForestConfig(ForestConfiguration forestConfig) {
    this.forestConfig = forestConfig;
    return this;
  }

  public ForestConfiguration getForestConfig() {
    return forestConfig;
  }
}
