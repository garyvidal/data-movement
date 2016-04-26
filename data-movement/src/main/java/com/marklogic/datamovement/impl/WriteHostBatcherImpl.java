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
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicLong;

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
  private int transactionSize;
  private String temporalCollection;
  private ServerTransform transform;
  private ForestConfiguration forestConfig;
  private BatchWriteSet writeSet;
  private ArrayList<BatchListener<WriteEvent>> successListeners = new ArrayList<>();
  private ArrayList<BatchFailureListener<WriteEvent>> failureListeners = new ArrayList<>();
  private AtomicLong batchNumber = new AtomicLong(0);
  private HashSet<String> hosts = new HashSet<>();
  private DatabaseClient[] clients;

  public WriteHostBatcherImpl(ForestConfiguration forestConfig) {
    super();
    this.forestConfig = forestConfig;
    for ( Forest forest : forestConfig.listForests()) {
      hosts.add(forest.getHostName());
    }
  }

  public synchronized void setClient(DatabaseClient client) {
    super.setClient(client);
    clients = new DatabaseClient[hosts.size()];
    int i=0;
    for ( String host : hosts ) {
      if ( host.equals(client.getHost()) ) {
        clients[i] = client;
      } else {
        clients[i] = DatabaseClientFactory.newClient(
          host,
          client.getPort(),
          client.getDatabase(),
          client.getUser(),
          client.getPassword(),
          client.getAuthentication(),
          client.getForestName(),
          client.getSSLContext(),
          client.getSSLHostnameVerifier()
        );
      }
      i++;
    }

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
    BatchWriteSet writeSet = getBatch();
    synchronized(writeSet) {
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

  private BatchWriteSet getBatch() {
    if ( writeSet == null ) {
      writeSet = initTransaction();
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

  /* treat any remaining writeSets as if they're full and send them to
   * ImportHostBatchFullListener
   */
  public void flush() {
    if ( writeSet != null ) {
      flushBatch(writeSet);
    }
  }

  public synchronized void flushBatch(BatchWriteSet writeSet) {
    // TODO: optimize by making this method unsynchronized so we don't block while communicating with server
    // if we're not initialized, return
    if ( writeSet == null ) return;
    // if there are no documents to write, return
    if ( writeSet.getWriteSet() == null || writeSet.getWriteSet().size() == 0 ) return;
    long batchNum = batchNumber.incrementAndGet();
    // use mod operator to round-robin through hosts
    int hostToUse = (int) (batchNum % clients.length);
    DatabaseClient client = clients[hostToUse];
    Transaction transaction = writeSet.getTransaction();
    Batch<WriteEvent> batch = writeSet.getBatchOfWriteEvents();
    try {
      client.newXMLDocumentManager().write(writeSet.getWriteSet(), getTransform(), transaction, getTemporalCollection());
      int batchNumberInTransaction = writeSet.getBatchNumberInTransaction();
      if ( batchNumberInTransaction >= getTransactionSize() ) {
        if ( transaction != null ) transaction.commit();
        for ( BatchListener<WriteEvent> successListener : successListeners ) {
          successListener.processEvent(client, batch);
        }
        clearAndInitTransaction();
      } else {
        clearAndInitBatch(transaction);
      }
    } catch (Throwable t) {
      try { if ( transaction != null ) transaction.rollback(); } catch(Throwable t2) {}
      clearAndInitTransaction();
      for ( BatchFailureListener<WriteEvent> failureListener : failureListeners ) {
        failureListener.processEvent(client, batch, t);
      }
    }
  }

  public synchronized void clearAndInitTransaction() {
    writeSet = null;
    initTransaction();
  }

  public synchronized void clearAndInitBatch(Transaction transaction) {
    writeSet = null;
    initBatch(transaction);
  }

  public synchronized BatchWriteSet initTransaction() {
    if ( writeSet != null ) return writeSet;
    Transaction transaction = null;
    if ( transactionSize > 1 ) {
       transaction = getClient().openTransaction();
    }
    return initBatch(transaction);
  }

  public synchronized BatchWriteSet initBatch(Transaction transaction) {
    if ( writeSet != null ) return writeSet;
    writeSet = new BatchWriteSet(1, getClient().newXMLDocumentManager().newWriteSet(),
      transaction, null);
    return writeSet;
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
