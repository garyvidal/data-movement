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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.Transaction;
import com.marklogic.client.document.DocumentWriteSet;
import com.marklogic.client.document.ServerTransform;
import com.marklogic.datamovement.Batch;
import com.marklogic.datamovement.Forest;
import com.marklogic.datamovement.WriteEvent;

public class BatchWriteSet {
  private DocumentWriteSet writeSet;
  private DatabaseClient client;
  private Transaction transaction;
  private ServerTransform transform;
  private String temporalCollection;
  private Forest forest;
  private Runnable onSuccess;
  private Consumer<Throwable> onFailure;
  private AtomicBoolean transactionRolledBack;

  public static class WriteEventImpl 
    extends DataMovementEventImpl<WriteEventImpl>
    implements WriteEvent
  {
    private String targetUri;

    public String getTargetUri() {
      return targetUri;
    }

    public WriteEventImpl withTargetUri(String targetUri) {
      this.targetUri = targetUri;
      return this;
    }
  }

  public BatchWriteSet(DocumentWriteSet writeSet, DatabaseClient client, Transaction transaction,
    Forest forest, ServerTransform transform, String temporalCollection, AtomicBoolean transactionRolledBack)
  {
    this.writeSet = writeSet;
    this.client = client;
    this.transaction = transaction;
    this.forest = forest;
    this.transform = transform;
    this.temporalCollection = temporalCollection;
    this.transactionRolledBack = transactionRolledBack;
  }

  public DocumentWriteSet getWriteSet() {
    return writeSet;
  }

  public void setWriteSet(DocumentWriteSet writeSet) {
    this.writeSet = writeSet;
  }

  public DatabaseClient getClient() {
    return client;
  }

  public void setClient(DatabaseClient client) {
    this.client = client;
  }

  public Transaction getTransaction() {
    return transaction;
  }

  public void setTransaction(Transaction transaction) {
    this.transaction = transaction;
  }

  public ServerTransform getTransform() {
    return transform;
  }

  public void setTransform(ServerTransform transform) {
    this.transform = transform;
  }

  public String getTemporalCollection() {
    return temporalCollection;
  }

  public void setTemporalCollection(String temporalCollection) {
    this.temporalCollection = temporalCollection;
  }

  public Forest getForest() {
    return forest;
  }

  public void setForest(Forest forest) {
    this.forest = forest;
  }

  public Runnable getOnSuccess() {
    return onSuccess;
  }

  public void onSuccess(Runnable onSuccess) {
    this.onSuccess = onSuccess;
  }

  public Consumer<Throwable>  getOnFailure() {
    return onFailure;
  }

  public void onFailure(Consumer<Throwable>  onFailure) {
    this.onFailure = onFailure;
  }

  public boolean getTransactionRolledBack() {
    return transactionRolledBack.get();
  }

  public void setTransactionRolledBack(AtomicBoolean transactionRolledBack) {
    this.transactionRolledBack = transactionRolledBack;
  }

  public Batch<WriteEvent> getBatchOfWriteEvents() {
    Batch<WriteEvent> batch = new BatchImpl<WriteEvent>();
    WriteEvent[] writeEvents = getWriteSet().stream()
            .map(writeOperation ->  new WriteEventImpl().withTargetUri(writeOperation.getUri()))
            .toArray(WriteEventImpl[]::new);
    batch.withItems(writeEvents);
    return batch;
  }
}
