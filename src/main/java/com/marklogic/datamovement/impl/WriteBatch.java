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

import com.marklogic.client.Transaction;
import com.marklogic.client.document.DocumentManager;
import com.marklogic.client.document.DocumentWriteSet;
import com.marklogic.datamovement.Forest;

public class WriteBatch {
  private DocumentWriteSet writeSet;
  private Transaction transaction;
  private int batchNumberInTransaction;
  private Forest forest;

  public WriteBatch(int batchNumberInTransaction, DocumentManager<?,?> docMgr,
    Transaction transaction, Forest forest)
  {
    this.batchNumberInTransaction = batchNumberInTransaction;
    this.writeSet = docMgr.newWriteSet();
    this.transaction = transaction;
    this.forest = forest;
  }

  public DocumentWriteSet getWriteSet() {
    return writeSet;
  }

  public void setWriteSet(DocumentWriteSet writeSet) {
    this.writeSet = writeSet;
  }

  public Transaction getTransaction() {
    return transaction;
  }

  public void setTransaction(Transaction transaction) {
    this.transaction = transaction;
  }

  public int getBatchNumberInTransaction() {
    return batchNumberInTransaction;
  }

  public void setBatchNumberInTransaction(int batchNumberInTransaction) {
    this.batchNumberInTransaction = batchNumberInTransaction;
  }

  public Forest getForest() {
    return forest;
  }

  public void setForest(Forest forest) {
    this.forest = forest;
  }
}
