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
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.Transaction;
import com.marklogic.client.document.ServerTransform;
import com.marklogic.client.impl.Utilities;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.client.io.marker.ContentHandle;
import com.marklogic.client.io.marker.DocumentMetadataWriteHandle;
import com.marklogic.client.io.marker.StructureReadHandle;
import com.marklogic.datamovement.Batch;
import com.marklogic.datamovement.BatchFailureListener;
import com.marklogic.datamovement.BatchListener;
import com.marklogic.datamovement.DataMovementException;
import com.marklogic.datamovement.Forest;
import com.marklogic.datamovement.ForestConfiguration;
import com.marklogic.datamovement.WriteHostBatcher;
import com.marklogic.datamovement.WriteEvent;

/**
 * Features
 *   - multiple threads can concurrently call add/addAs
 *     - we don't manage these threads, they're outside this
 *     - no synchronization or unnecessary delays while queueing
 *     - no extra threads required here
 *     - we don't proactively read streams, so don't leave them in the queue too long
 *   - topology-aware by calling /v1/forestinfo
 *     - get list of hosts which have writeable forests
 *     - each write hits the next writeable host for round-robin network calls
 *   - manage a threadPool of size threadCount for network calls
 *   - when batchSize reached, writes a batch
 *     - using a thread from threadPool
 *     - no synchronization or unnecessary delays while emptying queue
 *     - and calls each successListener (if not using transactions)
 *   - if usingTransactions (transactionSize > 1)
 *     - opens transactions as needed
 *       - using a thread from threadPool
 *       - but not before, lest we increase likelihood of transaction timeout
 *       - threads needing the transaction must wait for it to open
 *     - after batch write check if transactionSize reached and if so, commit the transaction
 *       - don't check before write to avoid race condition where the last batch writes and commits
 *         before the second to last batch writes
 *       - then call each successListener for each transaction batch
 *   - when a batch fails, calls each failureListener
 *     - and calls rollback (if using transactions)
 *       - using a thread from threadPool
 *   - flush writes all batches whether full or not
 *     - and commits the transaction for each batch so nothing is left uncommitted
 *     - and resets counter so the next batch will be a normal batch size
 *   - awaitCompletion allows the calling thread to block until all WriteHostBatcher threads are finished
 *     writing batches or committing transactions (or calling rollback)
 *
 * Design
 *   - track
 *     - one queue of DocumentToWrite
 *     - each host
 *       - client (contains http connection pool)
 *         - auth challenge once per client
 *       - number of batches
 *         - used to see where we are in a transaction
 *       - current transaction
 *         - with batches already written
 *       - tranaction permits track how many more batches can use the transaction
 *     - each transaction
 *       - host
 *
 * Known issues
 *   - does not guarantee minimal batch loss on transaction failure
 *     - if two batches attempt to write at the same time and one fails, the other will be part of
 *       the rollback whether it fails or not
 *     - however, any subsequent batches that attempt to write will be in a new transaction
 */
public class WriteHostBatcherImpl
  extends HostBatcherImpl<WriteHostBatcher>
  implements WriteHostBatcher
{
  private int transactionSize;
  private String temporalCollection;
  private ServerTransform transform;
  private ForestConfiguration forestConfig;
  private LinkedBlockingQueue<DocumentToWrite> queue = new LinkedBlockingQueue<>();
  private ArrayList<BatchListener<WriteEvent>> successListeners = new ArrayList<>();
  private ArrayList<BatchFailureListener<WriteEvent>> failureListeners = new ArrayList<>();
  private AtomicLong recordNumber = new AtomicLong(0);
  private AtomicLong batchNumber = new AtomicLong(0);
  private AtomicLong batchCounter = new AtomicLong(0);
  private AtomicLong transactionCounter = new AtomicLong(0);
  private HostInfo[] hostInfo;
  private boolean initialized = false;
  private WriteThreadPoolExecutor threadPool;
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private boolean usingTransactions = false;

  public WriteHostBatcherImpl(ForestConfiguration forestConfig) {
    super();
    this.forestConfig = forestConfig;
  }

  public synchronized void initialize() {
    if ( initialized == true ) return;
    if ( getClient() == null ) {
      throw new IllegalStateException("Client must be set before calling add or addAs");
    }
    if ( getBatchSize() <= 0 ) withBatchSize(1);
    if ( transactionSize > 1 ) usingTransactions = true;
System.out.println("DEBUG: [WriteHostBatcherImpl] usingTransactions =[" + usingTransactions  + "]");
    Forest[] forests = forestConfig.listForests();
    if ( forests.length == 0 ) {
      throw new IllegalStateException("WriteHostBatcher requires at least one writeable forest");
    }
    HashSet<String> hosts = new HashSet<>();
    for ( Forest forest : forests ) {
      if ( forest.getHostName() == null ) {
        throw new IllegalStateException("Hostname must not be null for any forest");
      }
      hosts.add(forest.getHostName());
    }
    hostInfo = new HostInfo[hosts.size()];
    DatabaseClient client = getClient();
    int i=0;
    for ( String host : hosts ) {
      hostInfo[i] = new HostInfo();
      hostInfo[i].hostName = host;
      hostInfo[i].transactionSize = getTransactionSize();
      if ( host.equals(client.getHost()) ) {
        hostInfo[i].client = client;
      } else {
        hostInfo[i].client = DatabaseClientFactory.newClient(
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
    int threadCount = getThreadCount();
    if ( threadCount <= 0 ) threadCount = hosts.size();
    // create a threadPool where threads are kept alive for up to one minute of inactivity
    threadPool = new WriteThreadPoolExecutor(threadCount, this);
    initialized = true;
  }

  public synchronized void setClient(DatabaseClient client) {
    requireNotInitialized();
    super.setClient(client);

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
    initialize();
    requireNotStopped();
    try {
System.out.println("DEBUG: [WriteHostBatcherImpl.add] uri=[" + uri + "]");
      queue.put( new DocumentToWrite(uri, metadataHandle, contentHandle) );
    } catch(InterruptedException e) {
      throw new IllegalStateException(
        "LinkedBlockingQueue is unbounded, so put should not block and therefore should not be interrupted", e);
    }
    long currentRecordNumber = recordNumber.incrementAndGet();
    // if we have queued batchSize, it's time to flush a batch
    long recordInBatch = batchCounter.incrementAndGet();
System.out.println("DEBUG: [WriteHostBatcherImpl] [Thread:" + Thread.currentThread().getName() + "] recordInBatch =[" + recordInBatch  + "]");
    boolean timeToWriteBatch = (recordInBatch % getBatchSize()) == 0;
    if ( timeToWriteBatch ) {
      BatchWriteSet writeSet = newBatchWriteSet(false);
      for (int i=0; i < getBatchSize(); i++ ) {
        try {
          DocumentToWrite doc = queue.take();
System.out.println("DEBUG: [WriteHostBatcherImpl.add] [queue.take] doc.uri=[" + doc.uri + "]");
System.out.println("DEBUG: [WriteHostBatcherImpl.add] writeSet=[" + writeSet + "]");
          writeSet.getWriteSet().add(doc.uri, doc.metadataHandle, doc.contentHandle);
        } catch(InterruptedException e) {
          throw new IllegalStateException(
            "LinkedBlockingQueue is unbounded, so put should not block and therefore should not be interrupted", e);
        }
      }
      threadPool.submit( new BatchWriter(writeSet, failureListeners) );
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

  private void requireNotInitialized() {
    if ( initialized == true ) {
      throw new IllegalStateException("Configuration cannot be changed after calling add or addAs");
    }
  }

  private void requireNotStopped() {
    if ( stopped.get() == true ) throw new IllegalStateException("This instance has been stopped");
  }

  private BatchWriteSet newBatchWriteSet(boolean forceNewTransaction) {
    long batchNum = batchNumber.incrementAndGet();
    int hostToUse = (int) (batchNum % hostInfo.length);
    HostInfo host = hostInfo[hostToUse];
    DatabaseClient hostClient = host.client;
System.out.println("DEBUG: [WriteHostBatcherImpl.newBatchWriteSet] usingTransactions =[" + usingTransactions  + "]");
    if ( usingTransactions ) {
      long transactionCount = transactionCounter.getAndIncrement();
System.out.println("DEBUG: [WriteHostBatcherImpl] transactionCount =[" + transactionCount  + "]");
      // if this is the first batch in this transaction, it's time to initialize a transaction
      boolean timeForNewTransaction = (transactionCount % getTransactionSize()) == 0;
System.out.println("DEBUG: [WriteHostBatcherImpl] timeForNewTransaction =[" + timeForNewTransaction  + "]");
      if ( timeForNewTransaction ) {
        // time for a new transaction, let's initialize it in a separate thread
        threadPool.submit( new TransactionOpener(host, hostClient, transactionSize) );
      }
    }
    TransactionInfo transactionInfo = host.getTransactionInfo();
    BatchWriteSet batchWriteSet = new BatchWriteSet(
      hostClient.newDocumentManager().newWriteSet(), hostClient, transactionInfo.transaction,
      null, getTransform(), getTemporalCollection(), transactionInfo.rolledBack);
System.out.println("DEBUG: [WriteHostBatcherImpl.newBatchWriteSet] batchWriteSet =[" + batchWriteSet  + "]");
    batchWriteSet.onSuccess( () -> {
        // if we're not using transactions then timeToCommit is always true
        boolean timeToCommit = true;
        if ( usingTransactions ) {
          long batchNumFinished = transactionInfo.batchesFinished.incrementAndGet();
          timeToCommit = (batchNumFinished == getTransactionSize());
          if ( forceNewTransaction || timeToCommit ) {
            // this is the last batch in the transaction
            transactionInfo.transaction.commit();
            // if we just did a forced commit, let's restart transactionCounter
            if ( forceNewTransaction == true ) transactionCounter.set(0);
            for ( BatchWriteSet transactionWriteSet : transactionInfo.batches ) {
              Batch<WriteEvent> batch = transactionWriteSet.getBatchOfWriteEvents();
              for ( BatchListener<WriteEvent> successListener : successListeners ) {
                successListener.processEvent(hostClient, batch);
              }
            }
          } else {
            // this is *not* the last batch in the transaction
            // so queue up this batchWriteSet
            try {
              transactionInfo.batches.put(batchWriteSet);
            } catch(InterruptedException e) {
              throw new IllegalStateException(
                "LinkedBlockingQueue is unbounded, so put should not block and therefore should not be interrupted", e);
            }
          }
        }
        if ( timeToCommit ) {
          Batch<WriteEvent> batch = batchWriteSet.getBatchOfWriteEvents();
          for ( BatchListener<WriteEvent> successListener : successListeners ) {
            successListener.processEvent(hostClient, batch);
          }
        }
    });
    batchWriteSet.onFailure( (throwable) -> {
System.out.println("DEBUG: [WriteHostBatcherImpl] throwable=[" + throwable + "]");
      // reset the transactionCounter so the next write will start a new transaction
      transactionCounter.set(0);
System.out.println("DEBUG: [WriteHostBatcherImpl] transactionInfo.rolledBack =[" + transactionInfo.rolledBack  + "]");
      boolean rolledBack = transactionInfo.rolledBack.getAndSet(true);
      if ( usingTransactions && rolledBack == false ) {
        if ( rolledBack == false ) {
          try { transactionInfo.transaction.rollback(); } catch(Throwable t2) {}
        }
        for ( BatchWriteSet transactionWriteSet : transactionInfo.batches ) {
          Batch<WriteEvent> batch = transactionWriteSet.getBatchOfWriteEvents();
          for ( BatchFailureListener<WriteEvent> failureListener : failureListeners ) {
            failureListener.processEvent(hostClient, batch, throwable);
          }
        }
      }
      Batch<WriteEvent> batch = batchWriteSet.getBatchOfWriteEvents();
      for ( BatchFailureListener<WriteEvent> failureListener : failureListeners ) {
        failureListener.processEvent(hostClient, batch, throwable);
      }
    });
    return batchWriteSet;
  }

  private Forest assign(String uri) {
    // TODO: actually get host or forest assignments
    return forestConfig.assign("default");
  }

  public WriteHostBatcher onBatchSuccess(BatchListener<WriteEvent> listener) {
    successListeners.add(listener);
    return this;
  }
  public WriteHostBatcher onBatchFailure(BatchFailureListener<WriteEvent> listener) {
    failureListeners.add(listener);
    return this;
  }

  public void flush() {
    requireNotStopped();
    ArrayList<DocumentToWrite> docs = new ArrayList<>();
    batchCounter.set(0);
    queue.drainTo(docs);
    Iterator<DocumentToWrite> iter = docs.iterator();
    boolean forceNewTransaction = true;
    while ( iter.hasNext() ) {
      BatchWriteSet writeSet = newBatchWriteSet(forceNewTransaction);
      for ( int i=0; i < getBatchSize() && iter.hasNext(); i++ ) {
        DocumentToWrite doc = iter.next();
System.out.println("DEBUG: [WriteHostBatcherImpl.flush] doc.uri=[" + doc.uri + "]");
System.out.println("DEBUG: [WriteHostBatcherImpl.flush] writeSet=[" + writeSet + "]");
        writeSet.getWriteSet().add(doc.uri, doc.metadataHandle, doc.contentHandle);
      }
      threadPool.submit( new BatchWriter(writeSet, failureListeners) );
    }
    try { threadPool.awaitCompletion(Long.MAX_VALUE, TimeUnit.DAYS); } catch(InterruptedException i) {}
  }

  public void stop() {
    threadPool.shutdown();
    stopped.lazySet(true);
  }

  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return threadPool.awaitTermination(timeout, unit);
  }

  public boolean awaitCompletion(long timeout, TimeUnit unit) throws InterruptedException {
    return threadPool.awaitCompletion(timeout, unit);
  }

  public void finalize() {
    stop();
  }

  public WriteHostBatcher withTransactionSize(int transactionSize) {
    requireNotInitialized();
    this.transactionSize = transactionSize;
    return this;
  }

  public int getTransactionSize() {
    return transactionSize;
  }

  public WriteHostBatcher withTemporalCollection(String collection) {
    requireNotInitialized();
    this.temporalCollection = collection;
    return this;
  }

  public String getTemporalCollection() {
    return temporalCollection;
  }

  public WriteHostBatcher withTransform(ServerTransform transform) {
    requireNotInitialized();
    this.transform = transform;
    return this;
  }

  public ServerTransform getTransform() {
    return transform;
  }

  public synchronized WriteHostBatcher withForestConfig(ForestConfiguration forestConfig) {
    requireNotInitialized();
    this.forestConfig = forestConfig;
    return this;
  }

  public ForestConfiguration getForestConfig() {
    return forestConfig;
  }

  public static class DocumentToWrite {
    public String uri;
    public DocumentMetadataWriteHandle metadataHandle;
    public AbstractWriteHandle contentHandle;

    public DocumentToWrite(String uri, DocumentMetadataWriteHandle metadata, AbstractWriteHandle content) {
      this.uri = uri;
      this.metadataHandle = metadata;
      this.contentHandle = content;
    }
  }

  public static class HostInfo {
    public String hostName;
    public DatabaseClient client;
    private AtomicReference<TransactionInfo> transactionInfo = new AtomicReference<>();
    public Semaphore transactionPermits = new Semaphore(0);
    public int transactionSize;

    private TransactionInfo getTransactionInfo() {
      // if any more batches can be written for this transaction then transactionPermits
      // can be acquired and this transaction is available
      // otherwise block until a new transaction is available with new permits
      try {
          transactionPermits.acquire();
          return transactionInfo.get();
      } catch (InterruptedException e) {
          return null;
      }
    }

    public void setTransactionInfo(TransactionInfo transactionInfo, int numPermits) {
      // first reference the new transactionInfo
      this.transactionInfo.set(transactionInfo);
      // then free up the given number of permits
      this.transactionPermits.release(numPermits);
    }

    public void releaseTransactionInfo(TransactionInfo toRelease) {
      int drainedPermits = this.transactionPermits.drainPermits();
      if ( this.transactionInfo.compareAndSet(toRelease, null) == false ) {
        // hmm, the transactionInfo is already new, I guess we can allow
        // it to finish its remaining number of batches
        this.transactionPermits.release(drainedPermits);
      }
    }
  }

  private static class TransactionInfo {
    private Transaction transaction;
    public AtomicBoolean rolledBack = new AtomicBoolean(false);
    public AtomicLong batchesFinished = new AtomicLong(0);
    public LinkedBlockingQueue<BatchWriteSet> batches = new LinkedBlockingQueue<>();
  }

  public static class TransactionOpener implements Runnable {
    private HostInfo host;
    private DatabaseClient client;
    private int transactionSize;

    public TransactionOpener(HostInfo host, DatabaseClient client, int transactionSize) {
      this.host = host;
      this.client = client;
      this.transactionSize = transactionSize;
    }

    public void run() {
      TransactionInfo transactionInfo = new TransactionInfo();
      Transaction realTransaction = client.openTransaction();
      // wrapping Transaction so I can call releaseTransactionInfo when rollback is called
      Transaction transaction = new Transaction() {
        public void commit() { realTransaction.commit(); }
        public List<javax.ws.rs.core.NewCookie> getCookies() { return realTransaction.getCookies(); }
        public String getHostId() { return realTransaction.getHostId(); }
        public String getTransactionId() { return realTransaction.getTransactionId(); }
        public <T extends StructureReadHandle> T readStatus(T handle) {
          return realTransaction.readStatus(handle);
        }
        public void rollback() {
          host.releaseTransactionInfo(transactionInfo);
          realTransaction.rollback();
        }
      };
      transactionInfo.transaction = transaction;
      host.setTransactionInfo(transactionInfo, transactionSize);
System.out.println("DEBUG: [WriteHostBatcherImpl.run] transactionSize=[" + transactionSize + "]");
    }
  }

  public static class BatchWriter implements Runnable {
    private BatchWriteSet writeSet;
    private ArrayList<BatchFailureListener<WriteEvent>> failureListeners;

    public BatchWriter(BatchWriteSet writeSet,
      ArrayList<BatchFailureListener<WriteEvent>> failureListeners )
    {
      if ( writeSet.getWriteSet().size() == 0 ) {
        throw new IllegalStateException("Attempt to write an empty batch");
      }
      this.writeSet = writeSet;
      this.failureListeners = failureListeners;
    }

    public void run() {
      try {
System.out.println("DEBUG: [WriteHostBatcherImpl] [Thread:" + Thread.currentThread().getName() + "] writeSet.getTransactionRolledBack()=[" + writeSet.getTransactionRolledBack() + "]");
System.out.println("DEBUG: [WriteHostBatcherImpl.run] [Thread:" + Thread.currentThread().getName() + "] writeSet=[" + writeSet + "]");
System.out.println("DEBUG: [WriteHostBatcherImpl] [Thread:" + Thread.currentThread().getName() + "] writeSet.getWriteSet().size()=[" + writeSet.getWriteSet().size() + "]");
        if ( ! writeSet.getTransactionRolledBack() ) {
          writeSet.getClient().newXMLDocumentManager().write(
            writeSet.getWriteSet(), writeSet.getTransform(),
            writeSet.getTransaction(), writeSet.getTemporalCollection()
          );
          writeSet.getOnSuccess().run();
        } else {
          throw new DataMovementException("Failed to write because transaction already underwent rollback", null);
        }
      } catch (Throwable t) {
System.out.println("DEBUG: [WriteHostBatcherImpl] t=[" + t + "]");
        writeSet.getOnFailure().accept(t);
      }
    }
  }

  public static class WriteThreadPoolExecutor extends ThreadPoolExecutor {
    private Object objectToNotifyFrom;
    private ConcurrentHashMap<Runnable,Future<?>> futures = new ConcurrentHashMap<>();

    public WriteThreadPoolExecutor(int threadCount, Object objectToNotifyFrom) {
      super(threadCount, threadCount, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>());
      this.objectToNotifyFrom = objectToNotifyFrom;
    }

    protected void afterExecute(Runnable task, Throwable t) {
      super.afterExecute(task, t);
      futures.remove(task);
    }

    public Future<?> submit(Runnable task) {
      Future<?> future = super.submit(task);
      futures.put(task, future);
      return future;
    }

    public boolean awaitCompletion(long timeout, TimeUnit unit) throws InterruptedException {
      ArrayList<Future<?>> snapshotOfFutures = new ArrayList<>();
      for ( Future<?> future : futures.values() ) {
        if ( future != null ) snapshotOfFutures.add( future );
      }
      CountDownLatch latch = new CountDownLatch(snapshotOfFutures.size());
      for ( Future<?> future : snapshotOfFutures ) {
        new Thread(() -> {
          try {
            future.get(timeout, unit);
            latch.countDown();
          } catch (TimeoutException e) {
          } catch (Exception e) {
            latch.countDown();
          }
        }).start();
      }
      return latch.await(timeout, unit);
    }

    protected void terminated() {
      super.terminated();
      synchronized(objectToNotifyFrom) {
        objectToNotifyFrom.notifyAll();
      }
    }
  }
  /*
  Host {
    String hostName
    DatabaseClient
    // if first batch in a transction, open transaction
    // if last batch in a transction, commit transaction
    AtomicLong batchesCount
    Transaction (optional) {
      Batch {
        WriteSet {
          DocumentRecord
        }
      }
    }
  }
   */
}
