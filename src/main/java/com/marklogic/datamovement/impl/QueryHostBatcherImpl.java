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
import java.util.Calendar;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.io.SearchHandle;
import com.marklogic.client.query.MatchDocumentSummary;
import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.query.QueryManager;
import com.marklogic.datamovement.Batch;
import com.marklogic.datamovement.BatchListener;
import com.marklogic.datamovement.FailureListener;
import com.marklogic.datamovement.Forest;
import com.marklogic.datamovement.ForestConfiguration;
import com.marklogic.datamovement.QueryHostBatcher;
import com.marklogic.datamovement.QueryHostException;

public class QueryHostBatcherImpl extends HostBatcherImpl<QueryHostBatcher> implements QueryHostBatcher {
  private QueryDefinition query;
  private ForestConfiguration forestConfig;
  private ArrayList<BatchListener<String>> urisReadyListeners = new ArrayList<>();
  private ArrayList<FailureListener<QueryHostException>> failureListeners = new ArrayList<>();
  private QueryThreadPoolExecutor threadPool;

  public QueryHostBatcherImpl(QueryDefinition query, ForestConfiguration forestConfig) {
    super();
    this.query = query;
    this.forestConfig = forestConfig;
  }

  public QueryHostBatcherImpl onUrisReady(BatchListener<String> listener) {
    urisReadyListeners.add(listener);
    return this;
  }

  public QueryHostBatcherImpl onQueryFailure(FailureListener<QueryHostException> listener) {
    failureListeners.add(listener);
    return this;
  }

  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    requireJobStarted();
    return threadPool.awaitTermination(timeout, unit);
  }

  public boolean isTerminated() {
    requireJobStarted();
    return threadPool.isTerminated();
  }

  public boolean isTerminating() {
    requireJobStarted();
    return threadPool.isTerminating();
  }

  private void requireJobStarted() {
    if ( threadPool == null ) {
      throw new IllegalStateException("Job not started. First call DataMovementManager.startJob(QueryHostBatcher)");
    }
  }

  void start() {
    threadPool = new QueryThreadPoolExecutor(1, this);
    HashMap<String,Forest> oneForestPerHost = new HashMap<>();
    Forest[] forests = forestConfig.listForests();
    for ( Forest forest : forests ) {
      oneForestPerHost.put(forest.getHostName(), forest);
    }
    // default threadCount to hostCount
    int hostCount = oneForestPerHost.size();
    int threadCount = getThreadCount();
    if ( threadCount <= 0 ) threadCount = hostCount;
    threadPool.setCorePoolSize(threadCount);
    threadPool.setMaximumPoolSize(threadCount);
    for ( String host : oneForestPerHost.keySet() ) {
      final Forest forest = oneForestPerHost.get(host);
      final QueryDefinition finalQuery = query;
      final AtomicLong batchNumber = new AtomicLong();
      // right now this just launches one thread per host
      // TODO: (maybe) respect thread count
      Runnable runnable = new Runnable() {
        public void run() {
          DatabaseClient client = null;
          try {
            long resultsSoFar = 0;
            client = forestConfig.getForestClient(forest);
            QueryManager queryMgr = client.newQueryManager();
            queryMgr.setPageLength(getBatchSize());
            Calendar queryStart = Calendar.getInstance();
            SearchHandle results;
            do {
              results = queryMgr.search(finalQuery, new SearchHandle(), resultsSoFar + 1);
              MatchDocumentSummary[] docs = results.getMatchResults();
              resultsSoFar += docs.length;
              String[] uris = new String[docs.length];
              for ( int i=0; i < docs.length; i++ ) {
                uris[i] = docs[i].getUri();
              }
              Batch<String> batch = new BatchImpl<String>()
                .withBatchNumber(batchNumber.getAndIncrement())
                .withItems(uris)
                .withTimestamp(queryStart)
                .withForest(forest);
              for ( BatchListener<String> listener : urisReadyListeners ) {
                listener.processEvent(client, batch);
              }
            } while ( results != null &&
                      ( results.getTotalResults() > resultsSoFar ) );
          } catch (Throwable t) {
            for ( FailureListener<QueryHostException> listener : failureListeners ) {
              listener.processFailure(client, new QueryHostException(null, t));
            }
          }
        }
      };
      threadPool.execute(runnable);
    }
    threadPool.shutdown();
  }

  public static class QueryThreadPoolExecutor extends ThreadPoolExecutor {
    private Object objectToNotifyFrom;

    public QueryThreadPoolExecutor(int threadCount, Object objectToNotifyFrom) {
      super(threadCount, threadCount, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
      this.objectToNotifyFrom = objectToNotifyFrom;
    }

    protected void terminated() {
      super.terminated();
      synchronized(objectToNotifyFrom) {
        objectToNotifyFrom.notifyAll();
      }
    }
  }
}
