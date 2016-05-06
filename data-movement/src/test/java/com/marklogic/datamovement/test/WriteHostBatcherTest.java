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
package com.marklogic.datamovement.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Random;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.Authentication;
import com.marklogic.client.document.DocumentManager;
import com.marklogic.client.document.DocumentPage;
import com.marklogic.client.document.DocumentRecord;
import com.marklogic.client.document.ServerTransform;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.FileHandle;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.query.DeleteQueryDefinition;
import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.StructuredQueryBuilder;
import com.marklogic.datamovement.DataMovementManager;
import com.marklogic.datamovement.JobTicket;
import com.marklogic.datamovement.WriteEvent;
import com.marklogic.datamovement.WriteHostBatcher;

public class WriteHostBatcherTest {
  private DataMovementManager moveMgr = DataMovementManager.newInstance();
  private static DatabaseClient client;
  private static DocumentManager<?,?> docMgr;
  private static String uri1 = "WriteHostBatcherTest_content_1.txt";
  private static String uri2 = "WriteHostBatcherTest_content_2.txt";
  private static String uri3 = "WriteHostBatcherTest_content_3.txt";
  private static String uri4 = "WriteHostBatcherTest_content_4.txt";
  private static String transform = "WriteHostBatcherTest_transform.sjs";
  private static String whbTestCollection = "WriteHostBatcherTest_" +
    new Random().nextInt(10000);


  @BeforeClass
  public static void beforeClass() {
    client = Common.connectEval();
    docMgr = client.newDocumentManager();
    installModule();
  }

  @AfterClass
  public static void afterClass() {
    docMgr.delete(uri1, uri2, uri3);
    QueryManager queryMgr = client.newQueryManager();
    DeleteQueryDefinition deleteQuery = queryMgr.newDeleteDefinition();
    deleteQuery.setCollections(whbTestCollection);
    queryMgr.delete(deleteQuery);

    client.release();
  }

  public static void installModule() {
    Common.newAdminClient().newServerConfigManager().newTransformExtensionsManager().writeJavascriptTransform(
      transform, new FileHandle(new File("src/test/resources/" + transform)));
  }

  @Test
  public void testSimple() throws Exception {
    moveMgr.setClient(client);
    String collection = whbTestCollection + ".testSimple";

    StringBuffer successBatch = new StringBuffer();
    StringBuffer failureBatch = new StringBuffer();
    WriteHostBatcher ihb1 =  moveMgr.newWriteHostBatcher()
      .withBatchSize(1)
      .onBatchSuccess(
        (client, batch) -> {
          for(WriteEvent w: batch.getItems()){
            successBatch.append(w.getTargetUri()+":");
          }
      })
      .onBatchFailure(
        (client, batch, throwable) -> {
          for(WriteEvent w: batch.getItems()){
            failureBatch.append(w.getTargetUri()+":");
          }
      });

    DocumentMetadataHandle meta = new DocumentMetadataHandle()
      .withCollections(collection, whbTestCollection);
    ihb1.add("/doc/jackson", meta, new JacksonHandle(new ObjectMapper().readTree("{\"test\":true}")))
      //.add("/doc/reader_wrongxml", new ReaderHandle)
      .add("/doc/string", meta, new StringHandle("test"));
      /*
      .add("/doc/file", docMeta2, new FileHandle)
      .add("/doc/is", new InputStreamHandle)
      .add("/doc/os_wrongjson", docMeta2, new OutputStreamHandle)
      .add("/doc/bytes", docMeta1, new BytesHandle)
      .add("/doc/dom", new DomHandle);
      */

    ihb1.flush();
  }
  @Test
  public void testWrites() throws Exception {
    String collection = whbTestCollection + ".testWrites";
    moveMgr.setClient(client);

    assertEquals( "Since the doc doesn't exist, docMgr.exists() should return null",
      null, docMgr.exists(uri1) );

    final StringBuffer successListenerWasRun = new StringBuffer();
    final StringBuffer failListenerWasRun = new StringBuffer();
    WriteHostBatcher batcher = moveMgr.newWriteHostBatcher()
      .withBatchSize(2)
      .withTransform(
        new ServerTransform(transform)
          .addParameter("newValue", "test1a")
      )
      .onBatchSuccess(
        (client, batch) -> {
          successListenerWasRun.append("true");
          assertEquals("There should be two items in the batch", 2, batch.getItems().length);
        }
      )
      .onBatchFailure(
        (client, batch, throwable) -> {
          failListenerWasRun.append("true");
          assertEquals("There should be two items in the batch", 2, batch.getItems().length);
        }
      );
    JobTicket ticket = moveMgr.startJob(batcher);

    DocumentMetadataHandle meta = new DocumentMetadataHandle()
      .withCollections(collection, whbTestCollection);
    JsonNode doc1 = new ObjectMapper().readTree("{ \"testProperty\": \"test1\" }");
    JsonNode doc2 = new ObjectMapper().readTree("{ \"testProperty2\": \"test2\" }");
    // the batch with this doc will fail to write because we say withFormat(JSON)
    // but it isn't valid JSON. That will trigger our onBatchFailure listener.
    StringHandle doc3 = new StringHandle("<thisIsNotJson>test3</thisIsNotJson>")
      .withFormat(Format.JSON);
    JsonNode doc4 = new ObjectMapper().readTree("{ \"testProperty4\": \"test4\" }");
    batcher.addAs(uri1, meta, doc1);
    batcher.addAs(uri2, meta, doc2);
    batcher.add(uri3, meta, doc3);
    batcher.add(uri4, meta, new JacksonHandle(doc4));
    batcher.flush();
    assertEquals("The success listener should have run", "true", successListenerWasRun.toString());
    assertEquals("The failure listener should have run", "true", failListenerWasRun.toString());

    QueryDefinition query = new StructuredQueryBuilder().collection(collection);
    DocumentPage docs = docMgr.search(query, 1);
    // only doc1 and doc2 wrote successfully, doc3 failed
    assertEquals("there should be two docs in the collection", 2, docs.getTotalSize());

    for (DocumentRecord record : docs ) {
      if ( uri1.equals(record.getUri()) ) {
        assertEquals( "the transform should have changed testProperty to 'test1a'",
          "test1a", record.getContentAs(JsonNode.class).get("testProperty").textValue() );
      }
    }
  }

  @Test
  public void testZeros() throws Exception {
    runWriteTest(0, 0, 1, 0, 10, "zeros");
  }

  @Test
  public void testOnes() throws Exception {
    runWriteTest(1, 1, 1, 1, 10, "ones");
  }

  @Test
  public void testExternalThreads() throws Exception {
    runWriteTest(1, 1, 3, 1, 10, "threads");
  }

  @Test
  public void testTransactionsAndThreads() throws Exception {
    runWriteTest(1, 2, 3, 3, 30, "transactionsThreads");
  }

  @Test
  public void testBatchesTransactionsThreads() throws Exception {
    runWriteTest(2, 2, 3, 3, 30, "batchesTransactionsThreads");
  }

  @Test
  public void testBatchesThreads() throws Exception {
    runWriteTest(2, 1, 20, 20, 200, "batchesThreads");
  }

  @Test
  public void testEverything() throws Exception {
    runWriteTest(2, 4, 20, 20, 200, "everything");
  }

  public void runWriteTest( int batchSize, int transactionSize,
    int externalThreadCount, int batcherThreadCount, int totalDocCount, String testName)
  {
    String config = "{ batchSize:           " + batchSize + ",\n" + 
                    "  transactionSize:     " + transactionSize + ",\n" + 
                    "  externalThreadCount: " + externalThreadCount + ",\n" + 
                    "  batcherThreadCount:  " + batcherThreadCount + ",\n" + 
                    "  totalDocCount:       " + totalDocCount + " }"; 
    System.out.println("Starting test " + testName + " with config=" + config);

    String collection = whbTestCollection + ".testWrites_" + testName;
    long start = System.currentTimeMillis();
    moveMgr.setClient(client);

    int expectedBatchSize = (batchSize > 0) ? batchSize : 1;
    final AtomicInteger successfulCount = new AtomicInteger(0);
    final AtomicInteger failureCount = new AtomicInteger(0);
    WriteHostBatcher batcher = moveMgr.newWriteHostBatcher()
      .withBatchSize(batchSize)
      .withTransactionSize(transactionSize)
      .withThreadCount(batcherThreadCount)
      .onBatchSuccess(
        (client, batch) -> {
          for ( WriteEvent event : batch.getItems() ) {
            successfulCount.incrementAndGet();
System.out.println("DEBUG: [WriteHostBatcherTest.onBatchSuccess] event.getTargetUri()=[" + event.getTargetUri() + "]");
          }
          assertEquals("There should be " + expectedBatchSize + " items in the batch",
            expectedBatchSize, batch.getItems().length);
        }
      )
      .onBatchFailure(
        (client, batch, throwable) -> {
          throwable.printStackTrace();
          for ( WriteEvent event : batch.getItems() ) {
            failureCount.incrementAndGet();
System.out.println("DEBUG: [WriteHostBatcherTest.onBatchFailure] event.getTargetUri()=[" + event.getTargetUri() + "]");
          }
          assertEquals("There should be " + expectedBatchSize + " items in the batch",
            expectedBatchSize, batch.getItems().length);
        }
      );
    JobTicket ticket = moveMgr.startJob(batcher);

    DocumentMetadataHandle meta = new DocumentMetadataHandle()
      .withCollections(whbTestCollection, collection);

    class MyRunnable implements Runnable {

      @Override
      public void run() {

        String threadName = Thread.currentThread().getName();
        // first write half the valid docs
        int docsPerExternalThread = (int) totalDocCount / externalThreadCount;
        int halfway = (int) docsPerExternalThread / 2;
        for (int j =1 ;j < halfway; j++){
          String uri = "/" + collection + "/"+ threadName + "/" + j + ".txt";
          batcher.add(uri, meta, new StringHandle("test").withFormat(Format.TEXT));
        }

        // then write one invalid doc
        // each transaction (two batches) with this doc will fail to write
        // because we say withFormat(JSON) but it isn't valid JSON.
        StringHandle nonJson = new StringHandle("<thisIsNotJson>test3</thisIsNotJson>")
          .withFormat(Format.JSON);
        String badUri = "/" + collection + "/"+ threadName + "/bad.json";
        batcher.add(badUri, meta, nonJson);

        // then write the second half of valid docs
        for (int j =halfway; j <= docsPerExternalThread; j++){
          String uri = "/" + collection + "/"+ threadName + "/" + j + ".txt";
          batcher.add(uri, meta, new StringHandle("test").withFormat(Format.TEXT));
        }

      }
    }
    Thread[] externalThreads = new Thread[externalThreadCount];
    for ( int i=0; i < externalThreads.length; i++ ) {
      externalThreads[i] = new Thread(new MyRunnable(), testName + i);
      externalThreads[i].start();
    }

    for ( Thread thread : externalThreads ) {
      try { thread.join(); } catch (Exception e) {}
    }
    batcher.flush();

System.out.println("DEBUG: [WriteHostBatcherTest] successfulCount.get()=[" + successfulCount.get() + "]");
System.out.println("DEBUG: [WriteHostBatcherTest] failureCount.get()=[" + failureCount.get() + "]");
    /*
    assertEquals("The success listener should have run six times",
      6, successListenerWasRun.get());
    assertEquals("The failure listener should have run five times", 
      5, failureCount.get());
    */

    QueryDefinition query = new StructuredQueryBuilder().collection(collection);
    DocumentPage docs = docMgr.search(query, 1);
    assertEquals("there should be " + successfulCount + " docs in the collection", successfulCount.get(), docs.getTotalSize());

    long duration = System.currentTimeMillis() - start;
    System.out.println("Completed test " + testName + " in " + duration + " millis");
  }

  @Test
  public void testAddMultiThreadedSuccess_Issue61() throws Exception{
    moveMgr.setClient(client);

    String collection = whbTestCollection + ".testAddMultiThreadedSuccess_Issue61";
    String query1 = "fn:count(fn:collection('" + collection + "'))";
    WriteHostBatcher batcher =  moveMgr.newWriteHostBatcher();
    batcher.withBatchSize(100);
    batcher.onBatchSuccess(
        (client, batch) -> {
        System.out.println("Batch size "+batch.getItems().length);
        for(WriteEvent w:batch.getItems()){
        //System.out.println("Success "+w.getTargetUri());
        }



        }
        )
      .onBatchFailure(
          (client, batch, throwable) -> {
          throwable.printStackTrace();
          for(WriteEvent w:batch.getItems()){
          System.out.println("Failure "+w.getTargetUri());
          }


          });
    moveMgr.startJob(batcher);

    DocumentMetadataHandle meta = new DocumentMetadataHandle()
      .withCollections(collection, whbTestCollection);

    class MyRunnable implements Runnable {

      @Override
        public void run() {

          for (int j =0 ;j < 100; j++){
            String uri ="/local/json-"+ j+"-"+Thread.currentThread().getId();
            System.out.println("Thread name: "+Thread.currentThread().getName()+"  URI:"+ uri);
            batcher.add(uri, meta, new StringHandle("test").withFormat(Format.TEXT));


          }
          batcher.flush();
        }  

    } 
    Thread t1,t2,t3;
    t1 = new Thread(new MyRunnable());
    t2 = new Thread(new MyRunnable());
    t3 = new Thread(new MyRunnable());
    t1.start();
    t2.start();
    t3.start();

    t1.join();
    t2.join();
    t3.join();

    int docCount = client.newServerEval().xquery(query1).eval().next().getNumber().intValue();
    assertEquals(300, docCount);
  }

  @Test
  public void testAddMultiThreadedSuccess_Issue48() throws Exception{
    moveMgr.setClient(client);

    String collection = whbTestCollection + ".testAddMultiThreadedSuccess_Issue48";
    String query1 = "fn:count(fn:collection('" + collection + "'))";
    WriteHostBatcher batcher =  moveMgr.newWriteHostBatcher();
    batcher.withBatchSize(120);
    batcher
      .onBatchSuccess( (client, batch) -> {
        System.out.println("Success Batch size "+batch.getItems().length);
        for(WriteEvent w:batch.getItems()){
          System.out.println("Success "+w.getTargetUri());
        }
      })
      .onBatchFailure( (client, batch, throwable) -> {
        throwable.printStackTrace();
        System.out.println("Failure Batch size "+batch.getItems().length);
        for(WriteEvent w:batch.getItems()){
          System.out.println("Failure "+w.getTargetUri());
        }
      });
    moveMgr.startJob(batcher);

    DocumentMetadataHandle meta = new DocumentMetadataHandle()
      .withCollections(collection, whbTestCollection);

    FileHandle fileHandle = new FileHandle(new File("src/test/resources/test.xml"));

    class MyRunnable implements Runnable {

      @Override
        public void run() {

          for (int j =0 ;j < 100; j++){
            String uri ="/local/json-"+ j+"-"+Thread.currentThread().getId();
            System.out.println("Thread name: "+Thread.currentThread().getName()+"  URI:"+ uri);
            batcher.add(uri, meta, fileHandle);


          }
          batcher.flush();
        } 

    }
    Thread t1,t2,t3;
    t1 = new Thread(new MyRunnable());
    t2 = new Thread(new MyRunnable());
    t3 = new Thread(new MyRunnable());
    t1.start();
    t2.start();
    t3.start();

    t1.join();
    t2.join();
    t3.join();

    int docCount = client.newServerEval().xquery(query1).eval().next().getNumber().intValue();
    assertEquals(300, docCount);
  }
}
