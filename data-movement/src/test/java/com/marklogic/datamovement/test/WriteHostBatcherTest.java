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
  private static String uri1 = "ImportHostBatcherTest_content_1.txt";
  private static String uri2 = "ImportHostBatcherTest_content_2.txt";
  private static String uri3 = "ImportHostBatcherTest_content_3.txt";
  private static String uri4 = "ImportHostBatcherTest_content_4.txt";
  private static String transform = "ImportHostBatcherTest_transform.sjs";
  private static String testTransactionsCollection = "ImportHostBatcherTest.testTransactions_" +
    new Random().nextInt(10000);


  @BeforeClass
  public static void beforeClass() {
    client = Common.connect();
    docMgr = client.newDocumentManager();
    installModule();
  }

  @AfterClass
  public static void afterClass() {
    docMgr.delete(uri1, uri2, uri3);
    QueryManager queryMgr = client.newQueryManager();
    DeleteQueryDefinition deleteQuery = queryMgr.newDeleteDefinition();
    deleteQuery.setCollections(testTransactionsCollection);
    queryMgr.delete(deleteQuery);

    client.release();
  }

  public static void installModule() {
    Common.newAdminClient().newServerConfigManager().newTransformExtensionsManager().writeJavascriptTransform(
      transform, new FileHandle(new File("src/test/resources/" + transform)));
  }

  @Test
  public void testWrites() throws Exception {
    String collection = "ImportHostBatcherTest.testWrites";
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
      .withCollections(collection);
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
  public void testTransactions() throws Exception {
    moveMgr.setClient(client);

    final AtomicInteger successListenerWasRun = new AtomicInteger(0);
    final AtomicInteger failListenerWasRun = new AtomicInteger(0);
    WriteHostBatcher batcher = moveMgr.newWriteHostBatcher()
      .withBatchSize(2)
      .withTransactionSize(2)
      .withThreadCount(2)
      .onBatchSuccess(
        (client, batch) -> {
          successListenerWasRun.incrementAndGet();
          for ( WriteEvent event : batch.getItems() ) {
System.out.println("DEBUG: [WriteHostBatcherTest.onBatchSuccess] event.getTargetUri()=[" + event.getTargetUri() + "]");
          }
          assertEquals("There should be two items in the batch", 2, batch.getItems().length);
        }
      )
      .onBatchFailure(
        (client, batch, throwable) -> {
          throwable.printStackTrace();
          failListenerWasRun.incrementAndGet();
          for ( WriteEvent event : batch.getItems() ) {
System.out.println("DEBUG: [WriteHostBatcherTest.onBatchFailure] event.getTargetUri()=[" + event.getTargetUri() + "]");
          }
          assertEquals("There should be two items in the batch", 2, batch.getItems().length);
        }
      );
    JobTicket ticket = moveMgr.startJob(batcher);

    DocumentMetadataHandle meta = new DocumentMetadataHandle()
      .withCollections(testTransactionsCollection);

    class MyRunnable implements Runnable {

      @Override
      public void run() {

        // first write four valid docs
        for (int j =1 ;j < 5; j++){
          String uri = "/" + testTransactionsCollection + "/"+ Thread.currentThread().getName() + "/" + j + ".txt";
          batcher.add(uri, meta, new StringHandle("test").withFormat(Format.TEXT));
        }

        // then write one invalid doc
        // each transaction (two batches) with this doc will fail to write
        // because we say withFormat(JSON) but it isn't valid JSON.
        StringHandle doc3 = new StringHandle("<thisIsNotJson>test3</thisIsNotJson>")
          .withFormat(Format.JSON);
        batcher.add("doc3.json", meta, doc3);

        // then write five more valid docs
        for (int j =5 ;j < 10; j++){
          String uri = "/" + testTransactionsCollection + "/"+ Thread.currentThread().getName() + "/" + j + ".txt";
          batcher.add(uri, meta, new StringHandle("test").withFormat(Format.TEXT));
        }

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
    batcher.flush();

System.out.println("DEBUG: [WriteHostBatcherTest] successListenerWasRun.get()=[" + successListenerWasRun.get() + "]");
System.out.println("DEBUG: [WriteHostBatcherTest] failListenerWasRun.get()=[" + failListenerWasRun.get() + "]");
    /*
    assertEquals("The success listener should have run six times",
      6, successListenerWasRun.get());
    assertEquals("The failure listener should have run five times", 
      5, failListenerWasRun.get());
    */

    QueryDefinition query = new StructuredQueryBuilder().collection(testTransactionsCollection);
    DocumentPage docs = docMgr.search(query, 1);
    assertEquals("there should be twenty four docs in the collection", 24, docs.getTotalSize());
  }
}
