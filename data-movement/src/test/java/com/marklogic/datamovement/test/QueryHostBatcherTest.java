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
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.query.StructuredQueryBuilder;
import com.marklogic.datamovement.DataMovementManager;
import com.marklogic.datamovement.JobTicket;
import com.marklogic.datamovement.QueryHostBatcher;
import com.marklogic.datamovement.WriteEvent;
import com.marklogic.datamovement.WriteHostBatcher;

public class QueryHostBatcherTest {
  private DataMovementManager moveMgr = DataMovementManager.newInstance();
  private static DatabaseClient client =
    DatabaseClientFactory.newClient("localhost", 8012, "admin", "admin", Authentication.DIGEST);
  private static DocumentManager<?,?> docMgr = client.newDocumentManager();
  private static String uri1 = "QueryHostBatcherTest_content_1.txt";
  private static String uri2 = "QueryHostBatcherTest_content_2.txt";
  private static String uri3 = "QueryHostBatcherTest_content_3.txt";
  private static String uri4 = "QueryHostBatcherTest_content_4.txt";
  private static String collection = "QueryHostBatcherTest";

  @BeforeClass
  public static void beforeClass() {
  }

  @AfterClass
  public static void afterClass() {
    docMgr.delete(uri1, uri2, uri3, uri4);
    client.release();
  }

  @Test
  public void testArgs() throws Exception {
    moveMgr.setClient(client);

    assertEquals( "Since the doc doesn't exist, docMgr.exists() should return null",
      null, docMgr.exists(uri1) );

    WriteHostBatcher writeBatcher = moveMgr.newWriteHostBatcher();
    moveMgr.startJob(writeBatcher);
    // a collection so we're only looking at docs related to this test
    DocumentMetadataHandle meta = new DocumentMetadataHandle()
      .withCollections(collection);
    // all the docs are one-word text docs
    writeBatcher.addAs(uri1, meta, "doc1");
    writeBatcher.addAs(uri2, meta, "doc2");
    writeBatcher.addAs(uri3, meta, "doc3");
    writeBatcher.addAs(uri4, meta, "doc4");
    writeBatcher.flush();

    // a collection query to get all four docs
    QueryDefinition query = new StructuredQueryBuilder().collection(collection);
    DocumentPage docs = docMgr.search(query, 1);
    assertEquals("there should be four docs in the collection", 4, docs.getTotalSize());

    final StringBuffer urisReadyListenerWasRun = new StringBuffer();
    final StringBuffer failListenerWasRun = new StringBuffer();
    QueryHostBatcher queryBatcher = moveMgr.newQueryHostBatcher(query)
      .onUrisReady(
        (client, batch) -> {
System.out.println("DEBUG: [QueryHostBatcherTest] batch.getForest().getForestName()=[" + batch.getForest().getForestName() + "]");
          urisReadyListenerWasRun.append("true");
          assertEquals("There should be four items in the batch", 4, batch.getItems().length);
        }
      )
      .onQueryFailure(
        (client, throwable) -> {
          failListenerWasRun.append("true");
          throwable.printStackTrace();
        }
      );
    moveMgr.startJob(queryBatcher);
    Thread.sleep(2000);

    assertEquals("The listener should have run", "true", urisReadyListenerWasRun.toString());
    assertEquals("The listener should not have run", "", failListenerWasRun.toString());
  }
}
