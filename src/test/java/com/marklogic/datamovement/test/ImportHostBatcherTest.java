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
import com.marklogic.datamovement.WriteEvent;
import com.marklogic.datamovement.WriteHostBatcher;

public class ImportHostBatcherTest {
  private DataMovementManager moveMgr = DataMovementManager.newInstance();
  private static DatabaseClient client =
    DatabaseClientFactory.newClient("localhost", 8000, "admin", "admin", Authentication.DIGEST);
  private static DocumentManager<?,?> docMgr = client.newDocumentManager();
  private static String uri1 = "ImportHostBatcherTest_content_1.txt";
  private static String uri2 = "ImportHostBatcherTest_content_2.txt";
  private static String uri3 = "ImportHostBatcherTest_content_3.txt";
  private static String collection = "ImportHostBatcherTest";
  private static String transform = "ImportHostBatcherTest_transform.sjs";

  @BeforeClass
  public static void beforeClass() {
    installModule();
  }

  @AfterClass
  public static void afterClass() {
    docMgr.delete(uri1, uri2, uri3);
    client.release();
  }

  public static void installModule() {
    client.newServerConfigManager().newTransformExtensionsManager().writeJavascriptTransform(
      transform, new FileHandle(new File("src/test/resources/" + transform)));
  }

  @Test
  public void testArgs() throws Exception {
    moveMgr.setClient(client);

    assertEquals( "Since the doc doesn't exist, docMgr.exists() should return null",
      null, docMgr.exists(uri1) );

    final StringBuffer successListenerWasRun = new StringBuffer();
    final StringBuffer failListenerWasRun = new StringBuffer();
    WriteHostBatcher batcher = moveMgr.newWriteHostBatcher()
      .withBatchSize(2)
      .withTransform(
        new ServerTransform(transform)
          .addParameter("newValue", "test2")
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
          assertEquals("There should be one item in the batch", 1, batch.getItems().length);
        }
      );
    JobTicket ticket = moveMgr.startJob(batcher);

    DocumentMetadataHandle meta = new DocumentMetadataHandle()
      .withCollections(collection);
    JsonNode doc1 = new ObjectMapper().readTree("{ \"testProperty\": \"test1\" }");
    JsonNode doc2 = new ObjectMapper().readTree("{ \"testProperty2\": \"test2\" }");
    StringHandle doc3 = new StringHandle("<thisIsNotJson>test3</thisIsNotJson>")
      .withFormat(Format.JSON);
    batcher.addAs(uri1, meta, doc1);
    batcher.addAs(uri2, meta, doc2);
    batcher.add(uri3, meta, doc3);
    batcher.flush();
    assertEquals("The listner should have run", "true", successListenerWasRun.toString());
    assertEquals("The listner should have run", "true", failListenerWasRun.toString());

    QueryDefinition query = new StructuredQueryBuilder().collection(collection);
    DocumentPage docs = docMgr.search(query, 1);
    assertEquals("there should be two docs in the collection", 2, docs.getTotalSize());

    for (DocumentRecord record : docs ) {
      if ( uri1.equals(record.getUri()) ) {
        assertEquals( "the transform should have changed testProperty to 'test2'",
          "test2", record.getContentAs(JsonNode.class).get("testProperty").textValue() );
      }
    }
  }
}
