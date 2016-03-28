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

import static org.custommonkey.xmlunit.XMLAssert.assertXMLEqual;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.HashMap;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.Authentication;
import com.marklogic.client.document.GenericDocumentManager;
import com.marklogic.client.impl.HandleAccessor;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.FileHandle;
import com.marklogic.client.query.CtsQueryDefinition;
import com.marklogic.client.query.DeleteQueryDefinition;
import com.marklogic.client.query.QueryManager;
import com.marklogic.datamovement.DataMovementManager;
import com.marklogic.datamovement.ExportDefinition;
import com.marklogic.datamovement.WriteHostBatcher;

public class ExportTest {
  private DataMovementManager moveMgr = DataMovementManager.newInstance();
  private static DatabaseClient client =
    DatabaseClientFactory.newClient("localhost", 8000, "admin", "admin", Authentication.DIGEST);
  private static GenericDocumentManager docMgr = client.newDocumentManager();
  private static String uri = "ExportTest_content.json";
  private static String collection = "ExportTest";
  private static HashMap<String, String> docs = new HashMap<>();
  static {
    docs.put("doc1.txt", "doc1");
    docs.put("doc2.xml", "<doc>doc2</doc>");
    docs.put("doc3.json", "{\"body\":\"doc3\"}");
  }
  private static String outputDir = "target/ExportTest/";

  @BeforeClass
  public static void beforeClass() {
    System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http.wire", "debug");
    for ( String uri : docs.keySet() ) {
      (new File(outputDir + uri)).delete();
    }
    (new File(outputDir)).delete();
  }

  @AfterClass
  public static void afterClass() {
    QueryManager queryMgr = client.newQueryManager();
    DeleteQueryDefinition deleteQuery = queryMgr.newDeleteDefinition();
    deleteQuery.setCollections(collection);
    queryMgr.delete(deleteQuery);
    client.release();
  }

  @Test
  public void testArgs() throws Exception {
    moveMgr.setClient(client);

    assertEquals( "Since the doc doesn't exist, docMgr.exists() should return null",
      docMgr.exists(uri), null );

    WriteHostBatcher writeBatcher = moveMgr.newWriteHostBatcher();
    moveMgr.startJob(writeBatcher);
    // a collection so we're only looking at docs related to this test
    DocumentMetadataHandle meta = new DocumentMetadataHandle()
      .withCollections(collection);
    // all the docs are one-word text docs
    for ( String uri : docs.keySet() ) {
      writeBatcher.addAs(uri, meta, docs.get(uri));
    }
    writeBatcher.flush();

    CtsQueryDefinition query = new ExportDefinitionTest.MockCtsQueryDefinition(
      "<cts:collection-query xmlns:cts=\"http://marklogic.com/cts\">" +
      "  <cts:uri>" + collection + "</cts:uri>" +
      "</cts:collection-query>"
    );
    ExportDefinition def = moveMgr.newExportDefinition()
      .withQueryFilter(query)
      .withOutputFilePath(outputDir);
    moveMgr.startJob(def);
    Thread.sleep(1000);
    for ( String uri : docs.keySet() ) {
      if ( uri.endsWith(".xml") ) {
        assertXMLEqual( "Exported file should match the file we wrote",
          HandleAccessor.contentAsString(new FileHandle(new File(outputDir + uri))),
          docs.get(uri)
        );
      } else {
        assertEquals( "Exported file should match the file we wrote",
          HandleAccessor.contentAsString(new FileHandle(new File(outputDir + uri))),
          docs.get(uri)
        );
      }
    }
  }
}


