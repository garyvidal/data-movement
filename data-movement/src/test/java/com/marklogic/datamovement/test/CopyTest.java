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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.Authentication;
import com.marklogic.client.query.CtsQueryDefinition;
import com.marklogic.datamovement.DataMovementManager;
import com.marklogic.datamovement.mlcp.CopyDefinition;

public class CopyTest {
  private DataMovementManager moveMgr = DataMovementManager.newInstance();
  private static DatabaseClient inputClient =
    DatabaseClientFactory.newClient("localhost", 8012, "java-unittest", "admin", "admin", Authentication.DIGEST);
  private static DatabaseClient outputClient =
    DatabaseClientFactory.newClient("localhost", 8000, "Documents", "admin", "admin", Authentication.DIGEST);
  private static String uri = "CopyTest_content.txt";
  private static String contents = "CopyTest content";

  @BeforeClass
  public static void beforeClass() {
    System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http.wire", "debug");
  }

  @AfterClass
  public static void afterClass() {
    inputClient.newDocumentManager().delete(uri);
    outputClient.newDocumentManager().delete(uri);
    inputClient.release();
    outputClient.release();
  }

  @Test
  public void testCopy() throws Exception {
    // verify that the file doesn't exist yet in the output db
    assertEquals( "Since the doc doesn't exist yet, exists() should return null",
      outputClient.newDocumentManager().exists(uri), null );

    // write a simple text file to the input db
    inputClient.newDocumentManager().writeAs(uri, contents);

    // perform the copy from input db to output db
    CtsQueryDefinition query = new ExportDefinitionTest.MockCtsQueryDefinition(
      "<cts:document-query xmlns:cts=\"http://marklogic.com/cts\">" +
      "  <cts:uri>" + uri + "</cts:uri>" +
      "</cts:document-query>"
    );
    CopyDefinition def = moveMgr.newCopyDefinition()
      .withInputClient(inputClient)
      .withOutputClient(outputClient)
      .withQueryFilter(query);
    moveMgr.startJob(def);

    Thread.sleep(1000);

    // validate that the file was copied to the output db
    assertEquals( outputClient.newDocumentManager().readAs(uri, String.class), contents );
  }
}
