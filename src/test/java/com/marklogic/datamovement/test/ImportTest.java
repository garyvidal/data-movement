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

import org.junit.Test;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.marklogic.datamovement.DataMovementManager;
import com.marklogic.datamovement.ImportDefinition;
import com.marklogic.datamovement.JobTicket;
import com.marklogic.datamovement.ModuleTransform;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.Authentication;
import com.marklogic.client.document.DocumentManager;

import com.marklogic.contentpump.ConfigConstants;

import java.util.ArrayList;
import java.util.List;

public class ImportTest {
  private DataMovementManager moveMgr = DataMovementManager.newInstance();
  private static DatabaseClient client =
    DatabaseClientFactory.newClient("localhost", 8000, "admin", "admin", Authentication.DIGEST);
  private static DocumentManager docMgr = client.newDocumentManager();
  private static String uri = "pom.xml";

  @BeforeClass
  public static void beforeClass() {
    //System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http.wire", "debug");
    docMgr.delete(uri);

  }

  @AfterClass
  public static void afterClass() {
    docMgr.delete(uri);
    client.release();
  }

  @Test
  public void testArgs() throws Exception {
    moveMgr.setClient(client);

    assertTrue( docMgr.exists(uri) == null );

    ImportDefinition def = moveMgr.newImportDefinition()
      .inputFilePath(uri)
      .outputUriReplace("/.*", uri);
    JobTicket ticket = moveMgr.startJob(def);
    assertTrue( docMgr.exists(uri) != null );
  }
}

