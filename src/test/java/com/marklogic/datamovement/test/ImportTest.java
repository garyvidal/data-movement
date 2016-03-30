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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.Authentication;
import com.marklogic.client.document.GenericDocumentManager;
import com.marklogic.client.io.FileHandle;
import com.marklogic.client.io.Format;
import com.marklogic.datamovement.DataMovementManager;
import com.marklogic.datamovement.ImportDefinition;

public class ImportTest {
  private DataMovementManager moveMgr = DataMovementManager.newInstance();
  private static DatabaseClient client =
    DatabaseClientFactory.newClient("localhost", 8000, "admin", "admin", Authentication.DIGEST);
  private static GenericDocumentManager docMgr = client.newDocumentManager();
  private static String uri = "ImportTest_content.json";
  private static String module = "ImportTest_transform.sjs";
  private static String moduleFunction = "ImportTest_transform_function";

  @BeforeClass
  public static void beforeClass() {
    System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http.wire", "debug");
    installModule();
  }

  @AfterClass
  public static void afterClass() {
    docMgr.delete(uri);
    client.release();
  }

  public static void installModule() {
    client.newServerConfigManager().newExtensionLibrariesManager().write(
      "/ext/" + module, new FileHandle(new File("src/test/resources/" + module)).withFormat(Format.TEXT));
  }

  @Test
  public void testArgs() throws Exception {
    moveMgr.setClient(client);

    assertEquals( "Since the doc doesn't exist, docMgr.exists() should return null",
      docMgr.exists(uri), null );

    ImportDefinition<?> def = moveMgr.newImportDefinition()
      .withInputFilePath("src/test/resources/" + uri)
      .withTransform(
        moveMgr.newModuleTransform("/ext/" + module, moduleFunction)
//          can't do this yet because of bug 37763
//          .addParameter("newValue", "test2")
      )
      // temporary work-around
      .withOption("transform_param", "test2")
      .withOutputUriReplacement("/.*", uri);
    moveMgr.startJob(def);
    Thread.sleep(1000);
    assertEquals( "the transform should have changed testProperty to 'test2'",
      ((JsonNode) docMgr.readAs(uri, JsonNode.class)).get("testProperty").textValue(), "test2" );
  }
}

