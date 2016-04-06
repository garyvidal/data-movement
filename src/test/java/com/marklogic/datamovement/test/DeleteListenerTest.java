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
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.query.StructuredQueryBuilder;
import com.marklogic.datamovement.DataMovementManager;
import com.marklogic.datamovement.DeleteListener;
import com.marklogic.datamovement.WriteHostBatcher;

public class DeleteListenerTest {
  private static DataMovementManager moveMgr = DataMovementManager.newInstance();
  private static DatabaseClient client =
    DatabaseClientFactory.newClient("localhost", 8000, "Documents", "admin", "admin", Authentication.DIGEST);
  private static String collection = "DeleteListenerTest";
  private static String docContents = "doc contents";

  @BeforeClass
  public static void beforeClass() {
    moveMgr.setClient(client);
    System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http.wire", "debug");
  }

  @AfterClass
  public static void afterClass() {
  }

  @Test
  public void testMassDelete() throws Exception {
    // write 100 simple text files to the db
    DocumentMetadataHandle meta = new DocumentMetadataHandle()
      .withCollections(collection);
    WriteHostBatcher batcher = moveMgr.newWriteHostBatcher();
    moveMgr.startJob(batcher);
    String[] uris = new String[100];
    for ( int i=0; i < 100; i++ ) {
      uris[i] = "doc" + i + ".txt";
      batcher.addAs(uris[i], meta, docContents);
    }
    batcher.flush();

    // verify that the files made it to the db
    assertEquals( "There should be 100 documents in the db",
      100, client.newDocumentManager().read(uris).size() );

    moveMgr.startJob(
      moveMgr.newQueryHostBatcher(
        new StructuredQueryBuilder().collection(collection)
      )
      .onUrisReady(new DeleteListener())
    );


    // validate that the docs were deleted
    assertEquals( "There should be 0 documents in the db",
      100, client.newDocumentManager().read(uris).size() );
  }
}

