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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.Authentication;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.document.DocumentManager;
import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.query.StructuredQueryBuilder;
import com.marklogic.datamovement.DataMovementManager;
import com.marklogic.datamovement.ExportToWriterListener;
import com.marklogic.datamovement.QueryHostBatcher;
import com.marklogic.datamovement.WriteHostBatcher;

public class ExportToWriterListenerTest {
  private static DataMovementManager moveMgr = DataMovementManager.newInstance();
  private static DatabaseClient client =
    DatabaseClientFactory.newClient("localhost", 8012, "admin", "admin", Authentication.DIGEST);
  private static String collection = "ExportToWriterListenerTest";
  private static String docContents = "doc contents";
  private static String outputFile = "target/ExportToWriterListenerTest.txt";

  @BeforeClass
  public static void beforeClass() {
    moveMgr.setClient(client);
    System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http.wire", "debug");
  }

  @AfterClass
  public static void afterClass() {
  }

  @Test
  public void testMassExportToWriter() throws Exception {
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
      client.newDocumentManager().read(uris).size(), 100 );

    final AtomicInteger i = new AtomicInteger();
    QueryDefinition query = new StructuredQueryBuilder().collection(collection);
    FileWriter writer = new FileWriter(outputFile);
    try {
      ExportToWriterListener exportListener = new ExportToWriterListener(writer)
        .withRecordSuffix("\n")
        .withMetadataCategory(DocumentManager.Metadata.COLLECTIONS)
        .onGenerateOutput(
          record -> {
            i.incrementAndGet();
            String collection = record.getMetadata(new DocumentMetadataHandle()).getCollections().iterator().next();
            String contents = record.getContentAs(String.class);
            return collection + "," + contents;
          }
        );

      QueryHostBatcher queryJob =
        moveMgr.newQueryHostBatcher(query)
          .withThreadCount(2)
          .withBatchSize(100)
          .onUrisReady(exportListener)
          .onQueryFailure( (client, throwable) -> throwable.printStackTrace() );
      moveMgr.startJob( queryJob );

      // wait for the export to finish
      boolean finished = queryJob.awaitTermination(3, TimeUnit.MINUTES);
      if ( finished == false ) {
        throw new IllegalStateException("ERROR: Job did not finish within three minutes");
      }
    } finally {
      writer.close();
    }

    // validate that the docs were exported
    FileReader fileReader = new FileReader(outputFile);
    try {
      BufferedReader reader = new BufferedReader(fileReader);
      try {
          int lines = 0;
          while ( reader.readLine() != null ) lines++;
          assertEquals( "There should be 100 lines in the output file", 100, lines );
      } finally {
        reader.close();
      }
    } finally {
      fileReader.close();
    }
  }
}
