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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.fail;

import com.marklogic.datamovement.Batch;
import com.marklogic.datamovement.CustomEvent;
import com.marklogic.datamovement.CustomJobReport;
import com.marklogic.datamovement.DataMovementManager;
import com.marklogic.datamovement.ForestBatcher;
import com.marklogic.datamovement.ForestBatchFailure;
import com.marklogic.datamovement.JobTicket;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import static com.marklogic.client.DatabaseClientFactory.Authentication.BASIC;
import com.marklogic.client.ResourceNotFoundException;
import com.marklogic.client.admin.ExtensionLibrariesManager;
import com.marklogic.client.document.DocumentPage;
import com.marklogic.client.document.DocumentRecord;
import com.marklogic.client.document.JSONDocumentManager;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.FileHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.StructuredQueryBuilder;
import com.marklogic.client.query.StructuredQueryDefinition;
import static com.marklogic.client.query.StructuredQueryBuilder.Operator;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Calendar;
import javax.xml.bind.DatatypeConverter;
import javax.xml.namespace.QName;

public class LegalHoldsTest {
  private static String directory = "/LegalHoldsTest/";
  private static JSONDocumentManager docMgr;
  private static ObjectMapper mapper = new ObjectMapper();

  @BeforeClass
  public static void beforeClass() {
      Common.connectEval();
      docMgr = Common.client.newJSONDocumentManager();
      //System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http.wire", "debug");
  }
  @AfterClass
  public static void afterClass() {
      Common.release();
  }

  @Test
  /** 10.  Duane is charged with implementing his organization's TTL policy,
   * such that after 7 years all transactional data must be deleted. He
   * performs a batch delete every month. Each batch is typically in the
   * millions of documents and has very selective criteria, e.g. "All
   * instruments of type X, originating in jurisdiction Y, having a state of
   * of 'resolved', 'closed', or 'finalized' that have not been marked 'hold'
   * or is not referenced by a document marked 'hold' that has not been
   * updated in the previous 7 years." He first does a dry run and gets a
   * report of the candidate documents. He has some code that sanity checks
   * those numbers against accounting data he gets from an external system.
   * If those numbers add up he runs the delete. Because of the size, he
   * knows his delete can't run in an atomic transaction. However, he's able
   * to specify that his export run at a single, consistent timestamp, thus
   * ignoring subsequent updates. After a successful run, he gets back a
   * report confirming that he deleted what he intended. In the case of an
   * unsuccessful delete he gets a detailed report of which documents were
   * deleted and why his job failed.
   */
  public void scenario10() throws Exception {
    setup();
    Calendar date = Calendar.getInstance();
    // change date to now minus seven years
    date.roll(Calendar.YEAR, -7);
    String sevenYearsAgo = DatatypeConverter.printDateTime(date);
    StructuredQueryBuilder sqb = new StructuredQueryBuilder();
    QueryDefinition query =
      sqb.and(
          sqb.value(sqb.element("type"), "X"),
          sqb.value(sqb.element("originJurisdiction"), "Y"),
          sqb.value(sqb.element("state"), "resolved", "closed", "finalized"),
          sqb.not( sqb.value(sqb.element("hold"), "true") ),
          sqb.range(
            sqb.element("lastModified"),
            "xs:dateTime", new String[0],
            Operator.LE, sevenYearsAgo
            )
          );
    QueryManager queryMgr = Common.client.newQueryManager();
    long start = 1;
    DocumentPage results = docMgr.search(query, start);
    ArrayNode uris = mapper.createArrayNode();
    for ( DocumentRecord doc : results ) {
      uris.add( doc.getUri() );
    }
    String urisToDelete =
      Common.client.newServerEval()
      .modulePath("/ext" + directory + "filterUrisReferencedByHolds.sjs")
      .addVariable("uris", new JacksonHandle(uris))
      .evalAs(String.class);
    docMgr.delete(urisToDelete.split(","));
    try {
      docMgr.readAs(directory + "file1.json", String.class);
      docMgr.readAs(directory + "file2.json", String.class);
      docMgr.readAs(directory + "file3.json", String.class);
      docMgr.readAs(directory + "file4.json", String.class);
    } catch (ResourceNotFoundException e) {
      fail("missing a file that should still be there: " + e);
    }
    try {
      docMgr.readAs(directory + "file5.json", String.class);
      fail("found file5.json which should not be there");
    } catch (ResourceNotFoundException e) {
    }
  }

  private void setup() throws Exception {
    installModule();
    uploadData();
  }

  private void installModule() throws Exception {
    DatabaseClient adminClient = Common.newAdminClient();

    // get a modules manager
    ExtensionLibrariesManager libsMgr = adminClient
      .newServerConfigManager().newExtensionLibrariesManager();

    // write server-side javascript module file to the modules database
    libsMgr.write("/ext" + directory + "filterUrisReferencedByHolds.sjs",
        new FileHandle(new File("src/test/resources/legal_holds/filterUrisReferencedByHolds.sjs"))
        .withFormat(Format.TEXT));
  }

  private void uploadData() throws Exception {
    File folder = new File("src/test/resources/legal_holds/data");
    for ( Path path: Files.newDirectoryStream(folder.toPath(), "*.json") ) {
      File file = path.toFile();
      docMgr.write(directory + file.getName(), new FileHandle(file));
    }
  }
}
