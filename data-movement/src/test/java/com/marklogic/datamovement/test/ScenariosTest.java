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

import static com.marklogic.client.DatabaseClientFactory.Authentication.BASIC;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilderFactory;

import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.io.DOMHandle;
import com.marklogic.datamovement.DataMovementManager;
import com.marklogic.datamovement.JobReport;
import com.marklogic.datamovement.JobTicket;
import com.marklogic.datamovement.WriteEvent;
import com.marklogic.datamovement.WriteHostBatcher;


public class ScenariosTest {
  @Test
  public void scenario1() {
  }

  /** 2. After some refactoring and testing, Deborah deploys her code to
   * dedicated acceptance and production clusters. In production, her code
   * needs to handle peak loads of 10M updates per day of documents roughly
   * 5KB each. Duane, her DBA has adequately sized the cluster, but Deborah
   * needs to be confident that she’s spreading the ingestion load such that
   * there are no hot spots and she can take advantage of the resources
   * available to increase throughput. She has instrumented her code, using
   * MarkLogic APIs, to get regularly updated measurements of number of
   * documents processed, number of bytes moved, warnings and errors, etc.
   */
  public void scenario2() throws Exception {
    DatabaseClient client = DatabaseClientFactory.newClient(
        "localhost", 8000, "user", "passwd", BASIC);
    OurJbossESBPlugin plugin = new OurJbossESBPlugin(client);
    plugin.acceptMessage(new Message());
  }

  private class Message {
    public Map<String, Object> getBody() throws Exception {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("uri", "http://marklogic.com/my/test/uri");
      Document document = 
        DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
      Element element = document.createElement("test");
      document.appendChild(element);
      map.put("content", document);
      return map;
    }
  }

  private class OurJbossESBPlugin {

    private int BATCH_SIZE = 1;
    private DataMovementManager moveMgr = DataMovementManager.newInstance();
    private JobTicket ticket;
    private WriteHostBatcher batcher;
    private Logger logger = Logger.getLogger(this.getClass().getName());

    public OurJbossESBPlugin(DatabaseClient client) {
      moveMgr.setClient(client);
      batcher = moveMgr.newWriteHostBatcher()
        .withJobName("OurJbossESBPlugin")
        .withBatchSize(BATCH_SIZE)
        // every time a batch is full, write it to the database via mlcp
        // this is the default, only included here to make it obvious
        //.onBatchFull( new MlcpBatchFullListener() )
        // log a summary report after each successful batch
        .onBatchSuccess( (hostClient, batch) ->  logger.info(getSummaryReport()) )
        .onBatchFailure(
            (hostClient, batch, throwable) -> {
            ArrayList<String> uris = new ArrayList<String>();
            for ( WriteEvent event : batch.getItems() ) {
            uris.add(event.getTargetUri());
            }
            logger.log(Level.WARNING, "FAILURE on batch:" + uris + "\n",
                throwable);
            }
            );
      ticket = moveMgr.startJob(batcher);
    }

    public void acceptMessage(Message message) throws Exception {
      String uri = (String) message.getBody().get("uri");
      Document xmlBody = (Document) message.getBody().get("content");
      // do processing and validation
      batcher.add(uri, new DOMHandle(xmlBody));
    }

    public String getSummaryReport() {
      JobReport report = moveMgr.getJobReport(ticket);
      return "batches: " + report.getSuccessBatchesCount() +
        ", docs: "       + report.getSuccessEventsCount() +
        ", bytes: "      + report.getBytesMoved() +
        ", failures: "   + report.getFailureEventsCount();
    }
  }
}
