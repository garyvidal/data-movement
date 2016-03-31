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

import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Test;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.Authentication;
import com.marklogic.contentpump.ConfigConstants;
import com.marklogic.datamovement.DataMovementManager;
import com.marklogic.datamovement.mlcp.JobDefinition;
import com.marklogic.datamovement.JobTicket;
import com.marklogic.datamovement.JobTicket.JobType;
import com.marklogic.datamovement.impl.JobDefinitionImpl;
import com.marklogic.datamovement.impl.MlcpUtil;

public class JobDefinitionTest {
  private static DataMovementManager moveMgr = DataMovementManager.newInstance();
  private static String host = "localhost";
  private static int    port = 8000;
  private static String user = "admin";
  private static String pass = "admin";
  private static String db   = "java-unittest";
  private static DatabaseClient client =
    DatabaseClientFactory.newClient(host, port, db, user, pass, Authentication.DIGEST);

  @AfterClass
  public static void afterClass() {
    //docMgr.delete(uri);
    client.release();
  }

  @Test
  public void testOptions() throws Exception {
    moveMgr.setClient(client);
    // we'll just use ImportDefinition to test since there's no 
    // generic JobDefinition constructor on DataMovementManager
    // we need JobDefinitionImpl because getMlcpArgs isn't on JobDefinition
    JobDefinitionImpl<?> def = (JobDefinitionImpl<?>) moveMgr.newImportDefinition();
    ArrayList<String> expectedMlcpParams = new ArrayList<String>();
    expectedMlcpParams.add(JobTicket.JobType.IMPORT.toString());

    // the following are set by moveMgr.setClient(client) which passes the client to JobDefinition
    expectedMlcpParams.add("-" + ConfigConstants.HOST); expectedMlcpParams.add(host);
    expectedMlcpParams.add("-" + ConfigConstants.PORT); expectedMlcpParams.add(String.valueOf(port));
    expectedMlcpParams.add("-" + ConfigConstants.USERNAME); expectedMlcpParams.add(user);
    expectedMlcpParams.add("-" + ConfigConstants.PASSWORD); expectedMlcpParams.add(pass);
    expectedMlcpParams.add("-" + ConfigConstants.DATABASE); expectedMlcpParams.add(db);

    def.withBatchSize(23);
    expectedMlcpParams.add("-" + ConfigConstants.BATCH_SIZE); expectedMlcpParams.add("23");

    def.withConf("conf.txt");
    expectedMlcpParams.add("-conf"); expectedMlcpParams.add("conf.txt");

    def.withMode(JobDefinition.Mode.LOCAL);
    expectedMlcpParams.add("-" + ConfigConstants.MODE);
    expectedMlcpParams.add(JobDefinition.Mode.LOCAL.toString().toLowerCase());

    def.withOptionsFile("options.txt");
    expectedMlcpParams.add("-" + ConfigConstants.OPTIONS_FILE); expectedMlcpParams.add("options.txt");

    def.withThreadCount(5);
    expectedMlcpParams.add("-" + ConfigConstants.THREAD_COUNT); expectedMlcpParams.add("5");

    def.withThreadCountPerSplit(8);
    expectedMlcpParams.add("-" + ConfigConstants.THREADS_PER_SPLIT); expectedMlcpParams.add("8");

    def.withTransactionSize(4);
    expectedMlcpParams.add("-" + ConfigConstants.TRANSACTION_SIZE); expectedMlcpParams.add("4");

    List<String> args = def.getMlcpArgs(JobType.IMPORT);
    // add them after the first arg (IMPORT)
    args.addAll( 1, MlcpUtil.argsForClient(client) );
    assertEquals(expectedMlcpParams, args);
  }
}


