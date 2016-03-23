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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.Authentication;
import com.marklogic.datamovement.DataMovementManager;
import com.marklogic.datamovement.Forest;
import com.marklogic.datamovement.ForestConfiguration;

public class ForestConfigTest {
  private DataMovementManager moveMgr = DataMovementManager.newInstance();
  private static DatabaseClient client =
    DatabaseClientFactory.newClient("localhost", 8012, "admin", "admin", Authentication.DIGEST);

  @BeforeClass
  public static void beforeClass() {
  }

  @AfterClass
  public static void afterClass() {
    client.release();
  }

  @Test
  public void testArgs() throws Exception {
    moveMgr.setClient(client);
    String defaultUser = client.getUser();
    String defaultPass = client.getPassword();
    Authentication defaultAuth = client.getAuthentication();
    ForestConfiguration forestConfig = moveMgr.readForestConfig();
    Forest[] forests = forestConfig.listForests();
    String defaultDatabase = forests[0].getDatabaseName();
    assertEquals(3, forests.length);
    for ( Forest forest : forests ) {
      DatabaseClient forestClient = forestConfig.getForestClient(forest);
      // not all forests for a database are on the same host, so all we
      // can check is that the hostname is not null
      assertNotNull(forest.getHostName());
      assertEquals(defaultUser, forestClient.getUser());
      assertEquals(defaultPass, forestClient.getPassword());
      // not all hosts have the original REST server, but all hosts have the uber port
      assertEquals(8000, forestClient.getPort());
      assertEquals(defaultDatabase, forest.getDatabaseName());
      assertEquals(defaultAuth, forestClient.getAuthentication());
      assertEquals(forest.getForestName(), forestClient.getForestName());
      assertEquals(true, forest.isUpdateable());
      assertEquals(false, forest.isDeleteOnly());
      long count = forest.getFragmentCount();
      forest.increaseFragmentCount(5);
      assertEquals(count + 5, forest.getFragmentCount());
      forest.decreaseFragmentCount(10);
      assertEquals(count - 5, forest.getFragmentCount());
      if ( ! "java-unittest-1".equals(forest.getForestName()) &&
           ! "java-unittest-2".equals(forest.getForestName()) &&
           ! "java-unittest-3".equals(forest.getForestName()) ) {
        fail("Unexpected forestName \"" + forest.getForestName() + "\"");
      }
    }
  }
}
