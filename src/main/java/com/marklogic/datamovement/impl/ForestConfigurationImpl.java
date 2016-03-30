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
package com.marklogic.datamovement.impl;

import java.util.Random;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.datamovement.Forest;
import com.marklogic.datamovement.ForestConfiguration;

public class ForestConfigurationImpl implements ForestConfiguration {
  private DatabaseClient primaryClient;
  private Forest[] forests;
  private AssignmentPolicy assignmentPolicy;

  public ForestConfigurationImpl(DatabaseClient primaryClient, Forest[] forests) {
    this.primaryClient = primaryClient;
    this.forests = forests;
  }

  public Forest[] listForests() {
    return forests;
  }

  public Forest assign(String uri) {
    // TODO: replace this random with real forest assignment
    int forestIdx = new Random().nextInt(forests.length);
    return forests[forestIdx];
  }

  public DatabaseClient getForestClient(Forest forest) {
    DatabaseClient client = DatabaseClientFactory.newClient(
      forest.getHostName(),
      primaryClient.getPort(),
      forest.getDatabaseName(),
      primaryClient.getUser(),
      primaryClient.getPassword(),
      primaryClient.getAuthentication(),
      forest.getForestName(),
      primaryClient.getSSLContext(),
      primaryClient.getSSLHostnameVerifier()
    );
    return client;
  }

  public AssignmentPolicy getAssignmentPolicy() {
    return assignmentPolicy;
  }

  public void setAssignmentPolicy(AssignmentPolicy policy) {
    this.assignmentPolicy = policy;
  }
}
