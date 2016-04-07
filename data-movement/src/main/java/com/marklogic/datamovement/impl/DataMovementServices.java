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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.eval.EvalResult;
import com.marklogic.client.eval.EvalResultIterator;
import com.marklogic.contentpump.ContentPump;
import com.marklogic.contentpump.utilities.OptionsFileUtil;
import com.marklogic.datamovement.mlcp.CopyDefinition;
import com.marklogic.datamovement.DataMovementException;
import com.marklogic.datamovement.mlcp.ExportDefinition;
import com.marklogic.datamovement.mlcp.ImportDefinition;
import com.marklogic.datamovement.mlcp.JobDefinition;
import com.marklogic.datamovement.JobReport;
import com.marklogic.datamovement.JobTicket;
import com.marklogic.datamovement.JobTicket.JobType;
import com.marklogic.datamovement.QueryHostBatcher;
import com.marklogic.datamovement.WriteHostBatcher;

public class DataMovementServices {
  private DatabaseClient client;

  public DatabaseClient getClient() {
    return client;
  }

  public DataMovementServices setClient(DatabaseClient client) {
    this.client = client;
    return this;
  }

  public ForestConfigurationImpl readForestConfig() {
    ArrayList<ForestImpl> forests = new ArrayList<>();
    // TODO: replace this eval with a more permanent solution that doesn't require the eval privileges
    EvalResultIterator results = client.newServerEval()
      .javascript(
        "xdmp.arrayValues(xdmp.databaseForests(xdmp.database()).toArray()" +
        "  .map(id => {return {" +
        "    id:id," +
        "    database: xdmp.database()," +
        "    name: xdmp.forestName(id)," +
        "    updatesAllowed: xdmp.forestUpdatesAllowed(id)," +
        "    host: xdmp.hostName(xdmp.forestHost(id))}}))")
      .eval();
    for ( EvalResult result : results ) {
      JsonNode forestNode = result.getAs(JsonNode.class);
      String host = forestNode.get("host").asText();
      String database = forestNode.get("database").asText();
      String id = forestNode.get("id").asText();
      String name = forestNode.get("name").asText();
      boolean isUpdateable = "all".equals(forestNode.get("updatesAllowed").asText());
      boolean isDeleteOnly = false; // TODO: get this for real after we start using a REST endpoint
      long fragmentCount = 0;       // TODO: get this for real after we start using a REST endpoint
      forests.add(new ForestImpl(host, database, name, id, isUpdateable, isDeleteOnly, fragmentCount));
    }

    return new ForestConfigurationImpl(client, forests.toArray(new ForestImpl[forests.size()]));
  }

  public JobTicket startJob(ImportDefinition<?> def) {
    return startMlcpJob(def, JobTicket.JobType.IMPORT);
  }

  public JobTicket startJob(ExportDefinition def) {
    return startMlcpJob(def, JobTicket.JobType.EXPORT);
  }
  public JobTicket startJob(CopyDefinition def) {
    return startMlcpJob(def, JobTicket.JobType.COPY);
  }

  public JobTicket startJob(WriteHostBatcher batcher) {
    // TODO: implement job tracking
    return new JobTicketImpl(generateJobId(), JobTicket.JobType.IMPORT_HOST_BATCHER);
  }

  public JobTicket startJob(QueryHostBatcher batcher) {
    // TODO: implement job tracking
    ((QueryHostBatcherImpl) batcher).start();
    return new JobTicketImpl(generateJobId(), JobTicket.JobType.QUERY_HOST_BATCHER);
  }

  public JobReport getJobReport(JobTicket ticket) {
    // TODO: implement
    return null;
  }

  public void stopJob(JobTicket ticket) {
    // TODO: implement
  }

  private JobTicketImpl startMlcpJob(JobDefinition<?> def, JobType jobType) {
    String jobId = generateJobId();
    List<String> argList = ((JobDefinitionImpl<?>) def).getMlcpArgs(jobType);
    argList.addAll( MlcpUtil.argsForClient(client) );
System.out.println("DEBUG: [DataMovementServices] argList =[" + String.join(" ", argList)  + "]");
System.out.println("DEBUG: [DataMovementServices] argList.size()=[" + argList.size()  + "]");
    String[] args = argList.toArray(new String[argList.size()]);
    try {
      String[] expandedArgs = OptionsFileUtil.expandArguments(args);
      ContentPump.runCommand(expandedArgs);
    } catch(Exception e) {
      throw new DataMovementException("error expanding arguments: " + e.toString(), e);
    }
    return new JobTicketImpl(jobId, jobType);
  }

  private String generateJobId() {
    return UUID.randomUUID().toString();
  }
}
