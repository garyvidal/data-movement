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
package com.marklogic.datamovement;

import com.marklogic.client.DatabaseClient;

public class DataMovementManager {
  private DatabaseClient client;

  private DataMovementManager() {
    // TODO: implement
  }

  public static DataMovementManager newInstance() {
    return new DataMovementManager();
  }

  public DataMovementManager setClient(DatabaseClient client) {
    this.client = client;
    return this;
  }

  public DatabaseClient getClient() {
    return client;
  }

  public JobTicket startJob(ImportDefinition def) {
    // TODO: implement
    return null;
  }
  public JobTicket startJob(ExportDefinition def) {
    // TODO: implement
    return null;
  }
  public JobTicket startJob(CopyDefinition def) {
    // TODO: implement
    return null;
  }
  public JobTicket startJob(UpdateDefinition def) {
    // TODO: implement
    return null;
  }
  public JobTicket startJob(DeleteDefinition def) {
    // TODO: implement
    return null;
  }

  public JobTicket startJob(ImportHostBatcher batcher) {
    // TODO: implement
    return null;
  }

  public JobTicket startJob(QueryHostBatcher batcher) {
    // TODO: implement
    return null;
  }

  public JobReport getJobReport(JobTicket ticket) {
    // TODO: implement
    return null;
  }

  public void stopJob(JobTicket ticket) {
    // TODO: implement
  }

  public ImportHostBatcher newImportHostBatcher() {
    //return new ImportHostBatcherImpl();
    // TODO: implement
    return null;
  }

  public ImportHostBatcher newQueryHostBatcher() {
    //return new QueryHostBatcherImpl();
    // TODO: implement
    return null;
  }

  public ImportDefinition newImportDefinition() {
    //return new ImportDefinitionImpl();
    // TODO: implement
    return null;
  }

  public ExportDefinition newExportDefinition() {
    //return new ExportDefinitionImpl();
    // TODO: implement
    return null;
  }

  public CopyDefinition newCopyDefinition() {
    //return new CopyDefinitionImpl();
    // TODO: implement
    return null;
  }

  public UpdateDefinition newUpdateDefinition() {
    //return new UpdateDefinitionImpl();
    // TODO: implement
    return null;
  }

  public DeleteDefinition newDeleteDefinition() {
    //return new DeleteDefinitionImpl();
    // TODO: implement
    return null;
  }

  public ModuleTransform newModuleTransform(String modulePath, String functionName) {
    //return new ModuleTransformImpl(modulePath, functionName);
    // TODO: implement
    return null;
  }

  public ModuleTransform newModuleTransform(String modulePath, String functionName, String functionNamespace) {
    //return new ModuleTransformImpl(modulePath, functionName, functionNamespace);
    // TODO: implement
    return null;
  }

  public AdhocTransform newAdhocTransform() {
    //return new AdhocTransformImpl();
    // TODO: implement
    return null;
  }

  public ForestConfiguration forestConfig() {
    // TODO: implement
    return null;
  }
}
