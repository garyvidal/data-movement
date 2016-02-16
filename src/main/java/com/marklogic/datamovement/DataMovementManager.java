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
import com.marklogic.datamovement.impl.ImportDefinitionImpl;
import com.marklogic.datamovement.impl.ImportHostBatcherImpl;
import com.marklogic.datamovement.impl.MlcpMovementServices;
import com.marklogic.datamovement.impl.DataMovementServices;

public class DataMovementManager {
  private DatabaseClient client;
  private DataMovementServices service = new MlcpMovementServices();

  private DataMovementManager() {
    // TODO: implement
  }

  public static DataMovementManager newInstance() {
    return new DataMovementManager();
  }

  public DataMovementManager setClient(DatabaseClient client) {
    this.client = client;
    service.setClient(client);
    return this;
  }

  public DatabaseClient getClient() {
    return client;
  }

  public JobTicket startJob(ImportDefinition def) {
    return service.startJob(def);
  }
  public JobTicket startJob(ExportDefinition def) {
    return service.startJob(def);
  }
  public JobTicket startJob(CopyDefinition def) {
    return service.startJob(def);
  }
  public JobTicket startJob(UpdateDefinition def) {
    return service.startJob(def);
  }
  public JobTicket startJob(DeleteDefinition def) {
    return service.startJob(def);
  }

  public JobTicket startJob(ImportHostBatcher batcher) {
    return service.startJob(batcher);
  }

  public JobTicket startJob(QueryHostBatcher batcher) {
    return service.startJob(batcher);
  }

  public JobReport getJobReport(JobTicket ticket) {
    return service.getJobReport(ticket);
  }

  public void stopJob(JobTicket ticket) {
    service.stopJob(ticket);
  }

  public ImportHostBatcher newImportHostBatcher() {
    return new ImportHostBatcherImpl();
  }

  public ImportHostBatcher newQueryHostBatcher() {
    //return new QueryHostBatcherImpl();
    // TODO: implement
    return null;
  }

  public ImportDefinition<?> newImportDefinition() {
    return new ImportDefinitionImpl();
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
    return new ModuleTransformImpl(modulePath, functionName, null);
  }

  public ModuleTransform newModuleTransform(String modulePath, String functionName, String functionNamespace) {
    return new ModuleTransformImpl(modulePath, functionName, functionNamespace);
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
