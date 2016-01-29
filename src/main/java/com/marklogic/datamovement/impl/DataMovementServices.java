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

import com.marklogic.datamovement.CopyDefinition;
import com.marklogic.datamovement.DeleteDefinition;
import com.marklogic.datamovement.ExportDefinition;
import com.marklogic.datamovement.ImportDefinition;
import com.marklogic.datamovement.ImportHostBatcher;
import com.marklogic.datamovement.JobReport;
import com.marklogic.datamovement.JobTicket;
import com.marklogic.datamovement.QueryHostBatcher;
import com.marklogic.datamovement.UpdateDefinition;

import com.marklogic.client.DatabaseClient;

public interface DataMovementServices<T extends DataMovementServices> {
  public T setClient(DatabaseClient client);
  public DatabaseClient getClient();

  public JobTicket startJob(ImportDefinition def);
  public JobTicket startJob(ExportDefinition def);
  public JobTicket startJob(CopyDefinition def);
  public JobTicket startJob(UpdateDefinition def);
  public JobTicket startJob(DeleteDefinition def);

  public JobTicket startJob(ImportHostBatcher batcher);

  public JobTicket startJob(QueryHostBatcher batcher);

  public JobReport getJobReport(JobTicket ticket);

  public void stopJob(JobTicket ticket);
}
