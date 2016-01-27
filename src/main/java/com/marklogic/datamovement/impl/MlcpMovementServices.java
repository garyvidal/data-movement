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

import com.marklogic.contentpump.ContentPump;
import com.marklogic.contentpump.utilities.OptionsFileUtil;

import java.util.List;

public class MlcpMovementServices implements DataMovementServices {

  public JobTicket startJob(ImportDefinition def) {
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

  private void startMlcpJob(List<String> argList) throws Exception {
    String[] args = argList.toArray(new String[] {});
    String[] expandedArgs = OptionsFileUtil.expandArguments(args);

    ContentPump.runCommand(expandedArgs);
  }
}
