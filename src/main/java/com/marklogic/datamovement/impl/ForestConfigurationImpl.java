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

import com.marklogic.client.DatabaseClient;
import com.marklogic.datamovement.Forest;
import com.marklogic.datamovement.ForestConfigChangeListener;
import com.marklogic.datamovement.ForestConfiguration;

public class ForestConfigurationImpl implements ForestConfiguration {
  private long updateInterval;
  private AssignmentPolicy assignmentPolicy;

  public ForestConfigurationImpl() {
  }

  public Forest[] listForests() {
    return new Forest[] {
      new ForestImpl()
    };
  }

  public Forest assign(String uri) {
    return new ForestImpl();
  }

  public DatabaseClient getForestClient(Forest forest) {
    // TODO: implement
    return null;
  }

  public ForestConfiguration onConfigChange(ForestConfigChangeListener listener) {
    // TODO: implement
    return null;
  }

  public long getUpdateInterval() {
    return updateInterval;
  }

  public ForestConfigurationImpl setUpdateInterval(long interval) {
    this.updateInterval = interval;
    return this;
  }

  public AssignmentPolicy getAssignmentPolicy() {
    return assignmentPolicy;
  }

  public void setAssignmentPolicy(AssignmentPolicy policy) {
    this.assignmentPolicy = policy;
  }
}
