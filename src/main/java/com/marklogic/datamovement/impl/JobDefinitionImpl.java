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

import com.marklogic.datamovement.JobDefinition;

import com.marklogic.client.DatabaseClient;

import java.util.Hashtable;
import java.util.Map;

public class JobDefinitionImpl<T extends JobDefinition>
  implements JobDefinition<T>
{
  private String jobName;
  private String optionsFilePath;
  private int threadCount;
  private long batchSize;
  private String database;
  private JobDefinition.Mode mode;
  private int transactionSize;
  private Map<String, String> options = new Hashtable<String, String>();

  public JobDefinitionImpl() {}

  public T jobName(String jobName) {
    this.jobName = jobName;
    return (T) this;
  }

  public T optionsFile(String optionsFilePath) {
    this.optionsFilePath = optionsFilePath;
    return (T) this;
  }

  public T threadCount(int threadCount) {
    this.threadCount = threadCount;
    return (T) this;
  }

  public T batchSize(long batchSize) {
    this.batchSize = batchSize;
    return (T) this;
  }

  public T database(String database) {
    this.database = database;
    return (T) this;
  }

  public T mode(JobDefinition.Mode mode) {
    this.mode = mode;
    return (T) this;
  }

  public T transactionSize(int transactionSize) {
    this.transactionSize = transactionSize;
    return (T) this;
  }

  public T setOption(String name, String value) {
    this.options.put(name, value);
    return (T) this;
  }

  public T setOptions(Map<String, String> options) {
    this.options.putAll(options);
    return (T) this;
  }
}
