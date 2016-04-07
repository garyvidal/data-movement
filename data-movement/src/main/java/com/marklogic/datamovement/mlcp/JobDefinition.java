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
package com.marklogic.datamovement.mlcp;

import java.util.Map;

public interface JobDefinition<T extends JobDefinition> {
  public T withJobName(String jobName);
  public String getJobName();
  public T withBatchSize(long batchSize);
  public long getBatchSize();
  public T withConf(String filepath);
  public String getConf();
  public T withMode(JobDefinition.Mode mode);
  public JobDefinition.Mode getMode();
  public T withOptionsFile(String optionsFilePath);
  public String getOptionsFile();
  public T withThreadCount(int threadCount);
  public int getThreadCount();
  public T withThreadCountPerSplit(int threadCount);
  public int getThreadCountPerSplit();
  public T withTransactionSize(int transactionSize);
  public int getTransactionSize();
  public T withOption(String name, String value);
  public T withOptions(Map<String,String> options);
  public String getOption(String name);
  public Map<String,String> getOptions();
  public T removeOption(String name);

  public enum Mode { LOCAL, DISTRIBUTED };
}
