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

import java.util.Map;

public interface JobDefinition<T extends JobDefinition> {
  public T jobName(String jobName);
  public T optionsFile(String optionsFilePath);
  public T threadCount(int threadCount);
  public T batchSize(long batchSize);
  public T database(String database);
  public T mode(JobDefinition.Mode mode);
  public T transactionSize(int transactionSize);
  public T setOption(String name, String value);
  public T setOptions(Map<String, String> options);

  public enum Mode { LOCAL, DISTRIBUTED };
}
