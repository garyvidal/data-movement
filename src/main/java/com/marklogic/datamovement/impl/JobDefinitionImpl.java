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

import com.marklogic.contentpump.ConfigConstants;

import java.util.LinkedHashMap;
import java.util.Map;

public class JobDefinitionImpl<T extends JobDefinition>
  implements JobDefinition<T>
{
  private String jobName;
  private String optionsFilePath;
  private int threadCount;
  private int threadCountPerSplit;
  private long batchSize;
  private String conf;
  private JobDefinition.Mode mode;
  private int transactionSize;
  private Map<String, String> options = new LinkedHashMap<String, String>();

  public JobDefinitionImpl() {}

  public T jobName(String jobName) {
    this.jobName = jobName;
    return (T) this;
  }

  public T optionsFile(String optionsFilePath) {
    this.optionsFilePath = optionsFilePath;
    return setOption(ConfigConstants.OPTIONS_FILE, optionsFilePath);
  }

  public T threadCount(int threadCount) {
    this.threadCount = threadCount;
    return setOption(ConfigConstants.THREAD_COUNT, String.valueOf(threadCountPerSplit));
  }

  public T threadCountPerSplit(int threadCount) {
    this.threadCountPerSplit = threadCountPerSplit;
    return setOption(ConfigConstants.THREADS_PER_SPLIT, String.valueOf(threadCountPerSplit));
  }

  public T batchSize(long batchSize) {
    this.batchSize = batchSize;
    return setOption(ConfigConstants.BATCH_SIZE, String.valueOf(batchSize));
  }

  public T conf(String filepath) {
    this.conf = filepath;
    return setOption("conf", filepath);
  }

  public T mode(JobDefinition.Mode mode) {
    this.mode = mode;
    return setOption(ConfigConstants.MODE, mode.toString().toLowerCase());
  }

  public T transactionSize(int transactionSize) {
    this.transactionSize = transactionSize;
    return setOption(ConfigConstants.TRANSACTION_SIZE, String.valueOf(transactionSize));
  }

  /** All option names must match mlcp comman-line options (without the preceeding hyphen).
   */
  public synchronized T setOption(String name, String value) {
    checkMlcpOptionName(name);
    this.options.put(name, value);
    return (T) this;
  }

  /** All option names must match mlcp comman-line options (without the preceeding hyphen).
   */
  public synchronized T setOptions(Map<String, String> options) {
    if ( options == null ) return (T) this;
    for ( String name : options.keySet() ) {
      checkMlcpOptionName(name);
    }
    this.options.putAll(options);
    return (T) this;
  }

  /** All option names must match mlcp comman-line options (without the preceeding hyphen).
   */
  public String getOption(String name) {
    checkMlcpOptionName(name);
    return this.options.get(name);
  }

  /** Return all options set on this instance.
   */
  public Map<String, String> getOptions() {
    return options;
  }

  /** All option names must match mlcp comman-line options (without the preceeding hyphen).
   */
  public synchronized T removeOption(String name) {
    checkMlcpOptionName(name);
    this.options.remove(name);
    return (T) this;
  }

  private void checkMlcpOptionName(String name) {
    if ( name == null ) throw new IllegalArgumentException("option name must not be null");
    try {
      if ( ConfigConstants.class.getDeclaredField(name.toUpperCase()) != null ||
           // for some reason "conf" is missing from ConfigConstants
           "conf".equals(name) )
      {
        return;
      }
    } catch (NoSuchFieldException e) {
    } catch (SecurityException e) {
    }
    throw new IllegalArgumentException("option '" + name +
      "' not found--check if your option name is valid");
  }
}
