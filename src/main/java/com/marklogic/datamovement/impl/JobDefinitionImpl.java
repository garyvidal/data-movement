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
import com.marklogic.contentpump.ConfigConstants;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class JobDefinitionImpl<T extends JobDefinition<T>>
  implements JobDefinition<T>
{
  private static Set<String> configConstantsValues = new HashSet<>();
  static {
    for ( Field field : ConfigConstants.class.getDeclaredFields() ) {
      try {
        configConstantsValues.add( field.get(ConfigConstants.class).toString() );
      } catch(Exception e) {}
    }
  }

  private String jobName;
  private JobDefinition.Mode mode;
  private Map<String, String> options = new LinkedHashMap<String, String>();

  public JobDefinitionImpl() {}

  public T withJobName(String jobName) {
    this.jobName = jobName;
    return (T) this;
  }

  public T withOptionsFile(String optionsFilePath) {
    return withOption(ConfigConstants.OPTIONS_FILE, optionsFilePath);
  }

  public T withThreadCount(int threadCount) {
    return withOption(ConfigConstants.THREAD_COUNT, String.valueOf(threadCount));
  }

  public int getThreadCount() {
    return getIntegerOption(ConfigConstants.THREAD_COUNT, 10);
  }

  public T withThreadCountPerSplit(int threadCount) {
    return withOption(ConfigConstants.THREADS_PER_SPLIT, String.valueOf(threadCount));
  }

  public int getThreadCountPerSplit() {
    return getIntegerOption(ConfigConstants.THREADS_PER_SPLIT, -1);
  }

  public T withBatchSize(long batchSize) {
    return withOption(ConfigConstants.BATCH_SIZE, String.valueOf(batchSize));
  }

  public long getBatchSize() {
    return getLongOption(ConfigConstants.BATCH_SIZE, 100);
  }

  public T withConf(String filepath) {
    return withOption("conf", filepath);
  }

  public String getConf() {
    return getOption("conf");
  }

  public T withMode(JobDefinition.Mode mode) {
    this.mode = mode;
    return withOption(ConfigConstants.MODE, mode.toString().toLowerCase());
  }

  public JobDefinition.Mode getMode() {
    return this.mode;
  }

  public T withTransactionSize(int transactionSize) {
    return withOption(ConfigConstants.TRANSACTION_SIZE, String.valueOf(transactionSize));
  }

  public int getTransactionSize() {
    return getIntegerOption(ConfigConstants.TRANSACTION_SIZE, 10);
  }

  /** All option names must match mlcp command-line options (without the preceeding hyphen).
   */
  public synchronized T withOption(String name, String value) {
    checkMlcpOptionName(name);
    this.options.put(name, value);
    return (T) this;
  }

  /** All option names must match mlcp command-line options (without the preceeding hyphen).
   */
  public synchronized T withOptions(Map<String, String> options) {
    if ( options == null ) return (T) this;
    for ( String name : options.keySet() ) {
      checkMlcpOptionName(name);
    }
    this.options.putAll(options);
    return (T) this;
  }

  /** All option names must match mlcp command-line options (without the preceeding hyphen).
   */
  public String getOption(String name) {
    checkMlcpOptionName(name);
    return this.options.get(name);
  }

  public boolean getBooleanOption(String name, boolean defaultValue) {
    String value = getOption(name);
    if ( value == null ) return defaultValue;
    return Boolean.valueOf(value);
  }

  public long getLongOption(String name, long defaultValue) {
    String value = getOption(name);
    if ( value == null ) return defaultValue;
    return Long.valueOf(value);
  }

  public int getIntegerOption(String name, int defaultValue) {
    String value = getOption(name);
    if ( value == null ) return defaultValue;
    return Integer.valueOf(value);
  }

  /** Return all options set on this instance.
   */
  public Map<String, String> getOptions() {
    return options;
  }

  /** All option names must match mlcp command-line options (without the preceeding hyphen).
   */
  public synchronized T removeOption(String name) {
    checkMlcpOptionName(name);
    this.options.remove(name);
    return (T) this;
  }

  private void checkMlcpOptionName(String name) {
    if ( name == null ) throw new IllegalArgumentException("option name must not be null");
    if ( configConstantsValues.contains(name) ||
         // for some reason "conf" is missing from ConfigConstants
         "conf".equals(name) )
    {
      return;
    }
    throw new IllegalArgumentException("option '" + name +
      "' not found--check if your option name is valid");
  }
}
