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

import com.marklogic.datamovement.mlcp.DataMovementTransform;
import com.marklogic.datamovement.mlcp.JobDefinition;
import com.marklogic.datamovement.JobTicket.JobType;
import com.marklogic.client.query.CtsQueryDefinition;
import com.marklogic.client.util.EditableNamespaceContext;
import com.marklogic.client.util.IterableNamespaceContext;
import com.marklogic.contentpump.ConfigConstants;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
  private ArrayList<String> collections = new ArrayList<>();
  private ArrayList<String> directories = new ArrayList<>();
  private JobDefinition.Mode mode;
  private EditableNamespaceContext namespaces = new EditableNamespaceContext();
  private Map<String, String> options = new LinkedHashMap<String, String>();
  private ArrayList<String> outputPermissions = new ArrayList<>();
  private ArrayList<String> outputUriReplace = new ArrayList<>();
  private int quality;
  private CtsQueryDefinition query;
  private DataMovementTransform<?> transform;

  public JobDefinitionImpl() {}

  @SuppressWarnings("unchecked")
  // Needed by all
  public T withJobName(String jobName) {
    this.jobName = jobName;
    return (T) this;
  }

  @Override
  // Needed by all
  public String getJobName() {
    return jobName;
  }

  // Needed by ExportDefinitionImpl and CopyDefinitionImpl
  public T withCollectionFilter(String... collections) {
    this.collections.clear();
    this.collections.addAll(Arrays.asList(collections));
    return withOption(ConfigConstants.COLLECTION_FILTER, String.join(",", collections));
  }

  // Needed by ExportDefinitionImpl and CopyDefinitionImpl
  public String[] getCollectionFilter() {
    return collections.toArray(new String[collections.size()]);
  }

  // Needed by ExportDefinitionImpl and CopyDefinitionImpl
  public T withCopyCollections(boolean copy) {
    return withOption(ConfigConstants.COPY_COLLECTIONS, String.valueOf(copy));
  }

  // Needed by ExportDefinitionImpl and CopyDefinitionImpl
  public boolean getCopyCollections() {
    return getBooleanOption(ConfigConstants.COPY_COLLECTIONS, true);
  }

  // Needed by ExportDefinitionImpl and CopyDefinitionImpl
  public T withCopyPermissions(boolean copy) {
    return withOption(ConfigConstants.COPY_PERMISSIONS, String.valueOf(copy));
  }

  // Needed by ExportDefinitionImpl and CopyDefinitionImpl
  public boolean getCopyPermissions() {
    return getBooleanOption(ConfigConstants.COPY_PERMISSIONS, true);
  }

  // Needed by ExportDefinitionImpl and CopyDefinitionImpl
  public T withCopyProperties(boolean copy) {
    return withOption(ConfigConstants.COPY_PROPERTIES, String.valueOf(copy));
  }

  // Needed by ExportDefinitionImpl and CopyDefinitionImpl
  public boolean getCopyProperties() {
    return getBooleanOption(ConfigConstants.COPY_PROPERTIES, true);
  }

  // Needed by ExportDefinitionImpl and CopyDefinitionImpl
  public T withCopyQuality(boolean copy) {
    return withOption(ConfigConstants.COPY_QUALITY, String.valueOf(copy));
  }

  // Needed by ExportDefinitionImpl and CopyDefinitionImpl
  public boolean getCopyQuality() {
    return getBooleanOption(ConfigConstants.COPY_QUALITY, true);
  }

  // Needed by ExportDefinitionImpl and CopyDefinitionImpl
  public T withDirectoryFilter(String... directories) {
    this.directories.clear();
    this.directories.addAll(Arrays.asList(directories));
    return withOption(ConfigConstants.DIRECTORY_FILTER, String.join(",", directories));
  }

  // Needed by ExportDefinitionImpl and CopyDefinitionImpl
  public String[] getDirectoryFilter() {
    return directories.toArray(new String[directories.size()]);
  }

  // Needed by ExportDefinitionImpl and CopyDefinitionImpl
  public T withDocumentSelector(String xpath) {
    return withOption(ConfigConstants.DOCUMENT_SELECTOR, xpath);
  }

  // Needed by ExportDefinitionImpl and CopyDefinitionImpl
  public String getDocumentSelector() {
    return getOption(ConfigConstants.DOCUMENT_SELECTOR);
  }

  // Needed by ImportDefinitionImpl and CopyDefinitionImpl
  public T withFastload(boolean fastload) {
    return withOption(ConfigConstants.FAST_LOAD, String.valueOf(fastload));
  }

  // Needed by ImportDefinitionImpl and CopyDefinitionImpl
  public boolean getFastload() {
    return getBooleanOption(ConfigConstants.FAST_LOAD, false);
  }

  // Needed by ExportDefinitionImpl and CopyDefinitionImpl
  public T withMaxSplitSize(long splitSize) {
    return withOption(ConfigConstants.MAX_SPLIT_SIZE, String.valueOf(splitSize));
  }

  // Needed by ExportDefinitionImpl and CopyDefinitionImpl
  public long getMaxSplitSize() {
    return getLongOption(ConfigConstants.MAX_SPLIT_SIZE, 20000);
  }

  // Needed by ImportDefinitionImpl and CopyDefinitionImpl
  public T withOutputCollections(String... collections) {
    this.collections.clear();
    this.collections.addAll(Arrays.asList(collections));
    return withOption(ConfigConstants.OUTPUT_COLLECTIONS, String.join(",", collections));
  }

  // Needed by ImportDefinitionImpl and CopyDefinitionImpl
  public String[] getOutputCollections() {
    return collections.toArray(new String[collections.size()]);
  }

  // Needed by ImportDefinitionImpl and CopyDefinitionImpl
  public T withOutputPermission(String role, String capability) {
    if ( role == null )       throw new IllegalArgumentException("role must not be null");
    if ( capability == null ) throw new IllegalArgumentException("capability must not be null");
    outputPermissions.add(role); outputPermissions.add(capability);
    return withOption(ConfigConstants.OUTPUT_PERMISSIONS, String.join(",", outputPermissions));
  }

  @SuppressWarnings("unchecked")
  // Needed by ImportDefinitionImpl and CopyDefinitionImpl
  public T withOutputPermissions(String role, String capability, String... roleCapabilityPairs) {
    outputPermissions.clear();
    withOutputPermission(role, capability);
    if ( roleCapabilityPairs == null ) return (T) this;
    this.outputPermissions.addAll(Arrays.asList(roleCapabilityPairs));
    return withOption(ConfigConstants.OUTPUT_PERMISSIONS, String.join(",", outputPermissions));
  }

  // Needed by ImportDefinitionImpl and CopyDefinitionImpl
  public String[] getOutputPermissions() {
    return outputPermissions.toArray(new String[outputPermissions.size()]);
  }

  // Needed by ImportDefinitionImpl and CopyDefinitionImpl
  public T withOutputPartition(String partition) {
    return withOption(ConfigConstants.OUTPUT_PARTITION, partition);
  }

  // Needed by ImportDefinitionImpl and CopyDefinitionImpl
  public String getOutputPartition() {
    return getOption(ConfigConstants.OUTPUT_PARTITION);
  }

  // Needed by ImportDefinitionImpl and CopyDefinitionImpl
  public T withOutputQuality(int quality) {
    this.quality = quality;
    return withOption(ConfigConstants.OUTPUT_QUALITY, String.valueOf(quality));
  }

  // Needed by ImportDefinitionImpl and CopyDefinitionImpl
  public int getOutputQuality() {
    return quality;
  }

  // Needed by ImportDefinitionImpl and CopyDefinitionImpl
  public T withOutputUriPrefix(String prefix) {
    return withOption(ConfigConstants.OUTPUT_URI_PREFIX, prefix);
  }

  // Needed by ImportDefinitionImpl and CopyDefinitionImpl
  public String getOutputUriPrefix() {
    return getOption(ConfigConstants.OUTPUT_URI_PREFIX);
  }

  // Needed by ImportDefinitionImpl and CopyDefinitionImpl
  public T withOutputUriReplacement(String pattern, String replacement) {
    if ( pattern == null )     throw new IllegalArgumentException("pattern must not be null");
    if ( replacement == null ) throw new IllegalArgumentException("replacement must not be null");
    outputUriReplace.add(pattern); outputUriReplace.add(replacement);
    return withOption(ConfigConstants.OUTPUT_URI_REPLACE, MlcpUtil.combineRegexPairs(outputUriReplace));
  }

  // Needed by ImportDefinitionImpl and CopyDefinitionImpl
  public T withOutputUriReplacements(String pattern, String replacement, String...patternReplacementPairs) {
    outputUriReplace.clear();
    withOutputUriReplacement(pattern, replacement);
    outputUriReplace.addAll(Arrays.asList(patternReplacementPairs));
    return withOption(ConfigConstants.OUTPUT_URI_REPLACE, MlcpUtil.combineRegexPairs(outputUriReplace));
  }

  // Needed by ImportDefinitionImpl and CopyDefinitionImpl
  public String[] getOutputUriReplace() {
    return outputUriReplace.toArray(new String[0]);
  }

  // Needed by ImportDefinitionImpl and CopyDefinitionImpl
  public T withOutputUriSuffix(String suffix) {
    return withOption(ConfigConstants.OUTPUT_URI_SUFFIX, suffix);
  }

  // Needed by ImportDefinitionImpl and CopyDefinitionImpl
  public String getOutputUriSuffix() {
    return getOption(ConfigConstants.OUTPUT_URI_SUFFIX);
  }


  // Needed by all
  public T withOptionsFile(String optionsFilePath) {
    return withOption(ConfigConstants.OPTIONS_FILE, optionsFilePath);
  }

  @Override
  // Needed by all
  public String getOptionsFile() {
    return getOption(ConfigConstants.OPTIONS_FILE);
  }

  // Needed by ExportDefinitionImpl and CopyDefinitionImpl
  public T withPathNamespace(String prefix, String namespaceUri) {
    this.namespaces.put(prefix, namespaceUri);
    return withPathNamespaces(this.namespaces);
  }

  // Needed by ExportDefinitionImpl and CopyDefinitionImpl
  public T withPathNamespaces(IterableNamespaceContext namespaces) {
    if ( namespaces != this.namespaces ) {
      this.namespaces.clear();
      if ( namespaces == null ) return (T) this;
      for ( String prefix : namespaces.getAllPrefixes() ) {
        this.namespaces.put(prefix, namespaces.getNamespaceURI(prefix));
      }
    }
    String paths = this.namespaces.keySet().stream().map(
      prefix -> prefix + "," + namespaces.getNamespaceURI(prefix)
    ).collect(Collectors.joining(","));
    return withOption(ConfigConstants.PATH_NAMESPACE, paths);
  }

  // Needed by ExportDefinitionImpl and CopyDefinitionImpl
  public IterableNamespaceContext getPathNamespaces() {
    return namespaces;
  }

  // Needed by ExportDefinitionImpl and CopyDefinitionImpl
  public T withQueryFilter(CtsQueryDefinition query) {
    this.query = query;
    return withOption(ConfigConstants.QUERY_FILTER, query.serialize());
  }

  // Needed by ExportDefinitionImpl and CopyDefinitionImpl
  public CtsQueryDefinition getQueryFilter() {
    return query;
  }

  // Needed by ExportDefinitionImpl and CopyDefinitionImpl
  public T withSnapshot(boolean snapshot) {
    return withOption(ConfigConstants.SNAPSHOT, String.valueOf(snapshot));
  }

  // Needed by ExportDefinitionImpl and CopyDefinitionImpl
  public boolean getSnapshot() {
    return getBooleanOption(ConfigConstants.SNAPSHOT, false);
  }

  // Needed by ImportDefinitionImpl and CopyDefinitionImpl
  public T withTemporalCollection(String collection) {
    return withOption(ConfigConstants.TEMPORAL_COLLECTION, collection);
  }

  // Needed by ImportDefinitionImpl and CopyDefinitionImpl
  public String getTemporalCollection() {
    return getOption(ConfigConstants.TEMPORAL_COLLECTION);
  }

  // Needed by all
  public T withThreadCount(int threadCount) {
    return withOption(ConfigConstants.THREAD_COUNT, String.valueOf(threadCount));
  }

  // Needed by all
  public int getThreadCount() {
    return getIntegerOption(ConfigConstants.THREAD_COUNT, 10);
  }

  // Needed by all
  public T withThreadCountPerSplit(int threadCount) {
    return withOption(ConfigConstants.THREADS_PER_SPLIT, String.valueOf(threadCount));
  }

  // Needed by all
  public int getThreadCountPerSplit() {
    return getIntegerOption(ConfigConstants.THREADS_PER_SPLIT, -1);
  }

  // Needed by all
  public T withBatchSize(long batchSize) {
    return withOption(ConfigConstants.BATCH_SIZE, String.valueOf(batchSize));
  }

  // Needed by all
  public long getBatchSize() {
    return getLongOption(ConfigConstants.BATCH_SIZE, 100);
  }

  // Needed by all
  public T withConf(String filepath) {
    return withOption("conf", filepath);
  }

  // Needed by all
  public String getConf() {
    return getOption("conf");
  }

  // Needed by all
  public T withMode(JobDefinition.Mode mode) {
    this.mode = mode;
    return withOption(ConfigConstants.MODE, mode.toString().toLowerCase());
  }

  // Needed by all
  public JobDefinition.Mode getMode() {
    return this.mode;
  }

  // Needed by all
  public T withTransactionSize(int transactionSize) {
    return withOption(ConfigConstants.TRANSACTION_SIZE, String.valueOf(transactionSize));
  }

  // Needed by all
  public int getTransactionSize() {
    return getIntegerOption(ConfigConstants.TRANSACTION_SIZE, 10);
  }

  // Needed by all
  public T withTransform(DataMovementTransform<?> transform) {
    this.transform = transform;
    MlcpUtil.clearOptionsForTransforms(getOptions());
    return withOptions( MlcpUtil.optionsForTransforms(transform) );
  }

  // Needed by all
  public DataMovementTransform<?> getTransform() {
    return transform;
  }

  @SuppressWarnings("unchecked")
  // Needed by all
  /** All option names must match mlcp command-line options (without the preceeding hyphen).
   */
  public synchronized T withOption(String name, String value) {
    checkMlcpOptionName(name);
    if ( value == null ) throw new IllegalArgumentException("Value of option '" + name +
      "' cannot be null");
    this.options.put(name, value);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  // Needed by all
  /** All option names must match mlcp command-line options (without the preceeding hyphen).
   */
  public synchronized T withOptions(Map<String, String> options) {
    if ( options == null ) return (T) this;
    for ( String name : options.keySet() ) {
      checkMlcpOptionName(name);
      if ( options.get(name) == null ) throw new IllegalArgumentException("Value of option '"
        + name + "' cannot be null");
    }
    this.options.putAll(options);
    return (T) this;
  }

  // Needed by all
  /** All option names must match mlcp command-line options (without the preceeding hyphen).
   */
  public String getOption(String name) {
    checkMlcpOptionName(name);
    return this.options.get(name);
  }

  // Needed by all
  public boolean getBooleanOption(String name, boolean defaultValue) {
    String value = getOption(name);
    if ( value == null ) return defaultValue;
    return Boolean.valueOf(value);
  }

  // Needed by all
  public long getLongOption(String name, long defaultValue) {
    String value = getOption(name);
    if ( value == null ) return defaultValue;
    return Long.valueOf(value);
  }

  // Needed by all
  public int getIntegerOption(String name, int defaultValue) {
    String value = getOption(name);
    if ( value == null ) return defaultValue;
    return Integer.valueOf(value);
  }

  /** Return all options set on this instance.
   */
  // Needed by all
  public Map<String, String> getOptions() {
    return options;
  }

  @SuppressWarnings("unchecked")
  /** All option names must match mlcp command-line options (without the preceeding hyphen).
   */
  // Needed by all
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

  // Needed by all
  public List<String> getMlcpArgs(JobType jobType) {
    ArrayList<String> args = new ArrayList<String>();
    args.add(jobType.toString());
    Map<String, String> options = getOptions();
    for ( String name: options.keySet() ) {
      args.add( "-" + name ); args.add( options.get(name) );
    }
    return args;
  }
}
