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

import com.marklogic.datamovement.BatchListener;
import com.marklogic.datamovement.BatchFailureListener;
import com.marklogic.datamovement.DataMovementTransform;
import com.marklogic.datamovement.ImportDefinition;
import com.marklogic.datamovement.ImportDefinition.XmlRepairLevel;
import com.marklogic.datamovement.ImportEvent;
import com.marklogic.datamovement.JobTicket;
import com.marklogic.datamovement.impl.MlcpUtil;

import com.marklogic.contentpump.ConfigConstants;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ImportDefinitionImpl
  extends JobDefinitionImpl<ImportDefinition>
  implements ImportDefinition
{
  private boolean cleanDirectory;
  private ArrayList<String> collections = new ArrayList<>();
  private int maxSplitSize;
  private int minSplitSize;
  private ArrayList<String> outputPermissions = new ArrayList<>();
  private ArrayList<String> outputUriReplace = new ArrayList<>();
  private int quality;
  private boolean tolerateErrors;
  private DataMovementTransform transform;
  private ImportDefinition.XmlRepairLevel xmlRepairLevel;
  private BatchListener<ImportEvent> onSuccessListener;
  private BatchFailureListener<ImportEvent> onFailureListener;

  public ImportDefinitionImpl() {}

  public ImportDefinition contentEncoding(String charset) {
    return setOption(ConfigConstants.CONTENT_ENCODING, charset);
  }

  public String getContentEncoding() {
    return getOption(ConfigConstants.CONTENT_ENCODING);
  }

  public ImportDefinition inputFilePath(String path) {
    return setOption(ConfigConstants.INPUT_FILE_PATH, path);
  }

  public String getInputFilePath() {
    return getOption(ConfigConstants.INPUT_FILE_PATH);
  }

  public ImportDefinition inputFilePattern(String pattern) {
    return setOption(ConfigConstants.INPUT_FILE_PATTERN, pattern);
  }

  public String getInputFilePattern() {
    return getOption(ConfigConstants.INPUT_FILE_PATTERN);
  }

  public ImportDefinition maxSplitSize(int splitSize) {
    this.maxSplitSize = splitSize;
    return setOption(ConfigConstants.MAX_SPLIT_SIZE, String.valueOf(splitSize));
  }

  public int getMaxSplitSize() {
    return maxSplitSize;
  }

  public ImportDefinition minSplitSize(int splitSize) {
    this.minSplitSize = splitSize;
    return setOption(ConfigConstants.MIN_SPLIT_SIZE, String.valueOf(splitSize));
  }

  public int getMinSplitSize() {
    return minSplitSize;
  }

  public ImportDefinition outputCleanDir(boolean cleanDirectory) {
    this.cleanDirectory = cleanDirectory;
    return setOption(ConfigConstants.OUTPUT_CLEANDIR, String.valueOf(cleanDirectory));
  }

  public boolean getOutputCleanDir() {
    return cleanDirectory;
  }
  public ImportDefinition outputCollections(String... collections) {
    this.collections.clear();
    this.collections.addAll(Arrays.asList(collections));
    return setOption(ConfigConstants.OUTPUT_COLLECTIONS, String.join(",", collections));
  }

  public String[] getOutputCollections() {
    return collections.toArray(new String[0]);
  }

  public ImportDefinition outputDirectory(String directory) {
    return setOption(ConfigConstants.OUTPUT_DIRECTORY, directory);
  }

  public String getOutputDirectory() {
    return getOption(ConfigConstants.OUTPUT_DIRECTORY);
  }

  public ImportDefinition outputPartition(String partition) {
    return setOption(ConfigConstants.OUTPUT_PARTITION, partition);
  }

  public String getOutputPartition() {
    return getOption(ConfigConstants.OUTPUT_PARTITION);
  }

  public ImportDefinition outputPermissions(String role, String capability) {
    outputPermissions.clear();
    if ( role == null )     throw new IllegalArgumentException("role must not be null");
    if ( capability == null ) throw new IllegalArgumentException("capability must not be null");
    outputPermissions.add(role); outputPermissions.add(capability);
    return setOption(ConfigConstants.OUTPUT_PERMISSIONS, String.join(",", outputPermissions));
  }

  public ImportDefinition outputPermissions(String role, String capability, String... roleCapabilityPairs) {
    outputPermissions(role, capability);
    this.outputPermissions.addAll(Arrays.asList(roleCapabilityPairs));
    return setOption(ConfigConstants.OUTPUT_PERMISSIONS, String.join(",", outputPermissions));
}

  public String[] getOutputPermissions() {
    return outputPermissions.toArray(new String[0]);
  }

  public ImportDefinition outputQuality(int quality) {
    this.quality = quality;
    return setOption(ConfigConstants.OUTPUT_QUALITY, String.valueOf(quality));
  }

  public int getOutputQuality() {
    return quality;
  }

  public ImportDefinition outputUriPrefix(String prefix) {
    return setOption(ConfigConstants.OUTPUT_URI_PREFIX, prefix);
  }

  public String getOutputUriPrefix() {
    return getOption(ConfigConstants.OUTPUT_URI_PREFIX);
  }

  public ImportDefinition outputUriReplace(String pattern, String replacement) {
    outputUriReplace.clear();
    if ( pattern == null )     throw new IllegalArgumentException("pattern must not be null");
    if ( replacement == null ) throw new IllegalArgumentException("replacement must not be null");
    outputUriReplace.add(pattern); outputUriReplace.add(replacement);
    return setOption(ConfigConstants.OUTPUT_URI_REPLACE, MlcpUtil.combineRegexPairs(outputUriReplace));
  }

  public ImportDefinition outputUriReplace(String pattern, String replacement, String...patternReplacementPairs) {
    outputUriReplace(pattern, replacement);
    outputUriReplace.addAll(Arrays.asList(patternReplacementPairs));
    return setOption(ConfigConstants.OUTPUT_URI_REPLACE, MlcpUtil.combineRegexPairs(outputUriReplace));
  }

  public String[] getOutputUriReplace() {
    return outputUriReplace.toArray(new String[0]);
  }
  public ImportDefinition outputUriSuffix(String suffix) {
    return setOption(ConfigConstants.OUTPUT_URI_SUFFIX, suffix);
  }

  public String getOutputUriSuffix() {
    return getOption(ConfigConstants.OUTPUT_URI_SUFFIX);
  }
  public ImportDefinition temporalCollection(String collection) {
    return setOption(ConfigConstants.TEMPORAL_COLLECTION, collection);
  }

  public String getTemporalCollection() {
    return getOption(ConfigConstants.TEMPORAL_COLLECTION);
  }

  public ImportDefinition tolerateErrors(boolean tolerateErrors) {
    this.tolerateErrors = tolerateErrors;
    return setOption(ConfigConstants.TOLERATE_ERRORS, String.valueOf(tolerateErrors));
  }

  public boolean getTolerateErrors() {
    return tolerateErrors;
  }

  public ImportDefinition transform(DataMovementTransform transform) {
    this.transform = transform;
    MlcpUtil.clearOptionsForTransforms(getOptions());
    return setOptions( MlcpUtil.optionsForTransforms(transform) );
  }

  public DataMovementTransform getTransform() {
    return transform;
  }

  public ImportDefinition xmlRepairLevel(ImportDefinition.XmlRepairLevel xmlRepairLevel) {
    this.xmlRepairLevel = xmlRepairLevel;
    if ( xmlRepairLevel == null ) {
      return removeOption(ConfigConstants.XML_REPAIR_LEVEL);
    }
    return setOption(ConfigConstants.XML_REPAIR_LEVEL, xmlRepairLevel.toString());
  }

  public ImportDefinition.XmlRepairLevel getXmlRepairLevel() {
    return xmlRepairLevel;
  }

  public ImportDefinition onBatchSuccess(BatchListener<ImportEvent> listener) {
    this.onSuccessListener = listener;
    return this;
  }

  public ImportDefinition onBatchFailure(BatchFailureListener<ImportEvent> listener) {
    this.onFailureListener = listener;
    return this;
  }

  public List<String> getMlcpArgs() {
    ArrayList<String> args = new ArrayList<String>();
    args.add(JobTicket.JobType.IMPORT.toString());
    Map<String, String> options = getOptions();
    for ( String name: options.keySet() ) {
      args.add( "-" + name ); args.add( options.get(name) );
    }
    return args;
  }
}
