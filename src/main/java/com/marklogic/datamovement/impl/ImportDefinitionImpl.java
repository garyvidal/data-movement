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
  private int maxSplitSize;
  private int minSplitSize;
  private ArrayList<String> outputUriReplace = new ArrayList<>();
  private DataMovementTransform transform;
  private ImportDefinition.XmlRepairLevel xmlRepairLevel;
  private BatchListener<ImportEvent> onSuccessListener;
  private BatchFailureListener<ImportEvent> onFailureListener;

  public ImportDefinitionImpl() {}

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

  public ImportDefinition inputFilePath(String path) {
    return setOption(ConfigConstants.INPUT_FILE_PATH, path);
  }

  public String getInputFilePath() {
    return getOption(ConfigConstants.INPUT_FILE_PATH);
  }

  public ImportDefinition transform(DataMovementTransform transform) {
    this.transform = transform;
    MlcpUtil.clearOptionsForTransforms(getOptions());
    setOptions( MlcpUtil.optionsForTransforms(transform) );
    return this;
  }

  public ImportDefinition inputFilePattern(String pattern) {
    return setOption(ConfigConstants.INPUT_FILE_PATTERN, pattern);
  }

  public String getInputFilePattern() {
    return getOption(ConfigConstants.INPUT_FILE_PATTERN);
  }

  public ImportDefinition outputUriReplace(String pattern, String replacement) {
    outputUriReplace.clear();
    if ( pattern == null )     throw new IllegalArgumentException("pattern must not be null");
    if ( replacement == null ) throw new IllegalArgumentException("replacement must not be null");
    outputUriReplace.add(pattern); outputUriReplace.add(replacement);
    setOption(ConfigConstants.OUTPUT_URI_REPLACE, MlcpUtil.combineRegexPairs(outputUriReplace));
    return this;
  }

  public ImportDefinition outputUriReplace(String pattern, String replacement, String...patternReplacePairs) {
    outputUriReplace(pattern, replacement);
    outputUriReplace.addAll(Arrays.asList(patternReplacePairs));
    setOption(ConfigConstants.OUTPUT_URI_REPLACE, MlcpUtil.combineRegexPairs(outputUriReplace));
    return this;
  }

  public String[] getOutputUriReplace() {
    return outputUriReplace.toArray(new String[0]);
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
