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

import com.marklogic.client.DatabaseClient;

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
  private String path;
  private String inputFilePattern;
  private ArrayList<String> outputUriReplace = new ArrayList<>();
  private DataMovementTransform transform;
  private ImportDefinition.XmlRepairLevel xmlRepairLevel;
  private BatchListener<ImportEvent> onSuccessListener;
  private BatchFailureListener<ImportEvent> onFailureListener;

  public ImportDefinitionImpl() {}

  public ImportDefinition maxSplitSize(int splitSize) {
    this.maxSplitSize = splitSize;
    return this;
  }

  public int getMaxSplitSize() {
    return maxSplitSize;
  }

  public ImportDefinition minSplitSize(int splitSize) {
    this.minSplitSize = splitSize;
    return this;
  }

  public int getMinSplitSize() {
    return minSplitSize;
  }

  public ImportDefinition inputFilePattern(String pattern) {
    this.inputFilePattern = inputFilePattern;
    return this;
  }

  public String getInputFilePath() {
    return path;
  }

  public ImportDefinition transform(DataMovementTransform transform) {
    this.transform = transform;
    return this;
  }

  public String getInputFilePattern() {
    return inputFilePattern;
  }

  public ImportDefinition inputFilePath(String path) {
    this.path = path;
    return this;
  }

  public ImportDefinition outputUriReplace(String pattern, String replacement) {
    outputUriReplace.clear();
    outputUriReplace.add(pattern); outputUriReplace.add(replacement);
    return this;
  }

  public ImportDefinition outputUriReplace(String pattern, String replacement, String...patternReplacePairs) {
    outputUriReplace(pattern, replacement);
    if ( patternReplacePairs != null && patternReplacePairs.length > 0 ) {
      if ( patternReplacePairs.length % 2 == 1 ) {
        throw new IllegalArgumentException("You must provide an even number of arguments--they are pairs " +
          "of pattern and replacement");
      }
      outputUriReplace.addAll(Arrays.asList(patternReplacePairs));
    }
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
    return this;
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
    try {
      args.addAll( MlcpUtil.argsFromGetters(this,
        "getMaxSplitSize", "getMinSplitSize", "getInputFilePath", "getXmlRepairLevel")
      );
      args.addAll( MlcpUtil.argsForRegexPairs(this,
        "getOutputUriReplace")
      );
      args.addAll( MlcpUtil.argsForTransforms(transform) );
    } catch(Exception e) {
      throw new DataMovementInternalError("error internal to DMSDK: " + e.toString(), e);
    }
    return args;
  }
}
