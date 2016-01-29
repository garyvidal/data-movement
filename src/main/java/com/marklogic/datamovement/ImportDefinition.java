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

public interface ImportDefinition extends JobDefinition<ImportDefinition> {
  public ImportDefinition maxSplitSize(int splitSize);
  public int getMaxSplitSize();
  public ImportDefinition minSplitSize(int splitSize);
  public int getMinSplitSize();
  public ImportDefinition inputFilePath(String path);
  public String getInputFilePath();
  public ImportDefinition inputFilePattern(String pattern);
  public String getInputFilePattern();
  public ImportDefinition outputUriReplace(String pattern, String replacement);
  public ImportDefinition outputUriReplace(String pattern, String replacement, String...patternReplacePairs);
  public String[] getOutputUriReplace();
  public ImportDefinition transform(DataMovementTransform transform);
  public DataMovementTransform getTransform();
  public ImportDefinition xmlRepairLevel(ImportDefinition.XmlRepairLevel xmlRepairLevel);
  public ImportDefinition.XmlRepairLevel getXmlRepairLevel();
  public ImportDefinition onBatchSuccess(BatchListener<ImportEvent> listener);
  public ImportDefinition onBatchFailure(BatchFailureListener<ImportEvent> listener);

  public enum XmlRepairLevel {
    DEFAULT,
    FULL,
    NONE;

    public String toString() {
      return super.toString().toLowerCase();
      
    }
  };
}
