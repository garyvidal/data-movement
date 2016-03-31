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

import com.marklogic.contentpump.ConfigConstants;
import com.marklogic.datamovement.BatchFailureListener;
import com.marklogic.datamovement.BatchListener;
import com.marklogic.datamovement.ExportDefinition;
import com.marklogic.datamovement.ExportEvent;

public class ExportDefinitionImpl
  extends JobDefinitionImpl<ExportDefinition>
  implements ExportDefinition
{
  private ExportDefinition.OutputType outputType;

  public ExportDefinitionImpl() {}

  public ExportDefinition withCompress(boolean compress) {
    return withOption(ConfigConstants.OUTPUT_COMPRESS, String.valueOf(compress));
  }

  public boolean getCompress() {
    return getBooleanOption(ConfigConstants.OUTPUT_COMPRESS, false);
  }

  public ExportDefinition withContentEncoding(String charset) {
    return withOption(ConfigConstants.CONTENT_ENCODING, charset);
  }

  public String getContentEncoding() {
    return getOption(ConfigConstants.CONTENT_ENCODING);
  }

  public ExportDefinition withIndented(boolean copy) {
    return withOption(ConfigConstants.OUTPUT_INDENTED, String.valueOf(copy));
  }

  public boolean getIndented() {
    return getBooleanOption(ConfigConstants.OUTPUT_INDENTED, false);
  }

  public ExportDefinition withOutputFilePath(String path) {
    return withOption(ConfigConstants.OUTPUT_FILE_PATH, path);
  }

  public String getOutputFilePath() {
    return getOption(ConfigConstants.OUTPUT_FILE_PATH);
  }

  public ExportDefinition withOutputType(ExportDefinition.OutputType outputType) {
    this.outputType = outputType;
    if ( outputType == null ) {
      return removeOption(ConfigConstants.XML_REPAIR_LEVEL);
    }
    return withOption(ConfigConstants.OUTPUT_TYPE, outputType.toString().toLowerCase());
  }

  public ExportDefinition.OutputType getOutputType() {
    return outputType;
  }

  public ExportDefinition onBatchSuccess(BatchListener<ExportEvent> listener) {
    throw new IllegalStateException("this feature is not yet implemented");
  }

  public ExportDefinition onBatchFailure(BatchFailureListener<ExportEvent> listener) {
    throw new IllegalStateException("this feature is not yet implemented");
  }

}
