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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

import com.marklogic.client.query.CtsQueryDefinition;
import com.marklogic.client.util.EditableNamespaceContext;
import com.marklogic.client.util.IterableNamespaceContext;
import com.marklogic.contentpump.ConfigConstants;
import com.marklogic.datamovement.BatchFailureListener;
import com.marklogic.datamovement.BatchListener;
import com.marklogic.datamovement.DataMovementTransform;
import com.marklogic.datamovement.ExportDefinition;
import com.marklogic.datamovement.ExportEvent;

public class ExportDefinitionImpl
  extends JobDefinitionImpl<ExportDefinition>
  implements ExportDefinition
{
  private ArrayList<String> collections = new ArrayList<>();
  private ArrayList<String> directories = new ArrayList<>();
  private EditableNamespaceContext namespaces = new EditableNamespaceContext();
  private ExportDefinition.OutputType outputType;
  private CtsQueryDefinition query;
  private DataMovementTransform<?> transform;

  public ExportDefinitionImpl() {}

  public ExportDefinition withCollectionFilter(String... collections) {
    this.collections.clear();
    this.collections.addAll(Arrays.asList(collections));
    return withOption(ConfigConstants.COLLECTION_FILTER, String.join(",", collections));
  }

  public String[] getCollectionFilter() {
    return collections.toArray(new String[collections.size()]);
  }

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

  public ExportDefinition withCopyCollections(boolean copy) {
    return withOption(ConfigConstants.COPY_COLLECTIONS, String.valueOf(copy));
  }

  public boolean getCopyCollections() {
    return getBooleanOption(ConfigConstants.COPY_COLLECTIONS, true);
  }

  public ExportDefinition withCopyPermissions(boolean copy) {
    return withOption(ConfigConstants.COPY_PERMISSIONS, String.valueOf(copy));
  }

  public boolean getCopyPermissions() {
    return getBooleanOption(ConfigConstants.COPY_PERMISSIONS, true);
  }

  public ExportDefinition withCopyProperties(boolean copy) {
    return withOption(ConfigConstants.COPY_PROPERTIES, String.valueOf(copy));
  }

  public boolean getCopyProperties() {
    return getBooleanOption(ConfigConstants.COPY_PROPERTIES, true);
  }

  public ExportDefinition withCopyQuality(boolean copy) {
    return withOption(ConfigConstants.COPY_QUALITY, String.valueOf(copy));
  }

  public boolean getCopyQuality() {
    return getBooleanOption(ConfigConstants.COPY_QUALITY, true);
  }

  public ExportDefinition withDirectoryFilter(String... directories) {
    this.directories.clear();
    this.directories.addAll(Arrays.asList(directories));
    return withOption(ConfigConstants.DIRECTORY_FILTER, String.join(",", directories));
  }

  public String[] getDirectoryFilter() {
    return directories.toArray(new String[directories.size()]);
  }

  public ExportDefinition withDocumentSelector(String xpath) {
    return withOption(ConfigConstants.DOCUMENT_SELECTOR, xpath);
  }

  public String getDocumentSelector() {
    return getOption(ConfigConstants.DOCUMENT_SELECTOR);
  }

  public ExportDefinition withIndented(boolean copy) {
    return withOption(ConfigConstants.OUTPUT_INDENTED, String.valueOf(copy));
  }

  public boolean getIndented() {
    return getBooleanOption(ConfigConstants.OUTPUT_INDENTED, false);
  }

  public ExportDefinition withMaxSplitSize(long splitSize) {
    return withOption(ConfigConstants.MAX_SPLIT_SIZE, String.valueOf(splitSize));
  }

  public long getMaxSplitSize() {
    return getLongOption(ConfigConstants.MAX_SPLIT_SIZE, 20000);
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

  public ExportDefinition withPathNamespace(String prefix, String namespaceUri) {
    this.namespaces.put(prefix, namespaceUri);
    return withPathNamespaces(this.namespaces);
  }

  public ExportDefinition withPathNamespaces(IterableNamespaceContext namespaces) {
    if ( namespaces != this.namespaces ) {
      this.namespaces.clear();
      if ( namespaces == null ) return this;
      for ( String prefix : namespaces.getAllPrefixes() ) {
        this.namespaces.put(prefix, namespaces.getNamespaceURI(prefix));
      }
    }
    String paths = this.namespaces.keySet().stream().map(
      prefix -> prefix + "," + namespaces.getNamespaceURI(prefix)
    ).collect(Collectors.joining(","));
    return withOption(ConfigConstants.PATH_NAMESPACE, paths);
  }

  public IterableNamespaceContext getPathNamespaces() {
    return namespaces;
  }

  public ExportDefinition withQueryFilter(CtsQueryDefinition query) {
    this.query = query;
    return withOption(ConfigConstants.QUERY_FILTER, query.serialize());
  }

  public CtsQueryDefinition getQueryFilter() {
    return query;
  }

  public ExportDefinition withSnapshot(boolean snapshot) {
    return withOption(ConfigConstants.SNAPSHOT, String.valueOf(snapshot));
  }

  public boolean getSnapshot() {
    return getBooleanOption(ConfigConstants.SNAPSHOT, false);
  }

  public ExportDefinition withTransform(DataMovementTransform<?> transform) {
    this.transform = transform;
    MlcpUtil.clearOptionsForTransforms(getOptions());
    return withOptions( MlcpUtil.optionsForTransforms(transform) );
  }

  public DataMovementTransform<?> getTransform() {
    return transform;
  }

  public ExportDefinition onBatchSuccess(BatchListener<ExportEvent> listener) {
    throw new IllegalStateException("this feature is not yet implemented");
  }

  public ExportDefinition onBatchFailure(BatchFailureListener<ExportEvent> listener) {
    throw new IllegalStateException("this feature is not yet implemented");
  }

}
