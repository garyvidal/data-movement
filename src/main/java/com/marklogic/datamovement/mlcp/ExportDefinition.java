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
package com.marklogic.datamovement.mlcp;

import com.marklogic.client.query.CtsQueryDefinition;
import com.marklogic.client.util.IterableNamespaceContext;

public interface ExportDefinition extends JobDefinition<ExportDefinition> {
  public ExportDefinition withQueryFilter(CtsQueryDefinition query);
  public CtsQueryDefinition getQueryFilter();
  public ExportDefinition withCollectionFilter(String... collections);
  public String[] getCollectionFilter();
  public ExportDefinition withCompress(boolean compress);
  public boolean getCompress();
  public ExportDefinition withContentEncoding(String charset);
  public String getContentEncoding();
  public ExportDefinition withCopyCollections(boolean copy);
  public boolean getCopyCollections();
  public ExportDefinition withCopyPermissions(boolean copy);
  public boolean getCopyPermissions();
  public ExportDefinition withCopyProperties(boolean copy);
  public boolean getCopyProperties();
  public ExportDefinition withCopyQuality(boolean copy);
  public boolean getCopyQuality();
  public ExportDefinition withDirectoryFilter(String... directories);
  public String[] getDirectoryFilter();
  public ExportDefinition withDocumentSelector(String xpath);
  public String getDocumentSelector();
  public ExportDefinition withIndented(boolean copy);
  public boolean getIndented();
  public ExportDefinition withMaxSplitSize(long splitSize);
  public long getMaxSplitSize();
  public ExportDefinition withOutputFilePath(String path);
  public String getOutputFilePath();
  public ExportDefinition withOutputType(ExportDefinition.OutputType path);
  public ExportDefinition.OutputType getOutputType();
  public ExportDefinition withPathNamespace(String prefix, String namespaceUri);
  public ExportDefinition withPathNamespaces(IterableNamespaceContext namespaces);
  public IterableNamespaceContext getPathNamespaces();
  public ExportDefinition withSnapshot(boolean snapshot);
  public boolean getSnapshot();
  public ExportDefinition withTransform(DataMovementTransform<?> transform);
  public DataMovementTransform<?> getTransform();

  public enum OutputType { ARCHIVE, DOCUMENT };
}
