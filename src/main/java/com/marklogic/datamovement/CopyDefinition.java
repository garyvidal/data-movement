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

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.query.CtsQueryDefinition;
import com.marklogic.client.util.IterableNamespaceContext;

public interface CopyDefinition extends JobDefinition<CopyDefinition> {
  public CopyDefinition withInputClient(DatabaseClient client);
  public DatabaseClient getInputClient();
  public CopyDefinition withOutputClient(DatabaseClient client);
  public DatabaseClient getOutputClient();
  public CopyDefinition withCollectionFilter(String... collections);
  public String[] getCollectionFilter();
  public CopyDefinition withCopyCollections(boolean copy);
  public boolean getCopyCollections();
  public CopyDefinition withCopyPermissions(boolean copy);
  public boolean getCopyPermissions();
  public CopyDefinition withCopyProperties(boolean copy);
  public boolean getCopyProperties();
  public CopyDefinition withCopyQuality(boolean copy);
  public boolean getCopyQuality();
  public CopyDefinition withDirectoryFilter(String... directories);
  public String[] getDirectoryFilter();
  public CopyDefinition withDocumentSelector(String xpath);
  public String getDocumentSelector();
  public CopyDefinition withFastload(boolean fastload);
  public boolean getFastload();
  public CopyDefinition withMaxSplitSize(long splitSize);
  public long getMaxSplitSize();
  public CopyDefinition withPathNamespace(String prefix, String namespaceUri);
  public CopyDefinition withPathNamespaces(IterableNamespaceContext namespaces);
  public IterableNamespaceContext getPathNamespaces();
  public CopyDefinition withOutputCollections(String... collections);
  public String[] getOutputCollections();
  public CopyDefinition withOutputPermission(String role, String capability);
  public CopyDefinition withOutputPermissions(String role, String capability, String... roleCapabilityPairs);
  public String[] getOutputPermissions();
  public CopyDefinition withOutputQuality(int quality);
  public int getOutputQuality();
  public CopyDefinition withOutputPartition(String partition);
  public String getOutputPartition();
  public CopyDefinition withOutputUriPrefix(String prefix);
  public String getOutputUriPrefix();
  public CopyDefinition withOutputUriReplacement(String pattern, String replacement);
  public CopyDefinition withOutputUriReplacements(String pattern, String replacement, String...patternReplacementPairs);
  public String[] getOutputUriReplace();
  public CopyDefinition withOutputUriSuffix(String suffix);
  public String getOutputUriSuffix();
  public CopyDefinition withQueryFilter(CtsQueryDefinition query);
  public CtsQueryDefinition getQueryFilter();
  public CopyDefinition withSnapshot(boolean snapshot);
  public boolean getSnapshot();
  public CopyDefinition withTemporalCollection(String collection);
  public String getTemporalCollection();
  public CopyDefinition withTransform(DataMovementTransform<?> transform);
  public DataMovementTransform<?> getTransform();
}
