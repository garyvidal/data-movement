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

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.query.CtsQueryDefinition;
import com.marklogic.client.util.IterableNamespaceContext;
import com.marklogic.datamovement.mlcp.DataMovementTransform;
import com.marklogic.datamovement.DataMovementManager;

/** Wraps <a href="https://docs.marklogic.com/guide/mlcp/copy">mlcp copy</a>.
 *
 * <p>All "setters" use "with" as a prefix and return the instance, so
 * configuration via method chaining like the following is possible:</p>
 *
 * <pre>   DataMovementManager moveMgr = DataMovementManager.newInstance();
 *   CopyDefinition def = moveMgr.newCopyDefinition()
 *     .withInputClient(inputClient)
 *     .withOutputClient(outputClient)
 *     .withQueryFilter(query);
 * </pre>
 *
 * <p>CopyDefinition includes all command-line options, but with names changed to
 * Java standards by removing underscores and using camel case.  Arguments are
 * native types, objects, enums, and varargs wherever possible to feel more
 * natural for Java developers.</p>
 *
 * <p>Once your instance of CopyDefinition is ready,
 * remember to call {@link DataMovementManager#startJob(CopyDefinition)} to
 * kick off the copy job.</p>
 *
 * <p><b>Warning</b> - mlcp currently does not throw any exceptions, and this
 * wrapper does not yet change that.  See standard out for mlcp warnings and
 * errors.  Additionally, job tracking (callbacks) and reporting are not yet
 * implemented.</p>
 **/
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
