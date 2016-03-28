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
package com.marklogic.datamovement.test;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;

import org.junit.Test;

import com.marklogic.client.document.ServerTransform;
import com.marklogic.client.io.Format;
import com.marklogic.client.query.CtsQueryDefinition;
import com.marklogic.client.util.EditableNamespaceContext;
import com.marklogic.contentpump.ConfigConstants;
import com.marklogic.datamovement.DataMovementManager;
import com.marklogic.datamovement.ExportDefinition;
import com.marklogic.datamovement.ImportDefinition.DataType;
import com.marklogic.datamovement.ImportDefinition.InputFileType;
import com.marklogic.datamovement.ImportDefinition.SequenceFileImportDefinition.SequenceValueType;
import com.marklogic.datamovement.JobTicket.JobType;
import com.marklogic.datamovement.JobTicket;
import com.marklogic.datamovement.impl.ExportDefinitionImpl;
import com.marklogic.datamovement.impl.ImportDefinitionImpl;

/** This test just confirms that the mlcp command line options get set to what we want them to be.
 * This test doesn't talk to the server.
 */
public class ExportDefinitionTest {
  private DataMovementManager moveMgr = DataMovementManager.newInstance();

  // Java Client API just has a place-holder interface for CtsQueryDefinition,
  // so for the moment we'll mock our own.  serialize() is the only method that
  // matters.
  public static class MockCtsQueryDefinition implements CtsQueryDefinition {
    private String contents;

    public MockCtsQueryDefinition(String contents) {
        this.contents = contents;
    }
    
    public String serialize() { return contents; }
    public String getOptionsName() { return null; }
    public void setOptionsName(String name) { }
    public String[] getCollections() { return null; }
    public void setCollections(String... collections) { }
    public String getDirectory() { return null; }
    public void setDirectory(String directory) { }
    public ServerTransform getResponseTransform() { return null; }
    public void setResponseTransform(ServerTransform transform) { }
  }

  @Test
  public void testOptions() throws Exception {
    ExportDefinitionImpl def = (ExportDefinitionImpl) moveMgr.newExportDefinition();
    ArrayList<String> expectedMlcpParams = new ArrayList<String>();
    expectedMlcpParams.add(JobTicket.JobType.EXPORT.toString());

    def.withCollectionFilter("filterA", "filterB", "filterC");
    expectedMlcpParams.add("-" + ConfigConstants.COLLECTION_FILTER); expectedMlcpParams.add("filterA,filterB,filterC");

    def.withCompress(true);
    expectedMlcpParams.add("-" + ConfigConstants.OUTPUT_COMPRESS); expectedMlcpParams.add("true");

    def.withContentEncoding("UTF-8");
    expectedMlcpParams.add("-" + ConfigConstants.CONTENT_ENCODING); expectedMlcpParams.add("UTF-8");

    def.withCopyCollections(false)
      .withCopyPermissions(false)
      .withCopyProperties (false)
      .withCopyQuality    (false);
    expectedMlcpParams.add("-" + ConfigConstants.COPY_COLLECTIONS); expectedMlcpParams.add("false");
    expectedMlcpParams.add("-" + ConfigConstants.COPY_PERMISSIONS); expectedMlcpParams.add("false");
    expectedMlcpParams.add("-" + ConfigConstants.COPY_PROPERTIES); expectedMlcpParams.add("false");
    expectedMlcpParams.add("-" + ConfigConstants.COPY_QUALITY); expectedMlcpParams.add("false");

    def.withDirectoryFilter("dirA", "dirB", "dirC");
    expectedMlcpParams.add("-" + ConfigConstants.DIRECTORY_FILTER);  expectedMlcpParams.add("dirA,dirB,dirC");

    def.withDocumentSelector("/my:some/other:xpath");
    expectedMlcpParams.add("-" + ConfigConstants.DOCUMENT_SELECTOR);  expectedMlcpParams.add("/my:some/other:xpath");

    def.withIndented(true);
    expectedMlcpParams.add("-" + ConfigConstants.OUTPUT_INDENTED); expectedMlcpParams.add("true");

    def.withMaxSplitSize(2);
    expectedMlcpParams.add("-" + ConfigConstants.MAX_SPLIT_SIZE);  expectedMlcpParams.add("2");

    def.withOutputFilePath("/some/dir");
    expectedMlcpParams.add("-" + ConfigConstants.OUTPUT_FILE_PATH); expectedMlcpParams.add("/some/dir");

    def.withOutputType(ExportDefinition.OutputType.ARCHIVE);
    expectedMlcpParams.add("-" + ConfigConstants.OUTPUT_TYPE);
    expectedMlcpParams.add(ExportDefinition.OutputType.ARCHIVE.toString().toLowerCase());

    def.withPathNamespace("other", "example.org/other");
    def.withPathNamespace("my", "example.org/my");
    expectedMlcpParams.add("-" + ConfigConstants.PATH_NAMESPACE);
    expectedMlcpParams.add("other,example.org/other,my,example.org/my");

    // let's do a quick check to make sure withPathNamespace works
    assertEquals(expectedMlcpParams, def.getMlcpArgs(JobType.EXPORT));

    EditableNamespaceContext namespaces = new EditableNamespaceContext();
    namespaces.put("my", "example.org/my2");
    // calling withPathNamespaces overwrites existing namespaces
    def.withPathNamespaces(namespaces);
    // overwrite the second to last element in the array
    expectedMlcpParams.set(expectedMlcpParams.size() - 2, "-" + ConfigConstants.PATH_NAMESPACE);
    // overwrite the last element in the array
    expectedMlcpParams.set(expectedMlcpParams.size() - 1, "my,example.org/my2");

    def.withQueryFilter(new MockCtsQueryDefinition("my cts query"));
    expectedMlcpParams.add("-" + ConfigConstants.QUERY_FILTER); expectedMlcpParams.add("my cts query");

    def.withSnapshot(true);
    expectedMlcpParams.add("-" + ConfigConstants.SNAPSHOT); expectedMlcpParams.add("true");

    assertEquals(expectedMlcpParams, def.getMlcpArgs(JobType.EXPORT));
  }
}
