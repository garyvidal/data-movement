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

import com.marklogic.client.io.Format;
import com.marklogic.contentpump.ConfigConstants;
import com.marklogic.datamovement.DataMovementManager;
import com.marklogic.datamovement.ImportDefinition.DataType;
import com.marklogic.datamovement.ImportDefinition.InputFileType;
import com.marklogic.datamovement.ImportDefinition.SequenceFileImportDefinition.SequenceValueType;
import com.marklogic.datamovement.JobTicket.JobType;
import com.marklogic.datamovement.JobTicket;
import com.marklogic.datamovement.impl.ImportDefinitionImpl;

public class ImportDefinitionTest {
  private DataMovementManager moveMgr = DataMovementManager.newInstance();

  @Test
  public void testSharedOptions() throws Exception {
    ImportDefinitionImpl<?> def = (ImportDefinitionImpl<?>) moveMgr.newImportDefinition();
    ArrayList<String> expectedMlcpParams = new ArrayList<String>();
    expectedMlcpParams.add(JobTicket.JobType.IMPORT.toString());

    def.withContentEncoding("UTF-8");
    expectedMlcpParams.add("-" + ConfigConstants.CONTENT_ENCODING); expectedMlcpParams.add("UTF-8");

    def.putDataType("a", DataType.NUMBER);
    def.putDataType("b", DataType.STRING);
    def.putDataType("c", DataType.BOOLEAN);
    expectedMlcpParams.add("-" + ConfigConstants.DATA_TYPE);
    expectedMlcpParams.add("a,number,b,string,c,boolean");

    def.withDocumentType(Format.BINARY);
    expectedMlcpParams.add("-" + ConfigConstants.DOCUMENT_TYPE); expectedMlcpParams.add("binary");

    def.withInputCompressed(true);
    expectedMlcpParams.add("-" + ConfigConstants.INPUT_COMPRESSED); expectedMlcpParams.add("true");

    def.withInputCompressionCodec("gzip");
    expectedMlcpParams.add("-" + ConfigConstants.INPUT_COMPRESSION_CODEC); expectedMlcpParams.add("gzip");

    def.withInputFilePath("/a/b/c");
    expectedMlcpParams.add("-" + ConfigConstants.INPUT_FILE_PATH); expectedMlcpParams.add("/a/b/c");

    def.withFilenameAsCollection(true);
    expectedMlcpParams.add("-" + ConfigConstants.OUTPUT_FILENAME_AS_COLLECTION); expectedMlcpParams.add("true");

    def.withInputFilePattern(".txt");
    expectedMlcpParams.add("-" + ConfigConstants.INPUT_FILE_PATTERN); expectedMlcpParams.add(".txt");

    def.withMaxSplitSize(2);
    expectedMlcpParams.add("-" + ConfigConstants.MAX_SPLIT_SIZE);  expectedMlcpParams.add("2");

    def.withMinSplitSize(1);
    expectedMlcpParams.add("-" + ConfigConstants.MIN_SPLIT_SIZE);  expectedMlcpParams.add("1");

    def.withOutputCleanDir(true);
    expectedMlcpParams.add("-" + ConfigConstants.OUTPUT_CLEANDIR); expectedMlcpParams.add("true");

    def.withOutputCollections("a", "b", "c");
    expectedMlcpParams.add("-" + ConfigConstants.OUTPUT_COLLECTIONS); expectedMlcpParams.add("a,b,c");

    def.withOutputDirectory("/some/dir");
    expectedMlcpParams.add("-" + ConfigConstants.OUTPUT_DIRECTORY); expectedMlcpParams.add("/some/dir");

    def.withOutputLanguage("en-us");
    expectedMlcpParams.add("-" + ConfigConstants.OUTPUT_LANGUAGE); expectedMlcpParams.add("en-us");

    def.withOutputPartition("somePartition");
    expectedMlcpParams.add("-" + ConfigConstants.OUTPUT_PARTITION); expectedMlcpParams.add("somePartition");

    def.withOutputPermissions("role1", "read", "role2", "update");
    expectedMlcpParams.add("-" + ConfigConstants.OUTPUT_PERMISSIONS); expectedMlcpParams.add("role1,read,role2,update");

    def.withOutputQuality(2);
    expectedMlcpParams.add("-" + ConfigConstants.OUTPUT_QUALITY); expectedMlcpParams.add("2");

    def.withOutputUriPrefix("/myPrefix/");
    expectedMlcpParams.add("-" + ConfigConstants.OUTPUT_URI_PREFIX); expectedMlcpParams.add("/myPrefix/");

    def.withOutputUriReplacements("/c:/files/", "", "/e:/prods/", "/products/");
    expectedMlcpParams.add("-" + ConfigConstants.OUTPUT_URI_REPLACE);
    expectedMlcpParams.add("/c:/files/,'',/e:/prods/,'/products/'");

    def.withOutputUriSuffix(".xml");
    expectedMlcpParams.add("-" + ConfigConstants.OUTPUT_URI_SUFFIX); expectedMlcpParams.add(".xml");

    def.withNamespace("myCompany");
    expectedMlcpParams.add("-" + ConfigConstants.NAMESPACE); expectedMlcpParams.add("myCompany");

    def.withTemporalCollection("compliance");
    expectedMlcpParams.add("-" + ConfigConstants.TEMPORAL_COLLECTION); expectedMlcpParams.add("compliance");

    def.withTolerateErrors(true);
    expectedMlcpParams.add("-" + ConfigConstants.TOLERATE_ERRORS); expectedMlcpParams.add("true");

    def.withTransform( moveMgr
      .newModuleTransform("/path/to/myModule.sjs", "myFunction", "http://marklogic.com/example/namespace")
        .addParameter("myParam", "test") );

    expectedMlcpParams.add("-" + ConfigConstants.TRANSFORM_MODULE);
    expectedMlcpParams.add("/path/to/myModule.sjs");

    expectedMlcpParams.add("-" + ConfigConstants.TRANSFORM_FUNCTION);
    expectedMlcpParams.add("myFunction");

    expectedMlcpParams.add("-" + ConfigConstants.TRANSFORM_NAMESPACE);
    expectedMlcpParams.add("http://marklogic.com/example/namespace");

    expectedMlcpParams.add("-" + ConfigConstants.TRANSFORM_PARAM);
    expectedMlcpParams.add("{\"myParam\":\"test\"}");

    def.withUriId("/test/doc1.json");
    expectedMlcpParams.add("-" + ConfigConstants.URI_ID); expectedMlcpParams.add("/test/doc1.json");

    def.withXmlRepairLevel(ImportDefinitionImpl.XmlRepairLevel.DEFAULT);
    expectedMlcpParams.add("-" + ConfigConstants.XML_REPAIR_LEVEL); expectedMlcpParams.add("default");

    assertEquals(expectedMlcpParams, def.getMlcpArgs(JobType.IMPORT));
  }

  @Test
  public void testAggregatesOptions() throws Exception {
    ArrayList<String> expectedMlcpParams = new ArrayList<String>();
    ImportDefinitionImpl<?> def = (ImportDefinitionImpl<?>)
      moveMgr.newImportDefinition()
        .withInputFileType(InputFileType.AGGREGATES)
        .withAggregateRecordElement("products")
        .withAggregateRecordNamespace("http://mycompany.com");
    expectedMlcpParams.add(JobTicket.JobType.IMPORT.toString());
    expectedMlcpParams.add("-" + ConfigConstants.INPUT_FILE_TYPE); expectedMlcpParams.add("aggregates");
    expectedMlcpParams.add("-" + ConfigConstants.AGGREGATE_RECORD_ELEMENT); expectedMlcpParams.add("products");
    expectedMlcpParams.add("-" + ConfigConstants.AGGREGATE_RECORD_NAMESPACE); expectedMlcpParams.add("http://mycompany.com");

    assertEquals(expectedMlcpParams, def.getMlcpArgs(JobType.IMPORT));
  }

  @Test
  public void testArchiveOptions() throws Exception {
    ArrayList<String> expectedMlcpParams = new ArrayList<String>();
    ImportDefinitionImpl<?> def = (ImportDefinitionImpl<?>)
      moveMgr.newImportDefinition()
        .withInputFileType(InputFileType.ARCHIVE)
        .withArchiveMetadataOptional(true)
        .withCopyCollections(false)
        .withCopyPermissions(false)
        .withCopyProperties (false)
        .withCopyQuality    (false);
    expectedMlcpParams.add(JobTicket.JobType.IMPORT.toString());
    expectedMlcpParams.add("-" + ConfigConstants.INPUT_FILE_TYPE); expectedMlcpParams.add("archive");
    expectedMlcpParams.add("-" + ConfigConstants.ARCHIVE_METADATA_OPTIONAL); expectedMlcpParams.add("true");
    expectedMlcpParams.add("-" + ConfigConstants.COPY_COLLECTIONS); expectedMlcpParams.add("false");
    expectedMlcpParams.add("-" + ConfigConstants.COPY_PERMISSIONS); expectedMlcpParams.add("false");
    expectedMlcpParams.add("-" + ConfigConstants.COPY_PROPERTIES); expectedMlcpParams.add("false");
    expectedMlcpParams.add("-" + ConfigConstants.COPY_QUALITY); expectedMlcpParams.add("false");

    assertEquals(expectedMlcpParams, def.getMlcpArgs(JobType.IMPORT));
  }

  @Test
  public void testDelimitedJsonOptions() throws Exception {
    ArrayList<String> expectedMlcpParams = new ArrayList<String>();
    ImportDefinitionImpl<?> def = (ImportDefinitionImpl<?>)
      moveMgr.newImportDefinition()
        .withInputFileType(InputFileType.DELIMITED_JSON)
        .withDelimiter("|")
        .withDelimitedRootName("document")
        .withGenerateUri(true)
        .withSplitInput(true);
    expectedMlcpParams.add(JobTicket.JobType.IMPORT.toString());
    expectedMlcpParams.add("-" + ConfigConstants.INPUT_FILE_TYPE); expectedMlcpParams.add("delimited_json");
    expectedMlcpParams.add("-" + ConfigConstants.DELIMITER); expectedMlcpParams.add("|");
    expectedMlcpParams.add("-" + ConfigConstants.DELIMITED_ROOT_NAME); expectedMlcpParams.add("document");
    expectedMlcpParams.add("-" + ConfigConstants.GENERATE_URI); expectedMlcpParams.add("true");
    expectedMlcpParams.add("-" + ConfigConstants.SPLIT_INPUT); expectedMlcpParams.add("true");

    assertEquals(expectedMlcpParams, def.getMlcpArgs(JobType.IMPORT));
  }

  @Test
  public void testDelimitedTextOptions() throws Exception {
    ArrayList<String> expectedMlcpParams = new ArrayList<String>();
    ImportDefinitionImpl<?> def = (ImportDefinitionImpl<?>)
      moveMgr.newImportDefinition()
        .withInputFileType(InputFileType.DELIMITED_TEXT);
    expectedMlcpParams.add(JobTicket.JobType.IMPORT.toString());
    expectedMlcpParams.add("-" + ConfigConstants.INPUT_FILE_TYPE); expectedMlcpParams.add("delimited_text");

    assertEquals(expectedMlcpParams, def.getMlcpArgs(JobType.IMPORT));
  }

  @Test
  public void testDocumentsOptions() throws Exception {
    ArrayList<String> expectedMlcpParams = new ArrayList<String>();
    ImportDefinitionImpl<?> def = (ImportDefinitionImpl<?>)
      moveMgr.newImportDefinition()
        .withInputFileType(InputFileType.DOCUMENTS)
        .withStreaming(true);
    expectedMlcpParams.add(JobTicket.JobType.IMPORT.toString());
    expectedMlcpParams.add("-" + ConfigConstants.INPUT_FILE_TYPE); expectedMlcpParams.add("documents");
    expectedMlcpParams.add("-" + ConfigConstants.STREAMING); expectedMlcpParams.add("true");

    assertEquals(expectedMlcpParams, def.getMlcpArgs(JobType.IMPORT));
  }

  @Test
  public void testForestOptions() throws Exception {
    ArrayList<String> expectedMlcpParams = new ArrayList<String>();
    ImportDefinitionImpl<?> def = (ImportDefinitionImpl<?>)
      moveMgr.newImportDefinition()
        .withInputFileType(InputFileType.FOREST)
        .withCollectionFilter("a", "b", "c")
        .withDirectoryFilter("a", "b", "c")
        .withTypeFilter("a", "b", "c");
    expectedMlcpParams.add(JobTicket.JobType.IMPORT.toString());
    expectedMlcpParams.add("-" + ConfigConstants.INPUT_FILE_TYPE); expectedMlcpParams.add("forest");
    expectedMlcpParams.add("-" + ConfigConstants.COLLECTION_FILTER); expectedMlcpParams.add("a,b,c");
    expectedMlcpParams.add("-" + ConfigConstants.DIRECTORY_FILTER); expectedMlcpParams.add("a,b,c");
    expectedMlcpParams.add("-" + ConfigConstants.TYPE_FILTER); expectedMlcpParams.add("a,b,c");

    assertEquals(expectedMlcpParams, def.getMlcpArgs(JobType.IMPORT));
  }

  @Test
  public void testRdfOptions() throws Exception {
    ArrayList<String> expectedMlcpParams = new ArrayList<String>();
    ImportDefinitionImpl<?> def = (ImportDefinitionImpl<?>)
      moveMgr.newImportDefinition()
        .withInputFileType(InputFileType.RDF)
        .withOutputGraph("outputGraph")
        .withOutputOverrideGraph("outputOverrideGraph");
    expectedMlcpParams.add(JobTicket.JobType.IMPORT.toString());
    expectedMlcpParams.add("-" + ConfigConstants.INPUT_FILE_TYPE); expectedMlcpParams.add("rdf");
    expectedMlcpParams.add("-" + ConfigConstants.OUTPUT_GRAPH); expectedMlcpParams.add("outputGraph");
    expectedMlcpParams.add("-" + ConfigConstants.OUTPUT_OVERRIDE_GRAPH); expectedMlcpParams.add("outputOverrideGraph");

    assertEquals(expectedMlcpParams, def.getMlcpArgs(JobType.IMPORT));
  }

  @Test
  public void testSequenceFileOptions() throws Exception {
    ArrayList<String> expectedMlcpParams = new ArrayList<String>();
    ImportDefinitionImpl<?> def = (ImportDefinitionImpl<?>)
      moveMgr.newImportDefinition()
        .withInputFileType(InputFileType.SEQUENCE_FILE)
        .withSequenceKeyClass("className")
        .withSequenceValueClass("valueClassName")
        .withSequenceValueType(SequenceValueType.BYTES_WRITABLE);
    expectedMlcpParams.add(JobTicket.JobType.IMPORT.toString());
    expectedMlcpParams.add("-" + ConfigConstants.INPUT_FILE_TYPE); expectedMlcpParams.add("sequencefile");
    expectedMlcpParams.add("-" + ConfigConstants.INPUT_SEQUENCEFILE_KEY_CLASS); expectedMlcpParams.add("className");
    expectedMlcpParams.add("-" + ConfigConstants.INPUT_SEQUENCEFILE_VALUE_CLASS); expectedMlcpParams.add("valueClassName");
    expectedMlcpParams.add("-" + ConfigConstants.INPUT_SEQUENCEFILE_VALUE_TYPE); expectedMlcpParams.add("BytesWritable");

    assertEquals(expectedMlcpParams, def.getMlcpArgs(JobType.IMPORT));
  }
}
