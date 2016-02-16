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

import com.marklogic.datamovement.ImportDefinition.AggregatesImportDefinition;
import com.marklogic.datamovement.ImportDefinition.ArchiveImportDefinition;
import com.marklogic.datamovement.ImportDefinition.DataType;
import com.marklogic.datamovement.ImportDefinition.DelimitedJsonImportDefinition;
import com.marklogic.datamovement.ImportDefinition.DelimitedTextImportDefinition;
import com.marklogic.datamovement.ImportDefinition.DocumentsImportDefinition;
import com.marklogic.datamovement.ImportDefinition.ForestImportDefinition;
import com.marklogic.datamovement.ImportDefinition.RdfImportDefinition;
import com.marklogic.datamovement.ImportDefinition.SequenceFileImportDefinition;
import com.marklogic.datamovement.ImportDefinition.SequenceFileImportDefinition.SequenceValueType;
import com.marklogic.datamovement.ImportDefinition.XmlRepairLevel;
import com.marklogic.datamovement.BatchFailureListener;
import com.marklogic.datamovement.BatchListener;
import com.marklogic.datamovement.DataMovementTransform;
import com.marklogic.datamovement.ImportDefinition;
import com.marklogic.datamovement.ImportEvent;
import com.marklogic.datamovement.JobTicket;
import com.marklogic.datamovement.impl.MlcpUtil;
import com.marklogic.client.io.Format;
import com.marklogic.contentpump.ConfigConstants;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class ImportDefinitionImpl<T extends ImportDefinition<T>>
  extends JobDefinitionImpl<T>
  implements ImportDefinition<T>
{
  private ArrayList<String> collections = new ArrayList<>();
  private Map<String,DataType> dataType = new LinkedHashMap<>();
  private InputFileType inputFileType;
  private ArrayList<String> outputPermissions = new ArrayList<>();
  private ArrayList<String> outputUriReplace = new ArrayList<>();
  private int quality;
  private DataMovementTransform transform;
  private ImportDefinition.XmlRepairLevel xmlRepairLevel;
  private BatchListener<ImportEvent> onSuccessListener;
  private BatchFailureListener<ImportEvent> onFailureListener;

  public ImportDefinitionImpl() {}

  public T withContentEncoding(String charset) {
    return withOption(ConfigConstants.CONTENT_ENCODING, charset);
  }

  public String getContentEncoding() {
    return getOption(ConfigConstants.CONTENT_ENCODING);
  }

  public T putDataType(String property, DataType type) {
    if ( property == null    ) throw new IllegalArgumentException("property must not be null");
    if ( property.equals("") ) throw new IllegalArgumentException("property must not be blank");
    if ( type == null        ) throw new IllegalArgumentException("type must not be null");
    this.dataType.put(property, type);
    String dataTypeOption = "";
    for ( String key : dataType.keySet() ) {
      if ( dataTypeOption.length() > 0 ) dataTypeOption += ",";
      dataTypeOption += key + "," + dataType.get(key).toString().toLowerCase();
    }
    return withOption(ConfigConstants.DATA_TYPE, dataTypeOption);
  }

  public DataType getDataType(String property) {
    return dataType.get(property);
  }

  public Map<String,DataType> getDataTypes() {
    return dataType;
  }

  public T withDocumentType(Format format) {
    if ( format == null ) throw new IllegalArgumentException("format must not be null");
    return withOption(ConfigConstants.DOCUMENT_TYPE, format.toString().toLowerCase());
  }

  public Format getDocumentType() {
    String docType = getOption(ConfigConstants.DOCUMENT_TYPE);
    if ( docType == null ) return null;
    return Format.valueOf(Format.class, docType.toUpperCase());
  }

  public T withInputCompressed(boolean compressed) {
    return withOption(ConfigConstants.INPUT_COMPRESSED, String.valueOf(compressed));
  }

  public boolean getInputCompressed() {
    return getBooleanOption(ConfigConstants.INPUT_COMPRESSED, false);
  }


  public T withInputCompressionCodec(String codec) {
    return withOption(ConfigConstants.INPUT_COMPRESSION_CODEC, codec);
  }

  public String getInputCompressionCodec() {
    return getOption(ConfigConstants.INPUT_COMPRESSION_CODEC);
  }


  public T withInputFilePath(String path) {
    return withOption(ConfigConstants.INPUT_FILE_PATH, path);
  }

  public String getInputFilePath() {
    return getOption(ConfigConstants.INPUT_FILE_PATH);
  }

  public <I extends ImportDefinition<I>> I withInputFileType(InputFileType<I> type) {
    if ( type == null ) throw new IllegalArgumentException("type must not be null");
    this.inputFileType = type;
    I newInstance = (I) type.newInstance(this);
    return newInstance.withOption(ConfigConstants.INPUT_FILE_TYPE, newInstance.getInputFileTypeValue());
  }

  public InputFileType<?> getInputFileType() {
    return inputFileType;
  }

  public String getInputFileTypeValue() {
    return null;
  }

  public static class InputFileTypeImpl<T extends ImportDefinition<T>> {
    public static final InputFileType<AggregatesImportDefinition>    AGGREGATES =
      fromInstance -> fromInstance.new AggregatesImportDefinitionImpl(fromInstance);
    public static final InputFileType<ArchiveImportDefinition>       ARCHIVE =
      fromInstance -> fromInstance.new ArchiveImportDefinitionImpl(fromInstance);
    public static final InputFileType<DelimitedJsonImportDefinition> DELIMITED_JSON =
      fromInstance -> fromInstance.new DelimitedJsonImportDefinitionImpl(fromInstance);
    public static final InputFileType<DelimitedTextImportDefinition> DELIMITED_TEXT =
      fromInstance -> fromInstance.new DelimitedTextImportDefinitionImpl(fromInstance);
    public static final InputFileType<DocumentsImportDefinition>     DOCUMENTS =
      fromInstance -> fromInstance.new DocumentsImportDefinitionImpl(fromInstance);
    public static final InputFileType<ForestImportDefinition>        FOREST =
      fromInstance -> fromInstance.new ForestImportDefinitionImpl(fromInstance);
    public static final InputFileType<RdfImportDefinition>           RDF =
      fromInstance -> fromInstance.new RdfImportDefinitionImpl(fromInstance);
    public static final InputFileType<SequenceFileImportDefinition>  SEQUENCE_FILE =
      fromInstance -> fromInstance.new SequenceFileImportDefinitionImpl(fromInstance);
  }

  public T withFilenameAsCollection(boolean filenameAsCollection) {
    return withOption(ConfigConstants.OUTPUT_FILENAME_AS_COLLECTION, String.valueOf(filenameAsCollection));
  }

  public boolean getFilenameAsCollection() {
    return getBooleanOption(ConfigConstants.OUTPUT_FILENAME_AS_COLLECTION, false);
  }


  public T withInputFilePattern(String pattern) {
    return withOption(ConfigConstants.INPUT_FILE_PATTERN, pattern);
  }

  public String getInputFilePattern() {
    return getOption(ConfigConstants.INPUT_FILE_PATTERN);
  }

  public T withMaxSplitSize(long splitSize) {
    return withOption(ConfigConstants.MAX_SPLIT_SIZE, String.valueOf(splitSize));
  }

  public long getMaxSplitSize() {
    return getLongOption(ConfigConstants.MAX_SPLIT_SIZE, Long.MAX_VALUE);
  }

  public T withMinSplitSize(long splitSize) {
    return withOption(ConfigConstants.MIN_SPLIT_SIZE, String.valueOf(splitSize));
  }

  public long getMinSplitSize() {
    return getLongOption(ConfigConstants.MIN_SPLIT_SIZE, Long.MAX_VALUE);
  }

  public T withOutputCleanDir(boolean cleanDirectory) {
    return withOption(ConfigConstants.OUTPUT_CLEANDIR, String.valueOf(cleanDirectory));
  }

  public boolean getOutputCleanDir() {
    return getBooleanOption(ConfigConstants.OUTPUT_CLEANDIR, false);
  }

  public T withOutputCollections(String... collections) {
    this.collections.clear();
    this.collections.addAll(Arrays.asList(collections));
    return withOption(ConfigConstants.OUTPUT_COLLECTIONS, String.join(",", collections));
  }

  public String[] getOutputCollections() {
    return collections.toArray(new String[collections.size()]);
  }

  public T withOutputDirectory(String directory) {
    return withOption(ConfigConstants.OUTPUT_DIRECTORY, directory);
  }

  public String getOutputDirectory() {
    return getOption(ConfigConstants.OUTPUT_DIRECTORY);
  }

  public T withOutputLanguage(String xmlLang) {
    return withOption(ConfigConstants.OUTPUT_LANGUAGE, xmlLang);
  }

  public String getOutputLanguage() {
    return getOption(ConfigConstants.OUTPUT_LANGUAGE);
  }

  public T withOutputPartition(String partition) {
    return withOption(ConfigConstants.OUTPUT_PARTITION, partition);
  }

  public String getOutputPartition() {
    return getOption(ConfigConstants.OUTPUT_PARTITION);
  }

  public T withOutputPermissions(String role, String capability) {
    outputPermissions.clear();
    if ( role == null )       throw new IllegalArgumentException("role must not be null");
    if ( capability == null ) throw new IllegalArgumentException("capability must not be null");
    outputPermissions.add(role); outputPermissions.add(capability);
    return withOption(ConfigConstants.OUTPUT_PERMISSIONS, String.join(",", outputPermissions));
  }

  public T withOutputPermissions(String role, String capability, String... roleCapabilityPairs) {
    withOutputPermissions(role, capability);
    if ( roleCapabilityPairs == null ) return (T) this;
    this.outputPermissions.addAll(Arrays.asList(roleCapabilityPairs));
    return withOption(ConfigConstants.OUTPUT_PERMISSIONS, String.join(",", outputPermissions));
}

  public String[] getOutputPermissions() {
    return outputPermissions.toArray(new String[outputPermissions.size()]);
  }

  public T withOutputQuality(int quality) {
    this.quality = quality;
    return withOption(ConfigConstants.OUTPUT_QUALITY, String.valueOf(quality));
  }

  public int getOutputQuality() {
    return quality;
  }

  public T withOutputUriPrefix(String prefix) {
    return withOption(ConfigConstants.OUTPUT_URI_PREFIX, prefix);
  }

  public String getOutputUriPrefix() {
    return getOption(ConfigConstants.OUTPUT_URI_PREFIX);
  }

  public T withOutputUriReplace(String pattern, String replacement) {
    outputUriReplace.clear();
    if ( pattern == null )     throw new IllegalArgumentException("pattern must not be null");
    if ( replacement == null ) throw new IllegalArgumentException("replacement must not be null");
    outputUriReplace.add(pattern); outputUriReplace.add(replacement);
    return withOption(ConfigConstants.OUTPUT_URI_REPLACE, MlcpUtil.combineRegexPairs(outputUriReplace));
  }

  public T withOutputUriReplace(String pattern, String replacement, String...patternReplacementPairs) {
    withOutputUriReplace(pattern, replacement);
    outputUriReplace.addAll(Arrays.asList(patternReplacementPairs));
    return withOption(ConfigConstants.OUTPUT_URI_REPLACE, MlcpUtil.combineRegexPairs(outputUriReplace));
  }

  public String[] getOutputUriReplace() {
    return outputUriReplace.toArray(new String[0]);
  }
  public T withOutputUriSuffix(String suffix) {
    return withOption(ConfigConstants.OUTPUT_URI_SUFFIX, suffix);
  }

  public String getOutputUriSuffix() {
    return getOption(ConfigConstants.OUTPUT_URI_SUFFIX);
  }

  public T withNamespace(String namespace) {
    return withOption(ConfigConstants.NAMESPACE, namespace);
  }

  public String getNamespace() {
    return getOption(ConfigConstants.NAMESPACE);
  }

  public T withTemporalCollection(String collection) {
    return withOption(ConfigConstants.TEMPORAL_COLLECTION, collection);
  }

  public String getTemporalCollection() {
    return getOption(ConfigConstants.TEMPORAL_COLLECTION);
  }

  public T withTolerateErrors(boolean tolerateErrors) {
    return withOption(ConfigConstants.TOLERATE_ERRORS, String.valueOf(tolerateErrors));
  }

  public boolean getTolerateErrors() {
    return getBooleanOption(ConfigConstants.ARCHIVE_METADATA_OPTIONAL, false);
  }

  public T withTransform(DataMovementTransform<?> transform) {
    this.transform = transform;
    MlcpUtil.clearOptionsForTransforms(getOptions());
    return withOptions( MlcpUtil.optionsForTransforms(transform) );
  }

  public DataMovementTransform<?> getTransform() {
    return transform;
  }

  public T withUriId(String uriId) {
    return withOption(ConfigConstants.URI_ID, uriId);
  }

  public String getUriId() {
    return getOption(ConfigConstants.URI_ID);
  }


  public T withXmlRepairLevel(ImportDefinition.XmlRepairLevel xmlRepairLevel) {
    this.xmlRepairLevel = xmlRepairLevel;
    if ( xmlRepairLevel == null ) {
      return removeOption(ConfigConstants.XML_REPAIR_LEVEL);
    }
    return withOption(ConfigConstants.XML_REPAIR_LEVEL, xmlRepairLevel.toString());
  }

  public ImportDefinition.XmlRepairLevel getXmlRepairLevel() {
    return xmlRepairLevel;
  }

  public T onBatchSuccess(BatchListener<ImportEvent> listener) {
    this.onSuccessListener = listener;
    return (T) this;
  }

  public T onBatchFailure(BatchFailureListener<ImportEvent> listener) {
    this.onFailureListener = listener;
    return (T) this;
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

  public class AggregatesImportDefinitionImpl
    extends ImportDefinitionImpl<AggregatesImportDefinition>
    implements AggregatesImportDefinition
  {
    public String getInputFileTypeValue() {
      return "aggregates";
    }

    public AggregatesImportDefinitionImpl(ImportDefinitionImpl<?> fromInstance) {
      withOptions(fromInstance.getOptions());
    }

    public AggregatesImportDefinition withAggregateRecordElement(String name) {
      return withOption(ConfigConstants.AGGREGATE_RECORD_ELEMENT, name);
    }

    public String getAggregateRecordElement() {
      return getOption(ConfigConstants.AGGREGATE_RECORD_ELEMENT);
    }

    public AggregatesImportDefinition withAggregateRecordNamespace(String namespace) {
      return withOption(ConfigConstants.AGGREGATE_RECORD_NAMESPACE, namespace);
    }

    public String getAggregateRecordNamespace() {
      return getOption(ConfigConstants.AGGREGATE_RECORD_NAMESPACE);
    }

  }

  public class ArchiveImportDefinitionImpl
    extends ImportDefinitionImpl<ArchiveImportDefinition>
    implements ArchiveImportDefinition
  {
    public String getInputFileTypeValue() {
      return "archive";
    }

    public ArchiveImportDefinitionImpl(ImportDefinitionImpl<?> fromInstance) {
      withOptions(fromInstance.getOptions());
    }

    public ArchiveImportDefinition withArchiveMetadataOptional(boolean optional) {
      return withOption(ConfigConstants.ARCHIVE_METADATA_OPTIONAL, String.valueOf(optional));
    }

    public boolean getArchiveMetadataOptional() {
      return getBooleanOption(ConfigConstants.ARCHIVE_METADATA_OPTIONAL, false);
    }


    public ArchiveImportDefinition withCopyCollections(boolean copy) {
      return withOption(ConfigConstants.COPY_COLLECTIONS, String.valueOf(copy));
    }

    public boolean getCopyCollections() {
      return getBooleanOption(ConfigConstants.COPY_COLLECTIONS, true);
    }

    public ArchiveImportDefinition withCopyPermissions(boolean copy) {
      return withOption(ConfigConstants.COPY_PERMISSIONS, String.valueOf(copy));
    }

    public boolean getCopyPermissions() {
      return getBooleanOption(ConfigConstants.COPY_PERMISSIONS, true);
    }

    public ArchiveImportDefinition withCopyProperties (boolean copy) {
      return withOption(ConfigConstants.COPY_PROPERTIES, String.valueOf(copy));
    }

    public boolean getCopyProperties() {
      return getBooleanOption(ConfigConstants.COPY_PROPERTIES, true);
    }

    public ArchiveImportDefinition withCopyQuality    (boolean copy) {
      return withOption(ConfigConstants.COPY_QUALITY, String.valueOf(copy));
    }

    public boolean getCopyQuality() {
      return getBooleanOption(ConfigConstants.COPY_QUALITY, true);
    }
  }

  public class DelimitedJsonImportDefinitionImpl
    extends ImportDefinitionImpl<DelimitedJsonImportDefinition>
    implements DelimitedJsonImportDefinition
  {
    public String getInputFileTypeValue() {
      return "delimited_json";
    }

    public DelimitedJsonImportDefinitionImpl(ImportDefinitionImpl<?> fromInstance) {
      withOptions(fromInstance.getOptions());
    }

    public DelimitedJsonImportDefinition withDelimiter(String delimeter) {
      return withOption(ConfigConstants.DELIMITER, delimeter);
    }

    public String getDelimiter() {
      return getOption(ConfigConstants.DELIMITER);
    }

    public DelimitedJsonImportDefinition withDelimitedRootName(String name) {
      return withOption(ConfigConstants.DELIMITED_ROOT_NAME, name);
    }

    public String getDelimitedRootName() {
      return getOption(ConfigConstants.DELIMITED_ROOT_NAME);
    }

    public DelimitedJsonImportDefinition withGenerateUri(boolean generate) {
      return withOption(ConfigConstants.GENERATE_URI, String.valueOf(generate));
    }

    public boolean getGenerateUri() {
      return getBooleanOption(ConfigConstants.GENERATE_URI, false);
    }

    public DelimitedJsonImportDefinition withSplitInput(boolean split) {
      return withOption(ConfigConstants.SPLIT_INPUT, String.valueOf(split));
    }

    public boolean getSplitInput() {
      return getBooleanOption(ConfigConstants.SPLIT_INPUT, false);
    }

  }

  public class DelimitedTextImportDefinitionImpl
    extends ImportDefinitionImpl<DelimitedTextImportDefinition>
    implements DelimitedTextImportDefinition
  {
    public String getInputFileTypeValue() {
      return "delimited_text";
    }

    public DelimitedTextImportDefinitionImpl(ImportDefinitionImpl<?> fromInstance) {
      withOptions(fromInstance.getOptions());
    }

  }

  public class DocumentsImportDefinitionImpl
    extends ImportDefinitionImpl<DocumentsImportDefinition>
    implements DocumentsImportDefinition
  {
    public String getInputFileTypeValue() {
      return "documents";
    }

    public DocumentsImportDefinitionImpl(ImportDefinitionImpl<?> fromInstance) {
      withOptions(fromInstance.getOptions());
    }

    public DocumentsImportDefinition withStreaming(boolean stream) {
      return withOption(ConfigConstants.STREAMING, String.valueOf(stream));
    }

    public boolean getStreaming() {
      return getBooleanOption(ConfigConstants.STREAMING, false);
    }

  }

  public class ForestImportDefinitionImpl
    extends ImportDefinitionImpl<ForestImportDefinition>
    implements ForestImportDefinition
  {
    public String getInputFileTypeValue() {
      return "forest";
    }

    public ForestImportDefinitionImpl(ImportDefinitionImpl<?> fromInstance) {
      withOptions(fromInstance.getOptions());
    }

    private ArrayList<String> collectionFilters = new ArrayList<>();
    private ArrayList<String> directoryFilters  = new ArrayList<>();
    private ArrayList<String> typeFilters       = new ArrayList<>();

    public ForestImportDefinition withCollectionFilter(String... forests) {
      this.collectionFilters.clear();
      this.collectionFilters.addAll(Arrays.asList(forests));
      return withOption(ConfigConstants.OUTPUT_COLLECTIONS, String.join(",", collectionFilters));
    }

    public String[] getCollectionFilter() {
      return collectionFilters.toArray(new String[collectionFilters.size()]);
    }

    public ForestImportDefinition withDirectoryFilter(String... directories) {
      this.directoryFilters.clear();
      this.directoryFilters.addAll(Arrays.asList(directories));
      return withOption(ConfigConstants.OUTPUT_COLLECTIONS, String.join(",", directoryFilters));
    }

    public String[] getDirectoryFilter() {
      return directoryFilters.toArray(new String[directoryFilters.size()]);
    }

    public ForestImportDefinition withTypeFilter(String... types) {
      this.typeFilters.clear();
      this.typeFilters.addAll(Arrays.asList(types));
      return withOption(ConfigConstants.OUTPUT_COLLECTIONS, String.join(",", typeFilters));
    }

    public String[] getTypeFilter() {
      return typeFilters.toArray(new String[typeFilters.size()]);
    }
  }

  public class RdfImportDefinitionImpl
    extends ImportDefinitionImpl<RdfImportDefinition>
    implements RdfImportDefinition
  {
    public String getInputFileTypeValue() {
      return "rdf";
    }

    public RdfImportDefinitionImpl(ImportDefinitionImpl<?> fromInstance) {
      withOptions(fromInstance.getOptions());
    }

    public RdfImportDefinition withOutputGraph(String graph) {
      return withOption(ConfigConstants.OUTPUT_GRAPH, graph);
    }

    public String getOutputGraph() {
      return getOption(ConfigConstants.OUTPUT_GRAPH);
    }

    public RdfImportDefinition withOutputOverrideGraph(String graph) {
      return withOption(ConfigConstants.OUTPUT_OVERRIDE_GRAPH, graph);
    }

    public String getOutputOverrideGraph() {
      return getOption(ConfigConstants.OUTPUT_OVERRIDE_GRAPH);
    }

  }

  public class SequenceFileImportDefinitionImpl
    extends ImportDefinitionImpl<SequenceFileImportDefinition>
    implements SequenceFileImportDefinition
  {
    public String getInputFileTypeValue() {
      return "sequencefile";
    }

    public SequenceFileImportDefinitionImpl(ImportDefinitionImpl<?> fromInstance) {
      withOptions(fromInstance.getOptions());
    }

    public SequenceFileImportDefinition withSequenceKeyClass(String className) {
      return withOption(ConfigConstants.INPUT_SEQUENCEFILE_KEY_CLASS, className);
    }

    public String getSequenceKeyClass() {
      return getOption(ConfigConstants.INPUT_SEQUENCEFILE_KEY_CLASS);
    }

    public SequenceFileImportDefinition withSequenceValueClass(String className) {
      return withOption(ConfigConstants.INPUT_SEQUENCEFILE_VALUE_CLASS, className);
    }

    public String getSequenceValueClass() {
      return getOption(ConfigConstants.INPUT_SEQUENCEFILE_KEY_CLASS);
    }

    public SequenceFileImportDefinition withSequenceValueType(SequenceValueType type) {
      if ( type == null ) throw new IllegalArgumentException("type must not be null");
      return withOption(ConfigConstants.INPUT_SEQUENCEFILE_VALUE_TYPE, type.toString());
    }

    public SequenceValueType getSequenceValueType() {
      return SequenceValueType.fromString(
        getOption(ConfigConstants.INPUT_SEQUENCEFILE_VALUE_TYPE)
      );
    }
  }
}
