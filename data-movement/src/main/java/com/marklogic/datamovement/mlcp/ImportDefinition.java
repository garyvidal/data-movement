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

import com.marklogic.client.io.Format;
import com.marklogic.datamovement.impl.ImportDefinitionImpl;

import java.util.Map;

public interface ImportDefinition<T extends ImportDefinition<T>>
  extends JobDefinition<T>
{
  public T withContentEncoding(String charset);
  public String getContentEncoding();
  public T withFastload(boolean fastload);
  public boolean getFastload();
  public T withInputFilePath(String path);
  public String getInputFilePath();
  public T withInputFilePattern(String pattern);
  public String getInputFilePattern();
  public <I extends ImportDefinition<I>> I withInputFileType(InputFileType<I> type);
  public InputFileType<?> getInputFileType();
  public String getInputFileTypeValue();
  public T withMaxSplitSize(long splitSize);
  public long getMaxSplitSize();
  public T withMinSplitSize(long splitSize);
  public long getMinSplitSize();
  public T withOutputCleanDir(boolean cleanDirectory);
  public boolean getOutputCleanDir();
  public T withOutputCollections(String... collections);
  public String[] getOutputCollections();
  public T withOutputDirectory(String directory);
  public String getOutputDirectory();
  public T withOutputLanguage(String xmlLang);
  public String getOutputLanguage();
  public T withOutputPartition(String partition);
  public String getOutputPartition();
  public T withOutputPermission(String role, String capability);
  public T withOutputPermissions(String role, String capability, String... roleCapabilityPairs);
  public String[] getOutputPermissions();
  public T withOutputQuality(int quality);
  public int getOutputQuality();
  public T withOutputUriPrefix(String prefix);
  public String getOutputUriPrefix();
  public T withOutputUriReplacement(String pattern, String replacement);
  public T withOutputUriReplacements(String pattern, String replacement, String...patternReplacementPairs);
  public String[] getOutputUriReplace();
  public T withOutputUriSuffix(String suffix);
  public String getOutputUriSuffix();
  public T withTemporalCollection(String collection);
  public String getTemporalCollection();
  public T withTolerateErrors(boolean tolerateErrors);
  public boolean getTolerateErrors();
  public T withTransform(DataMovementTransform<?> transform);
  public DataMovementTransform<?> getTransform();
  public T withXmlRepairLevel(ImportDefinition.XmlRepairLevel xmlRepairLevel);
  public XmlRepairLevel getXmlRepairLevel();

  public enum XmlRepairLevel {
    DEFAULT,
    FULL,
    NONE;

    public String toString() {
      return super.toString().toLowerCase();
    }
  };

  public interface InputFileType<T extends ImportDefinition<T>>
  {
    public final InputFileType<AggregatesImportDefinition> AGGREGATES =
      ImportDefinitionImpl.InputFileTypeImpl.AGGREGATES;
    public final InputFileType<ArchiveImportDefinition> ARCHIVE =
      ImportDefinitionImpl.InputFileTypeImpl.ARCHIVE;
    public final InputFileType<DelimitedJsonImportDefinition> DELIMITED_JSON =
      ImportDefinitionImpl.InputFileTypeImpl.DELIMITED_JSON;
    public final InputFileType<DelimitedTextImportDefinition> DELIMITED_TEXT =
      ImportDefinitionImpl.InputFileTypeImpl.DELIMITED_TEXT;
    public final InputFileType<DocumentsImportDefinition> DOCUMENTS =
      ImportDefinitionImpl.InputFileTypeImpl.DOCUMENTS;
    public final InputFileType<ForestImportDefinition> FOREST =
      ImportDefinitionImpl.InputFileTypeImpl.FOREST;
    public final InputFileType<RdfImportDefinition> RDF =
      ImportDefinitionImpl.InputFileTypeImpl.RDF;
    public final InputFileType<SequenceFileImportDefinition> SEQUENCE_FILE =
      ImportDefinitionImpl.InputFileTypeImpl.SEQUENCE_FILE;

    public T newInstance(ImportDefinitionImpl<?> fromInstance);
  };

  public interface AggregatesImportDefinition extends ImportDefinition<AggregatesImportDefinition> {
    public AggregatesImportDefinition withAggregateRecordElement(String name);
    public String getAggregateRecordElement();
    public AggregatesImportDefinition withAggregateRecordNamespace(String namespace);
    public String getAggregateRecordNamespace();
    public AggregatesImportDefinition withFilenameAsCollection(boolean filenameAsCollection);
    public boolean getFilenameAsCollection();
    public AggregatesImportDefinition withInputCompressed(boolean compressed);
    public boolean getInputCompressed();
    public AggregatesImportDefinition withInputCompressionCodec(String codec);
    public String getInputCompressionCodec();
    public AggregatesImportDefinition withUriId(String name);
    public String getUriId();
  }

  public interface ArchiveImportDefinition extends ImportDefinition<ArchiveImportDefinition> {
    public ArchiveImportDefinition withArchiveMetadataOptional(boolean optional);
    public boolean getArchiveMetadataOptional();
    public ArchiveImportDefinition withCopyCollections(boolean copy);
    public boolean getCopyCollections();
    public ArchiveImportDefinition withCopyPermissions(boolean copy);
    public boolean getCopyPermissions();
    public ArchiveImportDefinition withCopyProperties (boolean copy);
    public boolean getCopyProperties ();
    public ArchiveImportDefinition withCopyQuality    (boolean copy);
    public boolean getCopyQuality    ();
    public ArchiveImportDefinition withFilenameAsCollection(boolean filenameAsCollection);
    public boolean getFilenameAsCollection();
    public ArchiveImportDefinition withInputCompressed(boolean compressed);
    public boolean getInputCompressed();
    public ArchiveImportDefinition withInputCompressionCodec(String codec);
    public String getInputCompressionCodec();
  }

  public interface DelimitedTextImportDefinition extends ImportDefinition<DelimitedTextImportDefinition> {
    public DelimitedTextImportDefinition putDataType(String property, DataType type);
    public DataType getDataType(String property);
    public Map<String,DataType> getDataTypes();
    public DelimitedTextImportDefinition withDocumentType(Format format);
    public Format getDocumentType();
    public DelimitedTextImportDefinition withFilenameAsCollection(boolean filenameAsCollection);
    public boolean getFilenameAsCollection();
    public DelimitedTextImportDefinition withInputCompressed(boolean compressed);
    public boolean getInputCompressed();
    public DelimitedTextImportDefinition withInputCompressionCodec(String codec);
    public String getInputCompressionCodec();
    public DelimitedTextImportDefinition withNamespace(String namespace);
    public String getNamespace();
    public DelimitedTextImportDefinition withUriId(String name);
    public String getUriId();
  }

  public interface DelimitedJsonImportDefinition extends ImportDefinition<DelimitedJsonImportDefinition> {
    public DelimitedJsonImportDefinition putDataType(String property, DataType type);
    public DataType getDataType(String property);
    public Map<String,DataType> getDataTypes();
    public DelimitedJsonImportDefinition withDelimiter(String delimeter);
    public String getDelimiter();
    public DelimitedJsonImportDefinition withDelimitedRootName(String name);
    public String getDelimitedRootName();
    public DelimitedJsonImportDefinition withFilenameAsCollection(boolean filenameAsCollection);
    public boolean getFilenameAsCollection();
    public DelimitedJsonImportDefinition withInputCompressed(boolean compressed);
    public boolean getInputCompressed();
    public DelimitedJsonImportDefinition withInputCompressionCodec(String codec);
    public String getInputCompressionCodec();
    public DelimitedJsonImportDefinition withGenerateUri(boolean generate);
    public boolean getGenerateUri();
    public DelimitedJsonImportDefinition withNamespace(String namespace);
    public String getNamespace();
    public DelimitedJsonImportDefinition withSplitInput(boolean split);
    public boolean getSplitInput();
    public DelimitedJsonImportDefinition withUriId(String name);
    public String getUriId();
  }

  public interface DocumentsImportDefinition extends ImportDefinition<DocumentsImportDefinition> {
    public DocumentsImportDefinition withDocumentType(Format format);
    public Format getDocumentType();
    public DocumentsImportDefinition withFilenameAsCollection(boolean filenameAsCollection);
    public boolean getFilenameAsCollection();
    public DocumentsImportDefinition withInputCompressed(boolean compressed);
    public boolean getInputCompressed();
    public DocumentsImportDefinition withInputCompressionCodec(String codec);
    public String getInputCompressionCodec();
    public DocumentsImportDefinition withStreaming(boolean stream);
    public boolean getStreaming();
  }

  public interface ForestImportDefinition extends ImportDefinition<ForestImportDefinition> {
    public ForestImportDefinition withCollectionFilter(String... forests);
    public String[] getCollectionFilter();
    public ForestImportDefinition withDirectoryFilter(String... directories);
    public String[] getDirectoryFilter();
    public ForestImportDefinition withTypeFilter(String... types);
    public String[] getTypeFilter();
  }

  public interface RdfImportDefinition extends ImportDefinition<RdfImportDefinition> {
    public RdfImportDefinition withInputCompressed(boolean compressed);
    public boolean getInputCompressed();
    public RdfImportDefinition withInputCompressionCodec(String codec);
    public String getInputCompressionCodec();
    public RdfImportDefinition withOutputGraph(String graph);
    public String getOutputGraph();
    public RdfImportDefinition withOutputOverrideGraph(String graph);
    public String getOutputOverrideGraph();
  }

  public interface SequenceFileImportDefinition extends ImportDefinition<SequenceFileImportDefinition> {
    public SequenceFileImportDefinition withDocumentType(Format format);
    public Format getDocumentType();
    public SequenceFileImportDefinition withFilenameAsCollection(boolean filenameAsCollection);
    public boolean getFilenameAsCollection();
    public SequenceFileImportDefinition withInputCompressed(boolean compressed);
    public boolean getInputCompressed();
    public SequenceFileImportDefinition withInputCompressionCodec(String codec);
    public String getInputCompressionCodec();
    public SequenceFileImportDefinition withSequenceKeyClass(String className);
    public String getSequenceKeyClass();
    public SequenceFileImportDefinition withSequenceValueClass(String className);
    public String getSequenceValueClass();
    public SequenceFileImportDefinition withSequenceValueType(SequenceValueType type);
    public SequenceValueType getSequenceValueType();


    public enum SequenceValueType {
      BYTES_WRITABLE("BytesWritable"), TEXT("Text");

      private String value;

      SequenceValueType(String value) {
        this.value = value;
      }

      public String toString() {
        return value;
      }

      public static SequenceValueType fromString(String value) {
        if ( "BytesWritable".equals(value) ) {
          return BYTES_WRITABLE;
        } else if ( "Text".equals(value) ) {
          return TEXT;
        } else {
          throw new IllegalArgumentException("Not a valid SequenceValueType:\"" + value + "\"");
        }
      }
    };
  }

  public enum DataType { STRING, NUMBER, BOOLEAN };
}
