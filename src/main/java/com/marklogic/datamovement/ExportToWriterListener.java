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

import java.io.IOException;
import java.io.Writer;
import java.util.Set;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.document.DocumentManager;
import com.marklogic.client.document.DocumentPage;
import com.marklogic.client.document.DocumentRecord;
import com.marklogic.client.document.GenericDocumentManager;
import com.marklogic.client.document.ServerTransform;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.QueryManager;

public class ExportToWriterListener implements BatchListener<String> {
  private Writer writer;
  private String suffix;
  private String prefix;
  private ServerTransform transform;
  private QueryManager.QueryView view;
  private Set<DocumentManager.Metadata> categories;
  private Format nonDocumentFormat;

  public ExportToWriterListener(Writer writer) {
    this.writer = writer;
  }

  public void processEvent(DatabaseClient client, Batch<String> batch) {
    GenericDocumentManager docMgr = client.newDocumentManager();
    if ( view              != null ) docMgr.setSearchView(view);
    if ( categories        != null ) docMgr.setMetadataCategories(categories);
    if ( nonDocumentFormat != null ) docMgr.setNonDocumentFormat(nonDocumentFormat);
    DocumentPage docs = docMgr.read( transform, batch.getItems() );
    synchronized(writer) {
      for ( DocumentRecord doc : docs ) {
        Format format = doc.getFormat();
        if ( Format.BINARY.equals(format) ) {
          throw new IllegalStateException("Document " + doc.getUri() +
            " is binary and cannot be written.  Change your query to not select any binary documents.");
        } else {
          try {
            if ( prefix != null ) writer.write( prefix );
            writer.write( doc.getContent(new StringHandle()).get() );
            if ( suffix != null ) writer.write( suffix );
          } catch (IOException e) {
              throw new DataMovementException("Failed to write document \"" + doc.getUri() + "\"", e);
          }
        }
      }
    }
  }

  public ExportToWriterListener withRecordSuffix(String suffix) {
    this.suffix = suffix;
    return this;
  }

  public ExportToWriterListener withRecordPrefix(String prefix) {
    this.prefix = prefix;
    return this;
  }

  public ExportToWriterListener withTransform(ServerTransform transform) {
    this.transform = transform;
    return this;
  }

  public ExportToWriterListener withSearchView(QueryManager.QueryView view) {
    this.view = view;
    return this;
  }

  public ExportToWriterListener withMetadataCategories(Set<DocumentManager.Metadata> categories) {
    this.categories = categories;
    return this;
  }

  public ExportToWriterListener withNonDocumentFormat(Format nonDocumentFormat) {
    this.nonDocumentFormat = nonDocumentFormat;
    return this;
  }
}
