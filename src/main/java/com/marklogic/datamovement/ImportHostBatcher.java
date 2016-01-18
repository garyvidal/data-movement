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

import com.marklogic.client.document.DocumentDescriptor;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.document.DocumentWriteSet;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.client.io.marker.DocumentMetadataWriteHandle;

public interface ImportHostBatcher extends HostBatcher<ImportHostBatcher> {
  public ImportHostBatcher add(String uri, AbstractWriteHandle contentHandle);
  public ImportHostBatcher add(String uri, DocumentMetadataWriteHandle metadataHandle,
      AbstractWriteHandle contentHandle);
  public ImportHostBatcher onBatchSuccess(BatchListener<ImportEvent> listener);
  public ImportHostBatcher onBatchFailure(BatchFailureListener<ImportEvent> listener);
  /* treat any remaining batches as if they're full and send them
  */
  public void flush();
  public void finalize(); // calls flush()
  /* flush every <interval> milliseconds */
  public ImportHostBatcher autoFlushInterval(long interval);
  public ImportHostBatcher fastload(boolean fastload);
  public ImportHostBatcher transactionSize(int transactionSize);
  public ImportHostBatcher streaming(boolean streaming);
  public ImportHostBatcher temporalCollection(String collection);
  public ImportHostBatcher tolerateErrors(boolean tolerateErrors);
  public ImportHostBatcher transform(DataMovementTransform transform);
  public ImportHostBatcher outputPartition(String partition);
  public ImportHostBatcher xmlRepairLevel(ImportDefinition.XmlRepairLevel repairLevel);
}
