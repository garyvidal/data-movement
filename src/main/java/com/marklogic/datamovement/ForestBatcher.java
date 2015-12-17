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
//import com.marklogic.client.io.InputFormatHandle;

public interface ForestBatcher {
    public ForestBatcher jobName(String jobName);
    public ForestBatcher batchSize(int batchSize);
    /* update every <interval> milliseconds */
    public ForestBatcher forestConfigUpdateInterval(long interval);
    /* flush every <interval> milliseconds */
    public ForestBatcher autoFlushInterval(long interval);
    public ForestBatcher add(String uri, AbstractWriteHandle contentHandle);
    public ForestBatcher add(String uri, DocumentMetadataWriteHandle metadataHandle,
            AbstractWriteHandle contentHandle);
    public ForestBatcher onBatchFull(ForestBatchFullListener listener);
    public ForestBatcher onBatchSuccess(BatchListener<CustomEvent> listener);
    public ForestBatcher onBatchFailure(BatchFailureListener<CustomEvent> listener);
    /* treat any remaining batches as if they're full and send them to
     * ForestBatchFullListener
     */
    public void flush();
    public void finalize(); // calls flush()
}
