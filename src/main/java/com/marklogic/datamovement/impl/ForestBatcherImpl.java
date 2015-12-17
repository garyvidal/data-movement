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

import com.marklogic.datamovement.CustomEvent;
import com.marklogic.datamovement.ForestBatcher;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.document.DocumentDescriptor;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.client.io.marker.DocumentMetadataWriteHandle;
//import com.marklogic.client.io.InputFormatHandle;

import java.util.LinkedHashSet;

public class ForestBatcherImpl
    extends LinkedHashSet<DocumentWriteOperation>
    implements ForestBatcher
{
    public ForestBatcherImpl(DatabaseClient client) {
        super();
        // TODO: implement
    }

    public ForestBatcher jobName(String jobName) {
        // TODO: implement
        return this;
    }

    public ForestBatcher batchSize(int batchSize) {
        // TODO: implement
        return this;
    }

    /* update every <interval> milliseconds */
    public ForestBatcher forestConfigUpdateInterval(long interval) {
        // TODO: implement
        return this;
    }

    /* flush every <interval> milliseconds */
    public ForestBatcher autoFlushInterval(long interval) {
        // TODO: implement
        return this;
    }

    public ForestBatcher add(String uri, AbstractWriteHandle contentHandle) {
        // TODO: implement
        return this;
    }

    public ForestBatcher add(String uri, DocumentMetadataWriteHandle metadataHandle,
            AbstractWriteHandle contentHandle) {
        // TODO: implement
        return this;
    }

    public ForestBatcher onBatchFull(ForestBatchFullListener listener) {
        // TODO: implement
        return this;
    }

    public ForestBatcher onBatchSuccess(BatchListener<CustomEvent> listener) {
        // TODO: implement
        return this;
    }
    public ForestBatcher onBatchFailure(BatchFailureListener<CustomEvent> listener) {
        // TODO: implement
        return this;
    }

    /* treat any remaining batches as if they're full and send them to
     * ForestBatchFullListener
     */
    public void flush() {
        // TODO: implement
    }

    public void finalize() {
        // TODO: implement
    }
}
