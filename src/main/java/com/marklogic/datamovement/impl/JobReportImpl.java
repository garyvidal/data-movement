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

import com.marklogic.datamovement.DataMovementEvent;
import com.marklogic.datamovement.Batch;

import com.marklogic.client.Page;

import java.util.Date;

public class JobReportImpl<Y extends DataMovementEvent> {
    public long getBatchesCount() {
        // TODO: implement
        return 0;
    }

    public Page<Batch<Y>> getAllBatches(long start, int pageLength) {
        // TODO: implement
        return null;
    }

    public long getSuccessBatchesCount() {
        // TODO: implement
        return 0;
    }

    public Page<Batch<Y>> getSuccessBatches(long start, int pageLength) {
        // TODO: implement
        return null;
    }

    public long getFailureBatchesCount() {
        // TODO: implement
        return 0;
    }

    public Page<Batch<Y>> getFailureBatches(long start, int pageLength) {
        // TODO: implement
        return null;
    }

    public long getEventCount() {
        // TODO: implement
        return 0;
    }

    public long getSuccessCount() {
        // TODO: implement
        return 0;
    }

    public long getBytesMoved() {
        // TODO: implement
        return 0;
    }

    public boolean isJobComplete() {
        // TODO: implement
        return true;
    }

    public Date getReportTimestamp() {
        // TODO: implement
        return null;
    }

}
