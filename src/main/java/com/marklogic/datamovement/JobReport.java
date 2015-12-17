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

import com.marklogic.client.Page;

import java.util.Date;

public interface JobReport<Y extends DataMovementEvent> {
    public long getBatchesCount();
    public Page<Batch<Y>> getAllBatches(long start, int pageLength);
    public long getSuccessBatchesCount();
    public Page<Batch<Y>> getSuccessBatches(long start, int pageLength);
    public long getFailureBatchesCount();
    public Page<Batch<Y>> getFailureBatches(long start, int pageLength);
    public long getEventCount();
    public long getSuccessCount();
    public long getBytesMoved();
    public boolean isJobComplete();
    public Date getReportTimestamp();
}
