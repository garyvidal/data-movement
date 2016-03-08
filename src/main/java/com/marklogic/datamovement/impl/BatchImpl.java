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

import java.util.Calendar;

import com.marklogic.datamovement.Batch;
import com.marklogic.datamovement.DataMovementEvent;
import com.marklogic.datamovement.Forest;
import com.marklogic.datamovement.JobTicket;

public class BatchImpl<T extends DataMovementEvent> implements Batch<T> {
    private long batchNumber;
    private T[] items;
    private Calendar timestamp;
    private Forest forest;
    private long bytesMoved;
    private JobTicket jobTicket;

    public long getBatchNumber() {
        return batchNumber;
    }

    public void setBatchNumber(long batchNumber) {
        this.batchNumber = batchNumber;
    }

    public T[] getItems() {
        return items;
    }

    public void setItems(T[] items) {
        this.items = items;
    }

    public Calendar getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Calendar timestamp) {
        this.timestamp = timestamp;
    }

    public Forest getForest() {
        return forest;
    }

    public void setForest(Forest forest) {
        this.forest = forest;
    }

    public long getBytesMoved() {
        return bytesMoved;
    }

    public void setBytesMoved(long bytesMoved) {
        this.bytesMoved = bytesMoved;
    }

    public JobTicket getJobTicket() {
        return jobTicket;
    }

    public void setJobTicket(JobTicket jobTicket) {
        this.jobTicket = jobTicket;
    }
}
