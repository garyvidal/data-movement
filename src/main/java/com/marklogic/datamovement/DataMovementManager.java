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

import com.marklogic.client.DatabaseClient;

public class DataMovementManager {
    private DataMovementManager() {
        // TODO: implement
    }

    public static DataMovementManager newInstance() {
        return new DataMovementManager();
    }

    public JobTicket<CustomJobReport> custom(DatabaseClient client) {
        // TODO: implement
        return null;
    }

    public <X> X getJobReport(JobTicket<X> ticket) {
        // TODO: implement
        return null;
    }

    public void stopJob(JobTicket<?> ticket) {
        // TODO: implement
    }

    public ForestBatcher newForestBatcher(DatabaseClient client) {
        return new ForestBatcherImpl(client);
    }
}
