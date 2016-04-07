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

import com.marklogic.client.io.Format;

public class QueryHostException extends Exception implements QueryEvent {
  private QueryEvent queryEvent;

  public QueryHostException(QueryEvent queryEvent, Throwable cause) {
    super(cause);
    this.queryEvent = queryEvent;
  }
  public long getBytesMoved() {
    return queryEvent.getBytesMoved();
  }

  public long getJobRecordNumber() {
    return queryEvent.getJobRecordNumber();
  }

  public long getBatchRecordNumber() {
    return queryEvent.getBatchRecordNumber();
  }

  public String getSourceUri() {
    return queryEvent.getSourceUri();
  }

  public Forest getSourceForest() {
    return queryEvent.getSourceForest();
  }

  public Format getFormat() {
    return queryEvent.getFormat();
  }

  public String getMimetype() {
    return queryEvent.getMimetype();
  }
}
