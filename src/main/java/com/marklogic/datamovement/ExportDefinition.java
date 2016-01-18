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

import com.marklogic.client.query.CtsQueryDefinition;

public interface ExportDefinition extends JobDefinition<ExportDefinition> {
  ExportDefinition query(CtsQueryDefinition query);
  ExportDefinition outputFilePath(String path);
  ExportDefinition outputCompress(boolean compress);
  ExportDefinition transform(DataMovementTransform transform);
  ExportDefinition onBatchSuccess(BatchListener<ExportEvent> listener);
  ExportDefinition onOutputFileSuccess(BatchListener<ExportEvent> listener);
  ExportDefinition onBatchFailure(BatchFailureListener<ExportEvent> listener);
}
