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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.query.CtsQueryDefinition;
import com.marklogic.client.util.EditableNamespaceContext;
import com.marklogic.client.util.IterableNamespaceContext;
import com.marklogic.contentpump.ConfigConstants;
import com.marklogic.datamovement.BatchFailureListener;
import com.marklogic.datamovement.BatchListener;
import com.marklogic.datamovement.DataMovementTransform;
import com.marklogic.datamovement.CopyDefinition;
import com.marklogic.datamovement.CopyEvent;

public class CopyDefinitionImpl
  extends JobDefinitionImpl<CopyDefinition>
  implements CopyDefinition
{
  private DatabaseClient inputClient;
  private DatabaseClient outputClient;

  public CopyDefinitionImpl() {}

  public CopyDefinition withInputClient(DatabaseClient client) {
    if ( client == null ) throw new IllegalArgumentException("client must not be null");
    this.inputClient = client;
    withOption(ConfigConstants.INPUT_HOST, client.getHost());
    withOption(ConfigConstants.INPUT_PORT, String.valueOf(client.getPort()));
    if ( client.getUser() != null )     withOption(ConfigConstants.INPUT_USERNAME, client.getUser());
    if ( client.getPassword() != null ) withOption(ConfigConstants.INPUT_PASSWORD, client.getPassword());
    if ( client.getDatabase() != null ) withOption(ConfigConstants.INPUT_DATABASE, client.getDatabase());
    return this;
  }

  public DatabaseClient getInputClient() {
    return inputClient;
  }

  public CopyDefinition withOutputClient(DatabaseClient client) {
    if ( client == null ) throw new IllegalArgumentException("client must not be null");
    this.outputClient = client;
    withOption(ConfigConstants.OUTPUT_HOST, client.getHost());
    withOption(ConfigConstants.OUTPUT_PORT, String.valueOf(client.getPort()));
    if ( client.getUser() != null )     withOption(ConfigConstants.OUTPUT_USERNAME, client.getUser());
    if ( client.getPassword() != null ) withOption(ConfigConstants.OUTPUT_PASSWORD, client.getPassword());
    if ( client.getDatabase() != null ) withOption(ConfigConstants.OUTPUT_DATABASE, client.getDatabase());
    return this;
  }

  public DatabaseClient getOutputClient() {
    return outputClient;
  }

  public CopyDefinition onBatchSuccess(BatchListener<CopyEvent> listener) {
    throw new IllegalStateException("this feature is not yet implemented");
  }

  public CopyDefinition onBatchFailure(BatchFailureListener<CopyEvent> listener) {
    throw new IllegalStateException("this feature is not yet implemented");
  }
}
