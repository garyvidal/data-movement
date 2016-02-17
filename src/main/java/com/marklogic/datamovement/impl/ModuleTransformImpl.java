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

import com.marklogic.client.query.CtsQueryDefinition;
import com.marklogic.datamovement.ModuleTransform;

public class ModuleTransformImpl extends DataMovementTransformImpl<ModuleTransform> implements ModuleTransform {
  private String modulePath;
  private String functionName;
  private String functionNamespace;

  public ModuleTransformImpl(String modulePath, String functionName, String functionNamespace) {
    this.modulePath = modulePath;
    this.functionName = functionName;
    this.functionNamespace = functionNamespace;
  }

  public String getModulePath() {
    return modulePath;
  }
  public String getFunctionName() {
    return functionName;
  }
  public String getFunctionNamespace() {
    return functionNamespace;
  }
}

