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
package com.marklogic.datamovement.test;

import static org.junit.Assert.assertEquals;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Test;

import com.marklogic.contentpump.ConfigConstants;
import com.marklogic.datamovement.DataMovementManager;
import com.marklogic.datamovement.impl.MlcpUtil;
import com.marklogic.datamovement.mlcp.ModuleTransform;

public class MlcpUtilTest {
  private DataMovementManager moveMgr = DataMovementManager.newInstance();

  @Test
  public void testArgsForTransforms() throws Exception {
    LinkedHashMap<String,String> expectedMlcpParams = new LinkedHashMap<>();
    ModuleTransform transform = moveMgr.newModuleTransform(
      "/path/to/myModule.sjs", "myFunction", "http://marklogic.com/example/namespace");
    transform.addParameter("myParam", "test");

    expectedMlcpParams.put(ConfigConstants.TRANSFORM_MODULE,
      "/path/to/myModule.sjs");

    expectedMlcpParams.put(ConfigConstants.TRANSFORM_FUNCTION,
      "myFunction");

    expectedMlcpParams.put(ConfigConstants.TRANSFORM_NAMESPACE,
      "http://marklogic.com/example/namespace");

    expectedMlcpParams.put(ConfigConstants.TRANSFORM_PARAM,
      "{\"myParam\":\"test\"}");

    Map<String,String> generatedMlcpParams = MlcpUtil
      .optionsForTransforms(transform);
    assertEquals(expectedMlcpParams, generatedMlcpParams);
  }
}
