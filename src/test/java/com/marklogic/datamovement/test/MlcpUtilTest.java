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

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import com.marklogic.datamovement.DataMovementManager;
import com.marklogic.datamovement.ImportDefinition;
import com.marklogic.datamovement.ModuleTransform;
import com.marklogic.datamovement.impl.MlcpUtil;

import com.marklogic.contentpump.ConfigConstants;

import java.util.ArrayList;
import java.util.List;

public class MlcpUtilTest {
  private DataMovementManager moveMgr = DataMovementManager.newInstance();

  @Test
  public void testArgsFromGetters() throws Exception {
    ImportDefinition def = moveMgr.newImportDefinition();
    ArrayList<String> expectedMlcpParams = new ArrayList<String>();

    def.maxSplitSize(2);
    expectedMlcpParams.add("-" + ConfigConstants.MAX_SPLIT_SIZE);  expectedMlcpParams.add("2");

    def.minSplitSize(1);
    expectedMlcpParams.add("-" + ConfigConstants.MIN_SPLIT_SIZE);  expectedMlcpParams.add("1");

    def.inputFilePath("/a/b/c");
    expectedMlcpParams.add("-" + ConfigConstants.INPUT_FILE_PATH); expectedMlcpParams.add("/a/b/c");

    def.xmlRepairLevel(ImportDefinition.XmlRepairLevel.DEFAULT);
    expectedMlcpParams.add("-" + ConfigConstants.XML_REPAIR_LEVEL); expectedMlcpParams.add("default");

    List<String> generatedMlcpParams = MlcpUtil
      .argsFromGetters(def, "getMaxSplitSize", "getMinSplitSize", "getInputFilePath", "getXmlRepairLevel");
    assertEquals(expectedMlcpParams, generatedMlcpParams);
  }

  @Test
  public void testArgsForTransforms() throws Exception {
    ArrayList<String> expectedMlcpParams = new ArrayList<String>();
    ModuleTransform transform = moveMgr.newModuleTransform(
      "/path/to/myModule.sjs", "myFunction", "http://marklogic.com/example/namespace");
    transform.addParameter("myParam", "test");

    expectedMlcpParams.add("-" + ConfigConstants.TRANSFORM_MODULE);
    expectedMlcpParams.add("/path/to/myModule.sjs");

    expectedMlcpParams.add("-" + ConfigConstants.TRANSFORM_FUNCTION);
    expectedMlcpParams.add("myFunction");

    expectedMlcpParams.add("-" + ConfigConstants.TRANSFORM_NAMESPACE);
    expectedMlcpParams.add("http://marklogic.com/example/namespace");

    expectedMlcpParams.add("-" + ConfigConstants.TRANSFORM_PARAM);
    expectedMlcpParams.add("{\"myParam\":\"test\"}");

    List<String> generatedMlcpParams = MlcpUtil
      .argsForTransforms(transform);
    assertEquals(expectedMlcpParams, generatedMlcpParams);
  }
}
