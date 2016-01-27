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

import com.marklogic.datamovement.AdhocTransform;
import com.marklogic.datamovement.DataMovementTransform;
import com.marklogic.datamovement.JobDefinition;
import com.marklogic.datamovement.ModuleTransform;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MlcpUtil {
  // look in the method name for any cap char followed by lower-case chars
  private static Pattern pattern = Pattern.compile("[A-Z][a-z]+");

  /**
   * Helps prepare mlcp command line args by calling the getter for each
   * property, generating two command-line mlcp args per property:
   *   1) the property name preceded by a hyphen and with init cap words
   *      converted to lower case and separated by underscores
   *   2) the property value from calling the getter method for that property
   * For example, if def has a property "optionsFile" I can call
   * MlcpUtil.argsFromGetters(def, "getOptionsFile") and I will get back an
   * array of length 2 with the first element "-options_file" and the second
   * element the result of calling def.getOptionsFile().
   */
  public static List<String> argsFromGetters(JobDefinition def, String... getterMethodNames)
    throws NoSuchMethodException, IllegalAccessException, InvocationTargetException
  {
    ArrayList<String> mlcpArgs = new ArrayList<String>();
    for ( String methodName : getterMethodNames ) {
      String argName = convertMethodNameToArgName(methodName);
      mlcpArgs.add(argName);

      // get getter method
      Method getter = def.getClass().getDeclaredMethod(methodName, null);
      // invoke getter method on our def instance
      Object argValue = getter.invoke(def, null);

      mlcpArgs.add(argValue.toString());
    }
    return mlcpArgs;
  }

  /** Converts methodNames like "getOptionsFile" to command line argument names
   * like "-options_file"
   */
  private static String convertMethodNameToArgName(String methodName) {
    if ( methodName == null || ! methodName.startsWith("get") ) {
          throw new IllegalStateException("method name '" + methodName + "'" +
            " doesn't match expected pattern. It should begin with 'get'.");
    }
    ArrayList<String> words = new ArrayList<String>();
    Matcher matcher = pattern.matcher(methodName);
    // find each word (cap char followed by lower-case chars)
    while ( matcher.find() ) {
        // get the whole match (one word) and make it lower-case
        String word = matcher.group(0).toLowerCase();
        words.add(word);
    }
    if ( words.size() == 0 ) {
          throw new IllegalStateException("method name '" + methodName + "'" +
            " doesn't match expected pattern. It should have init-cap words in it.");
    }
    // begin with hyphen, then add each word connected with underscores
    return "-" + String.join("_", words);
  }

  public static List<String> argsForTransforms(DataMovementTransform transform) {
    ArrayList<String> args = new ArrayList<String>();
    if ( transform instanceof ModuleTransform ) {
      ModuleTransform moduleTransform = (ModuleTransform) transform;
      args.add("-transform_module");    args.add( moduleTransform.getModulePath() );
      args.add("-transform_function");  args.add( moduleTransform.getFunctionName() );
      args.add("-transform_namespace"); args.add( moduleTransform.getFunctionNamespace() );
    } else if ( transform instanceof AdhocTransform ) {
      throw new IllegalStateException("Not yet implemented in mlcp layer");
    }
    args.addAll( argsForTransformParams(transform) );
    return args;
  }

  private static List<String> argsForTransformParams(DataMovementTransform transform) {
    ArrayList<String> args = new ArrayList<String>();
    // if there are custom transform parameters to send
    if ( transform != null && transform.size() > 0 ) {
      ObjectMapper mapper = new ObjectMapper();
      // create a JSON object to package all parameters
      ObjectNode jsonObject = mapper.createObjectNode();
      Map<String, List<String>> params = transform;
      for ( String key : params.keySet() ) {
        List<String> values = params.get(key);
        if ( values != null && values.size() > 0 ) {
          if ( values.size() == 1 ) {
            jsonObject.put(key, values.get(0));
          } else {
            ArrayNode jsonArray = mapper.createArrayNode();
            for ( String value : values ) {
              jsonArray.add(value);
            }
            jsonObject.put(key, jsonArray);
          }
        }
      }
      // serialize the JSON object since mlcp only allows us to pass one string
      args.add("-transform_param"); args.add( jsonObject.toString() );
    }
    return args;
  }
}

