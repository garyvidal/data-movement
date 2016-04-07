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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.contentpump.ConfigConstants;
import com.marklogic.datamovement.mlcp.DataMovementTransform;
import com.marklogic.datamovement.mlcp.ModuleTransform;

public class MlcpUtil {

  /** Prepares mlcp command-line args -host, -port, -username, -password, and -database.
   * @param client the source for the values of the command line args
   * @return the command-line args and values, ready to pass to mlcp
   */
  public static List<String> argsForClient(DatabaseClient client) {
    ArrayList<String> mlcpArgs = new ArrayList<String>();
    if ( client != null ) {
      mlcpArgs.add("-" + ConfigConstants.HOST);        mlcpArgs.add(client.getHost());
      mlcpArgs.add("-" + ConfigConstants.PORT);        mlcpArgs.add(String.valueOf(client.getPort()));
      if ( client.getUser() != null ) {
        mlcpArgs.add("-" + ConfigConstants.USERNAME);  mlcpArgs.add(client.getUser());
      }
      if ( client.getPassword() != null ) {
        mlcpArgs.add("-" + ConfigConstants.PASSWORD);  mlcpArgs.add(client.getPassword());
      }
      if ( client.getDatabase() != null ) {
        mlcpArgs.add("-" + ConfigConstants.DATABASE);  mlcpArgs.add(client.getDatabase());
      }
    }
    return mlcpArgs;
  }

  /** For each getter, converts a List of pattern,replacement pairs into mlcp command-line syntax
   * "pattern,'string',pattern,'string'".
   * @param pairs a List of strings with alternating regex pattern then replacement
   * @return the pairs combined into a string value suitable to pass to mlcp
   */
  public static String combineRegexPairs(ArrayList<String> pairs) {
    StringBuffer value = new StringBuffer();
    if ( pairs == null ) throw new IllegalArgumentException("pairs must not be null");
    if ( pairs.size() % 2 == 1 ) {
      throw new IllegalArgumentException("You must provide an even number of arguments--they are pairs " +
        "of pattern and replacement");
    }
    for ( int i=0; i < pairs.size(); i += 2 ) {
      String pattern = pairs.get(i);
      String replacement = pairs.get(i+1);
      if ( value.length() > 0 ) value.append(",");
      value.append(pattern + ",'" + replacement + "'");
    }
    return value.toString();
  }

  /** Clears out options related to transforms.
   * @param options the map to clear
   */
  public static void clearOptionsForTransforms(Map<String,String> options) {
    options.remove(ConfigConstants.TRANSFORM_MODULE);
    options.remove(ConfigConstants.TRANSFORM_FUNCTION);
    options.remove(ConfigConstants.TRANSFORM_NAMESPACE);
    options.remove(ConfigConstants.TRANSFORM_PARAM);
  }

  /** Converts a ModuleTransform into the equivalent mlcp command-line options
   * @param transform the configured trasnform from which to get values
   * @return a Map with mlcp command-line options keys and values
   */
  public static Map<String,String> optionsForTransforms(DataMovementTransform<?> transform) {
    LinkedHashMap<String,String> options = new LinkedHashMap<>();
    if ( transform instanceof ModuleTransform ) {
      ModuleTransform moduleTransform = (ModuleTransform) transform;
      options.put(ConfigConstants.TRANSFORM_MODULE,    moduleTransform.getModulePath() );
      options.put(ConfigConstants.TRANSFORM_FUNCTION,  moduleTransform.getFunctionName() );
      if ( moduleTransform.getFunctionNamespace() != null ) {
        options.put(ConfigConstants.TRANSFORM_NAMESPACE, moduleTransform.getFunctionNamespace() );
      }
    } else {
      throw new IllegalStateException("Not yet implemented in mlcp layer");
    }
    options.putAll( optionsForTransformParams(transform) );
    return options;
  }

  private static Map<String,String> optionsForTransformParams(DataMovementTransform<?> transform) {
    LinkedHashMap<String,String> options = new LinkedHashMap<>();
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
            jsonObject.set(key, jsonArray);
          }
        }
      }
      // serialize the JSON object since mlcp only allows us to pass one string
      options.put(ConfigConstants.TRANSFORM_PARAM, jsonObject.toString() );
    }
    return options;
  }
}
