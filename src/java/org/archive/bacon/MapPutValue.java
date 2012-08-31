/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.archive.bacon;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Simple EvalFunc to put a key/value in an existing Map.
 */
public class MapPutValue extends EvalFunc<Map> {

    @Override
    public Map exec(Tuple input) throws IOException 
    {
      if (input == null || input.size() != 3) return null;
      
      try 
        {
          Map<String,Object> map = (Map<String,Object>) input.get(0);
          
          String key   = (String) input.get(1);
          Object value = input.get(2);

          map.put(key,value);

          return map;
	} catch (ClassCastException e){
		throw new RuntimeException("Map key must be a String");
	} catch (ArrayIndexOutOfBoundsException e){
		throw new RuntimeException("Function input must have even number of parameters");
        } catch (Exception e) {
            throw new RuntimeException("Error while creating a map", e);
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.MAP));
    }

}
