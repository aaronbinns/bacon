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
import java.util.List;
import java.util.ArrayList;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.FrontendException;

/**
 * Variation on SUBSTRING() which just takes the first N characters of
 * a string, effectively truncating it.  If the string is shorter than
 * N, or of equal length, then it is not modified, just returned
 * as-is.
 */
public class Truncate extends EvalFunc<String> {

    /**
     * Truncate the given string to the given length.  If the string
     * is already that length or shorter, return it unchanged.
     */
    @Override
    public String exec( Tuple input ) throws IOException 
    {
      String  source = (String) input.get(0);
      Integer length = (Integer)input.get(1);
      
      if ( source == null ) return source;
      if ( length == null ) return source;
      if ( length < 0     ) return source;
      
      if ( source.length() <= length ) return source;
      
      String truncated = source.substring(0,length);
      
      return truncated;
    }

    /**
     * Return a Schema for this function's return type: chararray (String)
     */
    @Override
    public Schema outputSchema(Schema input)
    {
      return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
    }
    
    /**
     * Because we create this function signature, Pig will ensure that
     * calls to this function will have the proper number and type of
     * arguments.  Therefore, we don't have to check them in the
     * exec() method.
     */
    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException
    {
      List<FuncSpec> funcList = new ArrayList<FuncSpec>();
      Schema s = new Schema();
      s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
      s.add(new Schema.FieldSchema(null, DataType.INTEGER));
      funcList.add(new FuncSpec(this.getClass().getName(), s));
      return funcList;
    }
}
