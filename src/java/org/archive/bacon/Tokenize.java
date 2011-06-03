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
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.FuncSpec;

/**
 * Version of the Pig built-in TOKENIZE() function which operates like
 * STRSPLIT() but returns the tokens in a Bag rather than a Tuple.
 *
 * Source code derived from the Pig TOKENIZE() and STRSPLIT()
 * built-ins.
 */
public class Tokenize extends EvalFunc<DataBag> 
{
  TupleFactory mTupleFactory = TupleFactory.getInstance();
  BagFactory   mBagFactory = BagFactory.getInstance();
  
  @Override
  public DataBag exec(Tuple input) throws IOException
  {
    try
      {
        if ( input == null || input.size() < 1 ) return null;
        
        String source = (String) input.get(0);
        String delim  = (input.size() > 1 ) ? (String)  input.get(1) : "\\s";
        int    length = (input.size() > 2 ) ? (Integer) input.get(2) : 0;
        
        if ( source == null || delim == null ) return null;
        
        DataBag output = mBagFactory.newDefaultBag();
        
        for ( String token : source.split(delim, length) )
          {
            output.add( mTupleFactory.newTuple( token ) );
          }
        
        return output;
      }
    catch (ExecException ee) 
      {
        throw ee;
      }
  }
  
  @SuppressWarnings("deprecation")
  @Override
  public Schema outputSchema(Schema input)
  {
    try
      {
        Schema.FieldSchema tokenFs = new Schema.FieldSchema("token", DataType.CHARARRAY); 
        Schema tupleSchema = new Schema(tokenFs);
        
        Schema.FieldSchema tupleFs;
        tupleFs = new Schema.FieldSchema("tuple_of_tokens", tupleSchema, DataType.TUPLE);
        
        Schema bagSchema = new Schema(tupleFs);
        bagSchema.setTwoLevelAccessRequired(true);
        Schema.FieldSchema bagFs = new Schema.FieldSchema("bag_of_tokenTuples",bagSchema, DataType.BAG);
        
        return new Schema(bagFs); 
      }
    catch (FrontendException e)
      {
        // throwing RTE because above schema creation is not expected
        // to throw an exception and also because superclass does not
        // throw exception
        throw new RuntimeException("Unable to compute Tokenize schema.");
      }   
  }
  
  /* Omit this from our Tokenizer so that the multi-input calls can be mapped to it.
  public List<FuncSpec> getArgToFuncMapping() throws FrontendException 
  {
    List<FuncSpec> funcList = new ArrayList<FuncSpec>();
    Schema s = new Schema();
    s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
    funcList.add(new FuncSpec(this.getClass().getName(), s));
    return funcList;
  }
  */
}

