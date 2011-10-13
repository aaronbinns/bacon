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
package org.archive.bacon.url;

import java.io.*;
import java.net.*;
import java.util.*;

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
import org.apache.pig.impl.util.WrappedIOException;
import org.apache.pig.FuncSpec;

/**
 * 
 */
public class PathExpand extends EvalFunc<DataBag> 
{
  TupleFactory mTupleFactory = TupleFactory.getInstance();
  BagFactory   mBagFactory = BagFactory.getInstance();
  
  @Override
  public DataBag exec(Tuple input) throws IOException
  {
    if ( input == null || input.size() < 1 ) return null;

    DataBag output = mBagFactory.newDefaultBag();
        
    try
      {
        URL u = new URL( (String) input.get(0) );
        
        String path = u.getPath().trim();

        if ( path.length() == 0 ) path = "/";

        String[] parts = path.split("[/]",-1);

        StringBuilder sb = new StringBuilder( 64 );
        for ( int i = 0; i < parts.length ; i++ )
          {
            if ( i == (parts.length-1) && parts[i].length() == 0 ) break;

            sb.append( parts[i] );
            if ( i < (parts.length-1) ) sb.append( '/' );
            URL up = new URL( u, sb.toString() );

            output.add( mTupleFactory.newTuple( up.toString() ) );
          }

        return output;
      }
    catch( MalformedURLException mue )
      {
        // If not a valid URL, return null.
        return null;
      }
    catch ( Exception e )
      {
        throw WrappedIOException.wrap("Caught exception processing input row ", e);
      }
  }
  
  @SuppressWarnings("deprecation")
  @Override
  public Schema outputSchema(Schema input)
  {
    try
      {
        Schema.FieldSchema pathFs = new Schema.FieldSchema("path", DataType.CHARARRAY); 
        Schema tupleSchema = new Schema(pathFs);
        
        Schema.FieldSchema tupleFs;
        tupleFs = new Schema.FieldSchema("path", tupleSchema, DataType.TUPLE);
        
        Schema bagSchema = new Schema(tupleFs);
        bagSchema.setTwoLevelAccessRequired(true);
        Schema.FieldSchema bagFs = new Schema.FieldSchema("paths",bagSchema, DataType.BAG);
        
        return new Schema(bagFs); 
      }
    catch (FrontendException e)
      {
        // throwing RTE because above schema creation is not expected
        // to throw an exception and also because superclass does not
        // throw exception
        throw new RuntimeException("Unable to compute PathExpand schema.");
      }   
  }
  
  /* Omit this from our PathExpandr so that the multi-input calls can be mapped to it.
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

