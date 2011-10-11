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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * 
 */

public class RegexExtractAll extends EvalFunc<DataBag>
{
  TupleFactory mTupleFactory = TupleFactory.getInstance();
  BagFactory   mBagFactory = BagFactory.getInstance();
  
  @Override
  public DataBag exec(Tuple input) throws IOException
  {
    try
      {
        if ( input == null || input.size() < 1 ) return null;

        String string = (String) input.get(0);
        String regex  = (String) input.get(1);

        if ( string == null || regex == null ) return null;

        Pattern p;
        try
          {
            p = Pattern.compile( regex );
          }
        catch ( Exception e )
          {
            throw new IOException( "RegexExtractAll : Mal-Formed Regular expression : " + regex );
          }
        
        Matcher m = p.matcher( string );

        DataBag output = mBagFactory.newDefaultBag();
        while ( m.find() )
          {
            Tuple t = mTupleFactory.newTuple( m.groupCount() );

            for ( int i = 0; i < m.groupCount(); i++ )
              {
                t.set( i, m.group(i+1) );
              }

            output.add( t );
          }
        
        return output;
      }
    catch ( ExecException ee )
      {
        throw ee;
      }
  }
  
  /*
  @SuppressWarnings("deprecation")
  @Override
  public Schema outputSchema( Schema input )
  {
    try
      {
        Schema.FieldSchema elementFs = new Schema.FieldSchema( "element", DataType.CHARARRAY );
        Schema tupleSchema = new Schema( elementFs );

        Schema.FieldSchema tupleFs;
        tupleFs = new Schema.FieldSchema( "tuple_of_elements", tupleSchema, DataType.TUPLE );

        Schema bagSchema = new Schema(tupleFs);
        bagSchema.setTwoLevelAccessRequired(true);
        Schema.FieldSchema bagFs = new Schema.FieldSchema("bag_of_elementTuples",bagSchema, DataType.BAG);

        return new Schema(bagFs);
      }
    catch ( FrontendException fe ) 
      {
        throw new RuntimeException("Unable to compute RegexExtractAll schema." );
      }
  }
  */
  
}

