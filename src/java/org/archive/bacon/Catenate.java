/*
 * Copyright 2011 Internet Archive
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.archive.bacon;

import java.io.*;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;


/**
 * Catenate all strings in the given input, with a delimiter string.
 * Kinda/sorta approximates Python str.join().
 *
 * If any input object is a tuple or bag, it is traversed recursively
 * and the all strings found within are catenated, separated by the
 * given delimiter.
 */ 
public class Catenate extends EvalFunc<String>
{

  public Catenate( )
    throws IOException
  {
    
  }

  public String exec( Tuple input )
    throws IOException
  {
    if ( input == null || input.size() < 2 ) return null;

    String delim = input.get(0).toString();

    StringBuilder sb = new StringBuilder();

    for ( int i = 1 ; i < input.size() ; i++ )
      {
        cat( sb, input.get(i), delim );                   
      }

    return sb.toString();
  }

  public void cat( StringBuilder sb, Object input, String delim )
    throws IOException
  {
    if ( input == null ) return;

    if ( input instanceof Tuple )
      {
        Tuple tuple = (Tuple) input;

        for ( Object o : tuple.getAll( ) )
          {
            cat( sb, o, delim );
          }
      }
    else if ( input instanceof DataBag )
      {
        DataBag bag = (DataBag) input;

        for ( Tuple t : bag )
          {
            for ( Object o : t.getAll( ) )
              {
                cat( sb, o, delim );
              }
          }
      }
    else
      {
        String s = input.toString();
        
        s = s.trim();
        
        if ( s.length() > 0 ) 
          {
            sb.append( s ).append( delim );
          }
      }

  }


  @SuppressWarnings("deprecation")
  @Override
  public Schema outputSchema(Schema input)
  {
    return new Schema( new Schema.FieldSchema( null, DataType.CHARARRAY ) ); 
  }
}
