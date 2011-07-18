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
 * Calculate the length of all strings in the given input.
 *
 * If the input is a string, its length is returned.  If the input is
 * null or a non-string type, 0 is returned.
 *
 * If the input is a tuple or bag, it is traversed recursively and the
 * total length of all strings found within is returned.  Non-string
 * types in the tuple/bag structure are ignored.
 * 
 */ 
public class StringLength extends EvalFunc<Long>
{

  public StringLength( )
    throws IOException
  {
    
  }

  public Long exec( Tuple input )
    throws IOException
  {
    Long length = length( input );
    
    return length;
  }

  public long length( Object input )
    throws IOException
  {
    if ( input == null ) return 0L;

    long length = 0L;

    if ( input instanceof String )
      {
        String s = (String) input;

        return s.length( );
      }
    else if ( input instanceof Tuple )
      {
        Tuple tuple = (Tuple) input;

        for ( Object o : tuple.getAll( ) )
          {
            length += length( o );
          }
      }
    else if ( input instanceof DataBag )
      {
        DataBag bag = (DataBag) input;

        for ( Tuple t : bag )
          {
            length += length( t );
          }
      }

    return length;
  }

  @SuppressWarnings("deprecation")
  @Override
  public Schema outputSchema(Schema input)
  {
    return new Schema( new Schema.FieldSchema( null, DataType.LONG ) ); 
  }
}
