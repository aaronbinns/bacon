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
import java.net.*;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.WrappedIOException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * ReBag: take the elements from a tuple and put them into a bag.
 *
 * This was motivated by the fact that the built-in functions STRSPLIT
 * and TOKENIZE don't work the way I want them to.  STRSPLIT returns a
 * tuple, and TOKENIZE does not allow for custom delimiters.  Sheesh.
 *
 * So, in addition to writing my own tokenizer, which acts like
 * STRSPLIT but returns a bag rather thana tuple; I have also written
 * this function to take the elements of a tuple and put them into a
 * bag.  I strongly suspect that this doesn't handle all the corner
 * cases nor follow all the Pig "good housekeeping" rules.
 *
 * I'm leaving this here as an experiment.
 */ 
public class ReBag extends EvalFunc<DataBag>
{
  TupleFactory tupleFactory = TupleFactory.getInstance();
  BagFactory   bagFactory   = BagFactory  .getInstance();

  /**
   * Re-bag the tuple elements.  Somewhat strangely, the incoming
   * tuple is wrapped inside another tuple, so we have to look inside
   * te nested tuple for the actual elements.  That is, the incoming
   * tuple looks like:
   *  ((foo,bar,baz))
   */
  public DataBag exec( Tuple input )
    throws IOException 
  {
    try 
      {
        if ( input == null ) return null;

        DataBag output = bagFactory.newDefaultBag();

        for ( Object o : input.getAll() )
          {
            if ( o instanceof Tuple )
              {
                Tuple inner = (Tuple) o;

                for ( Object p : inner.getAll() )
                  {
                    output.add( tupleFactory.newTuple( p ) );
                  }
              }
          }

        return output;
      }
    catch ( Exception e )
      {
        throw WrappedIOException.wrap("Caught exception processing input row ", e);
      }
  }

  /**
   * Generate an output schema based on the type of the first element
   * in the tuple being re-bagged.  We assume that the type of all the
   * elements are the same and can thus just look at the first one.
   */
  public Schema outputSchema( Schema input ) 
  {
    try
      {
        if ( input == null )
          {
            return Schema.generateNestedSchema( DataType.BAG, DataType.NULL );
          }
        
        Schema elementSchema = new Schema();

        for ( Schema.FieldSchema fs : input.getFields() )
          {
            if ( fs.type == DataType.TUPLE )
              {
                if ( fs.schema == null )
                  {
                    break ;
                  }

                for ( Schema.FieldSchema ifs : fs.schema.getFields() )
                  {
                    // The type of all the elements in the output bag
                    // are assumed to be the same as the type of the
                    // first element of the tuple being re-bagged.
                    elementSchema.add( ifs );
                    
                    break ;
                  }
              }
          }
        
        Schema bagSchema = new Schema( new Schema.FieldSchema( getSchemaName( this.getClass().getName().toLowerCase(), input ),
                                                               elementSchema, 
                                                               DataType.BAG ) );
        
        return bagSchema;                                       
      }
    catch (Exception e)
      {
        e.printStackTrace( System.err );
        return null;
      }
  }
}
