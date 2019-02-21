/*
 *
 */
package org.archive.bacon;

import java.io.*;
import java.util.*;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;


/**
 *
 */
public class Median extends EvalFunc<Tuple>
{

  public Median( )
    throws IOException
  {

  }

  public Tuple exec( Tuple input )
    throws IOException
  {
    if ( input == null || input.size() < 1 ) return null;

    DataBag         values = (DataBag) input.get(0);
    long            numValues = (values.size() / 2) + (values.size() % 2);
    Iterator<Tuple> titer  = values.iterator();

    while ( numValues > 1 )
      {
        titer.next();

        numValues--;
      }

    return titer.next();
  }

  @SuppressWarnings("deprecation")
  @Override
  public Schema outputSchema(Schema input)
  {
    return new Schema( new Schema.FieldSchema( null, DataType.TUPLE ) );
  }
}
