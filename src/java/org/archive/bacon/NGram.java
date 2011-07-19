/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding copyright
 * ownership.  The ASF licenses this file to you under the Apache
 * License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain a copy of
 * the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.archive.bacon;

import java.io.*;
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
import org.apache.pig.FuncSpec;

/**
 * Takes a bag of values and emits a new bag containing
 * n-grams.  The input parameters are:
 *   DataBag      bag of tuples of any type
 *   gram-length  length of gram
 *   offset       offset between gram start positions
 *
 * For example, consider the bag
 *
 *   stuff = { 'foo', 'bar', 'baz', 'frotz' }
 *
 * We can generate 2-grams with:
 *
 *   grams = NGRam( stuff, 2 );
 *
 * yielding:
 *
 *   grams = { ('foo','bar'), ('bar','baz'), ('baz','frotz') }
 *
 * Source code derived from the Pig TOKENIZE() and STRSPLIT()
 * built-ins.
 */
public class NGram extends EvalFunc<DataBag> 
{
  TupleFactory mTupleFactory = TupleFactory.getInstance();
  BagFactory   mBagFactory = BagFactory.getInstance();
  
  @Override
  public DataBag exec( Tuple input ) throws IOException
  {
    try
      {
        if ( input == null || input.size() < 1 ) return null;
        
        DataBag         tokens = (DataBag) input.get(0);
        Iterator<Tuple> titer  = tokens.iterator();

        int n      = (input.size() > 1 ) ? (Integer) input.get(1) : 1;
        int offset = (input.size() > 2 ) ? (Integer) input.get(2) : 1;

        if ( n < 1 || offset < 1 ) return null;
        
        LinkedList<Tuple> window = new LinkedList<Tuple>();
        for ( int i = 0 ; i < offset ; i++ ) window.add( null );  // Fill with sentinels

        DataBag output = mBagFactory.newDefaultBag();

        // In the following loop, we have to explicitly check for the
        // end of the input tokens.  The iterator returned by
        // DataBag.iterator() will happily return 'null' rather than
        // throw a NoSuchElementException when it hits the end of the
        // list.  In, violation of the Iterator.next() spec AFAICT.
      loop: 
        while ( true )
          {
            // Gobble up the front of the window, up to the 'offset'.
            int wsize = Math.min( window.size(), offset );
            for ( int j = wsize ; j > 0 ; j-- )
              {
                window.poll();
              }

            // If the offset is bigger than the ngram size, keep
            // eating tokens.
            for ( int j = 0 ; j < offset - wsize ; j++ )
              {
                if ( ! titer.hasNext( ) ) break loop;
                titer.next();
              }

            // Fill any remaining slots in the window to get N tokens
            while ( window.size() < n )
              {
                if ( ! titer.hasNext() ) break loop;
                
                window.add( titer.next() );
              }
            
            // Copy the tokens from the window to a new NGram, then
            // add that NGram to the output.
            Tuple ngram = mTupleFactory.newTuple( n );
            for ( int j = 0; j < n ; j++ )
              {
                ngram.set( j, window.get(j) );
              }
            
            output.add( ngram );
          }
        
        return output;
      }
    catch (ExecException ee) 
      {
        throw ee;
      }
  }
  
  /*
   * The schema isn't known until the UDF is called.  At point, all we
   * know is that we have a bag of tuples, but we don't know the #
   * elements in the tuples.
   *
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
        throw new RuntimeException("Unable to compute NGram schema.");
      }   
  }
  */
  
  /* Omit this from our NGramr so that the multi-input calls can be mapped to it.
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

