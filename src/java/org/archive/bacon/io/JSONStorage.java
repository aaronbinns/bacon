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
package org.archive.bacon.io;

import java.io.*;
import java.util.*;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;

import org.apache.pig.LoadFunc;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.BagFactory;

/**
 * Simple store function that writes out Pig 'map' objects as JSON
 * strings.
 *
 * It leverages PigStorage to support writing compressed files with
 * various encodings: gzip, bzip2, etc.
 *
 * In fact, this is really just a hack that expects to receive a Pig
 * 'map' object, converts it to a JSON string, then passes that string
 * on to PigStorage().
 * 
 */
public class JSONStorage extends LoadFunc implements StoreFuncInterface
{
  // Use a '\0' as the delimiter.  It should never appear, so if it
  // does show up, we should notice it!
  PigStorage   ps = new PigStorage("\0");

  RecordReader reader;
  TupleFactory mTupleFactory = TupleFactory.getInstance();
  BagFactory   mBagFactory   = BagFactory.getInstance();

  public JSONStorage( )
  {
  }

  /**
   * StoreFuncInterface
   */
  @Override
  public String relToAbsPathForStoreLocation( String location, Path curDir ) 
    throws IOException
  {
    return this.ps.relToAbsPathForStoreLocation( location, curDir );
  }

  @Override
  public OutputFormat getOutputFormat( )
  {
    return this.ps.getOutputFormat( );
  }

  @Override
  public void setStoreLocation( String location, Job job ) throws IOException
  {
    this.ps.setStoreLocation( location, job );
  }

  @Override
  public void checkSchema(ResourceSchema s) throws IOException
  {
    this.ps.checkSchema( s );
  }

  @Override
  public void prepareToWrite(RecordWriter writer) 
  {
    this.ps.prepareToWrite( writer );
  }

  @Override
  public void putNext( Tuple tuple ) throws IOException 
  {
    int size = tuple.size();

    try
      {
        JSONObject json;
        // If the tuple to serialize as JSON has one field which is a
        // Map, then serialize that Map, not the tuple.  Otherwise,
        // serialize the tuple.
        if ( tuple.size() == 1 && DataType.findType( tuple.get(0) ) == DataType.MAP )
          {
            json = (JSONObject) JSON.toJSON( tuple.get(0) );
          }
        else
          {
            json = (JSONObject) JSON.toJSON( tuple );
          }

        String jstring = json.toString();

        Tuple output = mTupleFactory.newTuple( jstring );
        
        this.ps.putNext( output );
      }
    catch ( JSONException je )
      {
        throw new IOException( je );
      }
  }

  @Override
  public void setStoreFuncUDFContextSignature(String signature)
  {
    this.ps.setStoreFuncUDFContextSignature( signature );
  }

  @Override
  public void cleanupOnFailure(String location, Job job) throws IOException
  {
    this.ps.cleanupOnFailure( location, job );
  }

  /**
   * LoadFunc
   */
  @Override
  public void setLocation(String location, Job job) throws IOException
  {
    this.ps.setLocation( location, job );
  }

  @Override
  public InputFormat getInputFormat() throws IOException
  {
    return this.ps.getInputFormat();
  }
  
  @Override
  public void prepareToRead(RecordReader reader, PigSplit split) throws IOException
  {
    this.reader = reader;
    
    // FIXME: Do we still even need to call PigStorage.prepareToRead()?
    this.ps.prepareToRead( reader, split );
  }

  @Override
  public Tuple getNext() throws IOException
  {
     try
      {
        if ( ! this.reader.nextKeyValue() )
          {
            return null;
          }

        Text text = (Text) this.reader.getCurrentValue( );
        
        if ( text == null ) return null;

        JSONObject json = new JSONObject( text.toString() );

        Object o = JSON.fromJSON( json );

        Tuple tuple;
        if ( o instanceof Map )
          {
            tuple = mTupleFactory.newTuple( o  );
          }
        else
          {
            tuple = (Tuple) o;
          }

        return tuple;
      }
    catch ( JSONException je )
      {
        throw new IOException( je );
      }
    catch ( InterruptedException e  ) 
      {
        // From the Pig example/howto code.
        int errCode = 6018;
        String errMsg = "Error while reading input";
        throw new ExecException(errMsg, errCode,PigException.REMOTE_ENVIRONMENT, e);
      }
  }

}
