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
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Job;

import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.LoadFunc;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

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
  PigStorage ps = new PigStorage("\0");

  boolean ignoreNulls = true;

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

    if ( size != 1 ) throw new IOException( "Tuple must have a single value, which is a map." );
    
    try
      {
        JSONObject json = (JSONObject) getJSON( tuple.get(0) );

        String jstring = json.toString();

        // FIXME: Is this kosher to over-write the tuple?
        tuple.set( 0, jstring );
        
        this.ps.putNext( tuple );
      }
    catch ( JSONException jsone )
      {
        throw new IOException( "JSON had a boo-boo", jsone );
      }
  }

  public Object getJSON( Object o )
    throws JSONException, IOException
  {
    switch ( DataType.findType( o ) )
      {
      case DataType.NULL:
        if ( this.ignoreNulls ) return null;
        return JSONObject.NULL;

      case DataType.BOOLEAN:
      case DataType.INTEGER:
      case DataType.LONG:
      case DataType.DOUBLE:
        return o;
        
      case DataType.FLOAT:
        return Double.valueOf( ((Float)o).floatValue() );
        
      case DataType.CHARARRAY:
        return o.toString( );
        
      case DataType.MAP:
        Map<String,Object> m = (Map<String,Object>) o;
        JSONObject json = new JSONObject();
        for( Map.Entry<String, Object> e: m.entrySet( ) )
          {
            String key   = e.getKey();
            Object value = getJSON( e.getValue() );
            
            // If the value is null, skip it.
            if ( null == value ) continue ;

            json.put( key, value );
          }
        return json;
        
      case DataType.TUPLE:
        {
          JSONArray values = new JSONArray();
          Tuple t = (Tuple) o;
          for ( int i = 0; i < t.size(); ++i ) 
            {
              Object value = getJSON( t.get(i) );
              
              if ( null == value ) continue ;
              
              values.put( value );
            }
          return values;
        }

      case DataType.BAG:
        {
          JSONArray values = new JSONArray();
          for ( Tuple t : ((DataBag) o) )
            {
              switch ( t.size() )
                {
                case 0:
                  continue ;

                case 1:
                  {
                    Object innerObject = getJSON( t.get(0) );

                    if ( null == innerObject ) continue ;

                    values.put( innerObject );
                  }
                  break;
                  
                default:
                  JSONArray innerList = new JSONArray();
                  for ( int i = 0; i < t.size(); ++i ) 
                    {
                      Object innerObject = getJSON( t.get(i) );

                      if ( null == innerObject ) continue ;

                      innerList.put( innerObject );
                    }
                  
                  values.put( innerList );
                  break;
                }
              
            }
          return values;
        }

      case DataType.BYTEARRAY:
        // FIXME?  What else can we do?  base-64 encoded string?
        System.err.println( "Pig BYTEARRAY not supported for JSONStorage" );
        return null;
        
      default:
        System.out.println( "unknown type: " + DataType.findType( o ) + " value: " + o.toString( ) );
        return null;
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
    this.ps.prepareToRead( reader, split );
  }

  @Override
  public Tuple getNext() throws IOException
  {
    // FIXME: Parse JSON string returned from PigStorage, then convert
    // that into Pig objects.
    return this.ps.getNext();
  }

}
