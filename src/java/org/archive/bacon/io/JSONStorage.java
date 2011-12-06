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

import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Job;

import org.apache.pig.StoreFunc;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

/**
 *
 *
 */
public class JSONStorage extends StoreFunc
{
  // Use a '\0' as the delimiter.  It should never appear, so if it
  // does show up, we should notice it!
  PigStorage ps = new PigStorage("\0");

  public OutputFormat getOutputFormat( )
  {
    return this.ps.getOutputFormat( );
  }

  public void prepareToWrite(RecordWriter writer) 
  {
    this.ps.prepareToWrite( writer );
  }

  public void putNext( Tuple tuple ) throws IOException 
  {
    int size = tuple.size();

    if ( size != 1 ) throw new IOException( "Tuple must have a single value, which is a map." );
    
    Map<String, Object> m = (Map<String, Object>) tuple.get(0);

    try
      {
        JSONObject json = getJSON( m );

        String jstring = json.toString();

        // FIXME: Is this kosher to over-write the tuple?
        tuple.set( 0, jstring );
        
        this.ps.putNext( tuple );
      }
/*
    try
      {
        JSONObject json = new JSONObject();
        for( Map.Entry<String, Object> e: m.entrySet( ) )
          {
            String key   = e.getKey();
            Object value = e.getValue();
            
            String svalue = null;
            switch ( DataType.findType( value ) )
              {
              case DataType.BOOLEAN:
                json.put( key, ((Boolean) value) );
                break;
                
              case DataType.INTEGER:
                json.put( key, ((Integer) value) );
                break;
                
              case DataType.LONG:
                json.put( key, ((Long) value) );
                break;
                
              case DataType.FLOAT:
                json.put( key, Double.valueOf( ((Float)value).floatValue() ) );
                break;
                
              case DataType.DOUBLE:
                json.put( key, ((Double) value) );
                break;
                
              case DataType.CHARARRAY:
                json.put( key, value.toString() );
                break;
                
                // TODO: Complex types!
              case DataType.MAP:
                json.put( key, getJSON( (Map<String,Object>) value ) );
                
              case DataType.NULL:
              default:
                System.out.println( "unknown type: " + DataType.findType( value ) + " value: " + value.toString( ) );
                continue ;
              }
          }
        
        String jstring = json.toString();

        System.out.println( "jstring: " + jstring );
        
        // Is this kosher to over-write the tuple?
        tuple.set( 0, jstring );
        
        this.ps.putNext( tuple );
      }
*/
    catch ( JSONException jsone )
      {
        throw new IOException( "JSON had a boo-boo", jsone );
      }
  }
  
  public JSONObject getJSON( Map<String,Object> m )
    throws JSONException
  {
    JSONObject json = new JSONObject();

    for( Map.Entry<String, Object> e: m.entrySet( ) )
      {
        String key   = e.getKey();
        Object value = e.getValue();
        
        String svalue = null;
        switch ( DataType.findType( value ) )
          {
          case DataType.BOOLEAN:
            json.put( key, ((Boolean) value) );
            break;
            
          case DataType.INTEGER:
            json.put( key, ((Integer) value) );
            break;
            
          case DataType.LONG:
            json.put( key, ((Long) value) );
            break;
            
          case DataType.FLOAT:
            json.put( key, Double.valueOf( ((Float)value).floatValue() ) );
            break;
            
          case DataType.DOUBLE:
            json.put( key, ((Double) value) );
            break;
            
          case DataType.CHARARRAY:
            json.put( key, value.toString() );
            break;
            
            // TODO: Complex types!
          case DataType.MAP:
            json.put( key, getJSON( (Map<String,Object>) value ) );
            
          case DataType.NULL:
          default:
            System.out.println( "unknown type: " + DataType.findType( value ) + " value: " + value.toString( ) );
            continue ;
          }
       
      } 
  
    return json;
  }


  public void setStoreLocation( String location, Job job ) throws IOException
  {
    this.ps.setStoreLocation( location, job );
  }

}
