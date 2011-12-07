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
        JSONObject json = (JSONObject) toJSON( tuple.get(0) );

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

  public Object toJSON( Object o )
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
            Object value = toJSON( e.getValue() );
            
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
              Object value = toJSON( t.get(i) );
              
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
                    Object innerObject = toJSON( t.get(0) );

                    if ( null == innerObject ) continue ;

                    values.put( innerObject );
                  }
                  break;
                  
                default:
                  JSONArray innerList = new JSONArray();
                  for ( int i = 0; i < t.size(); ++i ) 
                    {
                      Object innerObject = toJSON( t.get(i) );

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
    this.reader = reader;
    
    // FIXME: Do we still even need to call PigStorage.prepareToRead()?
    this.ps.prepareToRead( reader, split );
  }

  @Override
  public Tuple getNext() throws IOException
  {
    // FIXME: Parse JSON string returned from PigStorage, then convert
    // that into Pig objects.
    // return this.ps.getNext();

    try
      {
        if ( ! this.reader.nextKeyValue() )
          {
            return null;
          }

        Text text = (Text) this.reader.getCurrentValue( );
        
        if ( text == null ) return null;

        JSONObject json = new JSONObject( text.toString() );

        // Tuple tuple = (Tuple) fromJSON( json );
        Tuple tuple = mTupleFactory.newTuple( fromJSON( json ) );

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

  public Object fromJSON( Object o ) throws JSONException
  {
    if ( o instanceof String  ||
         o instanceof Long    ||
         o instanceof Double  ||
         o instanceof Integer )
      {
        return o;
      }
    else if ( o instanceof JSONObject )
      {
        JSONObject json = (JSONObject) o;

        Map<String,Object> map = new HashMap<String,Object>( json.length() );

        for ( String key : JSONObject.getNames( json ) )
          {
            Object value = json.get( key );
            
            if ( json.isNull( key ) )
              {
                // TODO!
              }
            else
              {
                // FIXME: recurse the value
                map.put( key, fromJSON( value ) );
              }
          }

        /*
          Tuple tuple = mTupleFactory.newTuple( map );
          return tuple;
        */
        return map;
      }
    else if ( o instanceof JSONArray )
      {
        // FIXME: Add some magic to the key (like a leading @ char) to specify
        //        if we convert from JSONArray to a Tuple or a Bag.
        //        For now, just bag it.

        JSONArray json = (JSONArray) o;
        
        List<Tuple> tuples = new ArrayList<Tuple>( json.length() );

        for ( int i = 0; i < json.length() ; i++ )
          {
            tuples.add( mTupleFactory.newTuple( fromJSON( json.get(i) ) ) );
          }

        DataBag bag = mBagFactory.newDefaultBag( tuples );

        return bag;
      }
    else if ( o instanceof Boolean )
      {
        // FIXME: Since Pig doesn't have a true boolean data type, is
        // this even allowed?  Should we map it to 0/1?
      }
    else
      {
        // FIXME: What to do here?
      }

    return null;
  }

}
