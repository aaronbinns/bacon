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
package org.archive.bacon.nutchwax;

import java.io.*;
import java.util.*;

import org.apache.hadoop.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;

import org.apache.pig.*;
import org.apache.pig.data.*;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.executionengine.ExecException;

import org.apache.nutch.parse.*;
import org.apache.nutch.metadata.Metadata;


/**
 * Apache Pig UDF to load metadata records from a Nutch(WAX) segment.
 *
 * This loader assumes that the path is to the 'parse_data'
 * sub-directory of a Nutch(WAX) segment.
 *
 * It returns a Tuple for each link, of the form:
 *   (url:chararray,
 *    digest:chararray,
 *    title:chararray,
 *    length:long,
 *    date:chararray,
 *    type:chararray,
 *    collection:chararray,
 *    boiled:chararray,
 *    links: { tuple(toUrl:chararray, anchor:chararray) }
 * )
 */
public class MetadataLoader extends LoadFunc
{
  private RecordReader<Text,Writable> reader;

  private TupleFactory mTupleFactory = TupleFactory.getInstance();
  private BagFactory   mBagFactory   = BagFactory.getInstance();
  
  /**
   * The Nutch(WAX) "parse_data" segment is just a SequenceFile
   * with Text keys and Writable values.
   */ 
  public InputFormat getInputFormat( )
    throws IOException
  {
    return new SequenceFileInputFormat<Text,Writable>( );
  }

  /**
   * Reads a Nutch(WAX) metadata record and returns a Tuple containing
   * the metadata values.  Any null String values are returned as "",
   * but since 'length' is a Long, we return null if there is not a
   * length value for a record.
   */
  public Tuple getNext( )
    throws IOException
  {
    try 
      {
        if ( ! reader.nextKeyValue( ) )
          {
            return null;
          }
        
        Writable value;

        while ( ( value = this.reader.getCurrentValue( ) ) != null )
          {
            // Whoah, what to do?  Skip it for now
            if ( ! ( value instanceof ParseData ) ) continue ;
                    
            ParseData pd = (ParseData) value;

            Metadata meta = pd.getContentMeta( );

            Tuple tuple = mTupleFactory.newTuple( );

            tuple.append( meta.get( "url"    ) );
            tuple.append( meta.get( "digest" ) );
            tuple.append( pd.getTitle( ) );
            try { tuple.append( new Long( meta.get( "length" ) ) ); } catch ( NumberFormatException nfe ) { tuple.append( null ); }
            tuple.append( meta.get( "date"       ) );
            tuple.append( meta.get( "type"       ) );
            tuple.append( meta.get( "collection" ) );
            tuple.append( meta.get( "boiled"     ) );
            
            DataBag links = mBagFactory.newDefaultBag();
            for ( Outlink link : pd.getOutlinks() )
              {
                Tuple lt = mTupleFactory.newTuple( 2 );
                lt.set( 0, link.getToUrl ( ) );
                lt.set( 1, link.getAnchor( ) );

                links.add( lt );
              }

            tuple.append( links );

            return tuple;
          }

        return null;
      }
    catch ( InterruptedException e ) 
      {
        // From the Pig example/howto code.
        int errCode = 6018;
        String errMsg = "Error while reading input";
        throw new ExecException(errMsg, errCode,PigException.REMOTE_ENVIRONMENT, e);
      }
  }

  /**
   * Convenience function to ensure no nulls, only empty strings.
   */
  private String get( Metadata meta, String key )
  {
    String value = meta.get( key );

    if ( value == null ) return "";

    return value;
  }


  /**
   * Just save the given reader.  Dunno what to do with the 'split'.
   */
  public void prepareToRead( RecordReader reader, PigSplit split )
    throws IOException
  {
    this.reader = reader;
  }

  /**
   * The 'location' is a path string, which could contain wildcards.
   * Expand the wildcards and add each matching path to the input.
   */
  public void setLocation( String location, Job job )
    throws IOException
  {
    // Can we do this?
    //   
    // MultipleInputs.addInputPath( conf, new Path( p, "parse_data" ), SequenceFileInputFormat.class, Map.class );
    // MultipleInputs.addInputPath( conf, new Path( p, "parse_text" ), SequenceFileInputFormat.class, Map.class );

    // For now, let's assume the user gave the full path to the parse_data subdir.

    // Expand any filename globs, and add each to the input paths.
    FileStatus[] files = FileSystem.get( job.getConfiguration( ) ).globStatus( new Path( location ) );

    for ( FileStatus file : files )
      {
        FileInputFormat.addInputPath( job, file.getPath( ) );
      }
  }

}
