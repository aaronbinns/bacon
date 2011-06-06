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
 * Apache Pig UDF to load parsed_content records from a Nutch(WAX)
 * segment.
 *
 * This loader assumes that the path is to the 'parse_text'
 * sub-directory of a Nutch(WAX) segment.
 *
 * It returns a Tuple for each link, of the form:
 *   (url:chararray,
 *    digest:chararray,
 *    content:chararray)
 */
public class ContentLoader extends LoadFunc
{
  private RecordReader<Text,Writable> reader;
  
  
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

        String key = this.reader.getCurrentKey().toString();

        int lastSpace = key.lastIndexOf( ' ' );
        if ( lastSpace < 0 || (lastSpace == key.length() - 1) ) return null;

        String url    = key.substring( 0, lastSpace );
        String digest = key.substring( lastSpace + 1 );
        
        Writable value;

        while ( ( value = this.reader.getCurrentValue( ) ) != null )
          {
            // Whoah, what to do?  Skip it for now
            if ( ! ( value instanceof ParseText ) ) continue ;
                    
            ParseText pt = (ParseText) value;

            String content = pt.toString();

            Tuple tuple = TupleFactory.getInstance( ).newTuple( );

            tuple.append( url    );
            tuple.append( digest );
            tuple.append( content );

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
    // For now, let's assume the user gave the full path to the parse_text subdir.

    // Expand any filename globs, and add each to the input paths.
    FileStatus[] files = FileSystem.get( job.getConfiguration( ) ).globStatus( new Path( location ) );

    for ( FileStatus file : files )
      {
        FileInputFormat.addInputPath( job, file.getPath( ) );
      }
  }

}
