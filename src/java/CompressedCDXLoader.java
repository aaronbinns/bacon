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
 * Apache Pig UDF to load CDX files using multi-gzip compression.
 *
 * It returns a Tuple with as many fields as in the CDX line, which is
 * usually 9.  The fields are untyped.
 */
public class CompressedCDXLoader extends LoadFunc
{
  private RecordReader<LongWritable,Text> reader;

  public InputFormat getInputFormat( )
    throws IOException
  {
    return new MultipleGZIPTextInputFormat( );
  }

  public Tuple getNext( )
    throws IOException
  {
    try 
      {
        if ( ! reader.nextKeyValue( ) )
          {
            return null;
          }
        
        Tuple tuple = TupleFactory.getInstance( ).newTuple( );
        
        Text line = this.reader.getCurrentValue( );
        
        String[] fields = line.toString().split( "[ ]" );
        
        for ( String field : fields )
          {
            tuple.append( field );
          }
        
        return tuple;
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
    // Expand any filename globs, and add each to the input paths.
    FileStatus[] files = FileSystem.get( job.getConfiguration( ) ).globStatus( new Path( location ) );
    
    for ( FileStatus file : files )
      {
        FileInputFormat.addInputPath( job, file.getPath( ) );
      }
  }

}
