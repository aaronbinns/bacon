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

/**
 * Apache Pig UDF to load outlinks from a Nutch(WAX) segment.
 *
 * This loader assumes that the path is to the 'parse_data'
 * sub-directory of a Nutch(WAX) segment.
 *
 * It returns a Tuple for each link, of the form:
 *   (from:chararray,to:chararray,anchor:chararray)
 */
public class OutlinkLoader extends LoadFunc
{
  private RecordReader<Text,Writable> reader;
  
  String    from     = null;
  Outlink[] outlinks = null;
  int       pos      = 0;
  
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
   * This method is slightly complicated because each NutchWAX record
   * contains all the outlinks for that page.  Thus 1 NW record can
   * produce many Tuples.  So, this code advances to the next record,
   * saves its place and emits Tuples for all the links; then advances
   * to the next record.
   *
   * Any 'null' values are mapped to "" for simplicity.
   */
  public Tuple getNext( )
    throws IOException
  {
    try 
      {
        if ( from == null )
          {
            if ( ! reader.nextKeyValue( ) )
              {
                return null;
              }
            
            Writable value = this.reader.getCurrentValue();
            
            if ( value instanceof ParseData )
              {
                ParseData pd = (ParseData) value;
                
                this.from     = pd.getContentMeta( ).get( "url" );
                this.outlinks = pd.getOutlinks( );
                this.pos      = 0;

                // Is it possible that the record's metadata has a
                // null url property?  Just in case, use "".
                if ( this.from == null ) this.from = "";
              }
            else
              {
                // Whoah, what to do?  For now just skip it.
              }
          }

        if ( this.from != null )
          {
            if ( this.pos < outlinks.length )
              {
                Tuple tuple = TupleFactory.getInstance( ).newTuple( );
                
                Outlink outlink = this.outlinks[this.pos++];

                String toUrl  = outlink.getToUrl( );
                String anchor = outlink.getAnchor( );

                if ( toUrl  == null ) toUrl  = "";
                if ( anchor == null ) anchor = "";
                
                tuple.append( from   );
                tuple.append( toUrl  );
                tuple.append( anchor );
                    
                if ( this.pos >= outlinks.length )
                  {
                    // Reset the state variables to trigger reading of
                    // the next record.
                    this.from     = null;
                    this.outlinks = null;
                    this.pos      = 0;
                  }
                
                return tuple;
              }
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
