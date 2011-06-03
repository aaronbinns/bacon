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
package org.archive.bacon;

import java.io.*;
import java.net.*;
import java.text.*;
import java.util.*;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;

/**
 * Simple Pig EvalFunc which takes a chararray assumed to be a "date"
 * in WARC or ARC format and convert it to "number of seconds since
 * the epoch" format, returning a long value.
 */ 
public class TIME extends EvalFunc<Long>
{
  
  public TIME( )
    throws IOException
  {
    
  }

  public Long exec( Tuple input )
    throws IOException 
  {
    if ( input == null || input.size() == 0 ) return null;

    try
      {
        String s = (String) input.get(0);

        String format = null;
        switch ( s.length() )
          {
          default:
            // Unknown format
            return null;

          case 20:
            // WARC format: "2011-05-05T18:55:26Z"
            format = "yyyy-MM-dd'T'HH:mm:ss'Z'";
            break;

          case 14:
            // ARC format : "20110505185526"
            format = "yyyyMMddHHmmss";
            break;
          }

        SimpleDateFormat sdf = new SimpleDateFormat( format );

        Date d = sdf.parse( s );
        
        return d.getTime();
      }
    catch ( Exception e )
      {
        throw WrappedIOException.wrap("Caught exception processing input row ", e);
      }
  }
}
