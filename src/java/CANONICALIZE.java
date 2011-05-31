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
import java.net.*;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;

import org.archive.wayback.util.url.AggressiveUrlCanonicalizer;

/**
 * Simple Pig EvalFunc which takes a chararray assumed to be a URL and
 * returns the domain, as determined by the IDNHelper.
 */ 
public class CANONICALIZE extends EvalFunc<String>
{
  AggressiveUrlCanonicalizer canonicalizer;

  public CANONICALIZE( )
    throws IOException
  {
    this.canonicalizer = new AggressiveUrlCanonicalizer();
  }


  public String exec( Tuple input )
    throws IOException 
  {
    if ( input == null || input.size() == 0 ) return null;

    try
      {
        String c = this.canonicalizer.canonicalize( (String) input.get(0) );

        if ( c.length() > 10 && c.startsWith("http://www.") || c.startsWith("https://www.") )
          {
            c = "http://" + c.substring(11);
          }

        return c;
      }
    catch ( Exception e )
      {
        throw WrappedIOException.wrap("Caught exception processing input row ", e);
      }
  }
}