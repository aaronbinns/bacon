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

/**
 * Simple Pig EvalFunc which takes a chararray assumed to be a URL and
 * returns the domain, as determined by the IDNHelper.
 */ 
public class DOMAIN extends EvalFunc<String>
{
  IDNHelper helper;

  public DOMAIN( )
    throws IOException
  {
    InputStream is = IDNHelper.class.getClassLoader( ).getResourceAsStream( "effective_tld_names.dat" );

    if ( is == null )
      {
        throw new RuntimeException( "Cannot load tld rules: effective_tld_names.dat" );
      }

    Reader r = new InputStreamReader( is, "utf-8" );
    
    this.helper = IDNHelper.build( r );
  }


  public String exec( Tuple input )
    throws IOException 
  {
    if ( input == null || input.size() == 0 ) return null;

    try
      {
        URL u = new URL( (String) input.get(0) );

        String domain = this.helper.getDomain( u );
        
        // If domain cannot be determined, return empty string.
        if ( domain == null ) domain = "";

        return domain;
      }
    catch ( MalformedURLException mue )
      {
        // If nto a valid URL, just return an empty string.
        return "";
      }
    catch ( Exception e )
      {
        throw WrappedIOException.wrap("Caught exception processing input row ", e);
      }
  }
}