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
package org.archive.bacon.url;

import java.io.*;
import java.net.*;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;

import org.archive.wayback.util.url.AggressiveUrlCanonicalizer;

/**
 * Pig EvalFunc which calls the Wayback aggressive URL canonicalizer.
 * 
 * The Wayback aggressive URL canonicalizer does <em>not</em> strip
 * 'www.' from the front of many URLs (JIRA ACC-109), so we do that
 * ourselves, after the Wayback canonicalizer is finished.
 */ 
public class Canonicalize extends EvalFunc<String>
{
  AggressiveUrlCanonicalizer canonicalizer;

  public Canonicalize( )
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

        // In some rare cases the Wayback canonicalizer can return null.
        if ( c == null ) return null;

        // See JIRA ACC-109
        if ( c.length() > 10 )
          {
            if ( c.startsWith("http://www.") ) 
              {
                c = "http://" + c.substring(11);
              }
            else if ( c.startsWith("https://www.") )
              {
                c = "https://" + c.substring(12);
              }
          }

        // Ensure i18n domains are canonicalized into PunyCode.
        try
          {
            URL u = new URL(c);

            String host  = u.getHost();

            if ( host != null )
              {
                String ahost = IDN.toASCII( host, java.net.IDN.ALLOW_UNASSIGNED );
                
                if ( ! host.equals( ahost ) )
                  {
                    u = new URL( u.getProtocol(),
                                 ahost,
                                 u.getPort(),
                                 u.getFile() );
                    
                    c = u.toString();
                  }
              }

            // Ensure http://example.org has trailing '/'
            String path = u.getPath() == "" ? "/" : u.getPath();

            // Run the path through a decoder, then re-encode and normalize using the URI class.
            path = path.replaceAll( "[+]", "%2b" );  // Don't decode '+' into ' '.
            path = URLDecoder.decode( path, "utf-8" );
            URI uriPath = new URI( null, null, path, null );
            try
              {
                uriPath = uriPath.normalize();
              }
            catch ( InternalError ie )
              {
                System.err.println( "A-ha, triggers URI's InternalError: " + path );
              }
            path = uriPath.getRawPath();
            
            // Hacks for the query
            String query = u.getQuery();
            if ( query != null )
              {
                // Strip multiple & and any trailing &
                query = "?" + query;
                query = query.replaceAll( "[&][&]+", "&" );
                query = query.replaceAll( "[&]$", "" );
              }
            else
              {
                query = "";
              }

            // Hack for "#!/foo" stuff, a la twitter, etc.
            String ref = u.getRef() != null && u.getRef().startsWith( "!" ) ? u.getRef() : null;
            if ( ref != null )
              {
                ref = "#" + ref;
              }
            else
              {
                ref = "";
              }

            // Now, rebuild the URL path + query + ref using the modified values.
            u = new URL( u, path + query + ref );

            // If the protocol is http and the port is explicitly set
            // as "80", strip out the explicit port.
            if ( ("http" .equals(u.getProtocol()) && 80  == u.getPort()) ||
                 ("https".equals(u.getProtocol()) && 443 == u.getPort()) )
              {
                u = new URL( u.getProtocol(), 
                             u.getHost(),
                             -1,
                             u.getFile() );
              }

            c = u.toString();
          }
        catch ( Throwable t )
          {
            // Do nothing, leave the canonicalized URI as it is.
          }

        if ( c == null ) return null;

        // Last, but not least, ensure no whitespace in the URL
        // Change ' ' to %20 but remove all other whitespace.
        c = c.trim();
        c = c.replaceAll( "[ ]", "%20" );
        c = c.replaceAll( "\\s", "" );
        
        return c;
      }
    catch ( Exception e )
      {
        throw WrappedIOException.wrap("Caught exception processing input row ", e);
      }
  }

}
