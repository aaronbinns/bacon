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
import java.util.*;
import java.util.regex.*;

/**
 * <p>
 *   Helper class for handling (international) domain names which
 *   determines which part of a fully-qualified hostname is the
 *   domain, or "site".
 * </p>
 * <p>
 *   It's designed to use the rules maintained by Mozilla and the Public
 *   Suffix List project:
 *   <ul>
 *     <li>http://mxr.mozilla.org/mozilla-central/source/netwerk/dns/effective_tld_names.dat?raw=1</li>
 *     <li>http://publicsuffix.org/index.html</li>
 *   </ul>
 * </p>
 * <p>
 * Typically, it is instantiated with the rules from the
 * <code>effective_tld_names.dat</code> file, but the rules can be
 * augmented for custom domain name determination.
 * For example, if a project wanted to treat each subdomain
 * under <code>blogger.com</code> as a separate domain, then
 * the rule could be added:
 * <pre>blogger.com</pre>
 * which would yield:
 * <pre>foo.blogger.com
 * bar.blogger.com
 * baz.blogger.com</pre>
 * as separate domains.  Without this rule, they would all be
 * collasped into just <code>blogger.come</code>.
 * </p>
 */
public class IDNHelper
{
  public Set<String>  exact   = new HashSet<String>();
  public Set<String>  exclude = new HashSet<String>();
  public Set<Pattern> wild    = new HashSet<Pattern>();

  public void addRule( String rule )
  {
    // Handle simple wildcards rules
    if ( rule.startsWith( "*." ) )
      {
        if ( rule.length() < 3 ) return ;

        rule = IDN.toASCII( rule.substring( 2 ) );

        // Transform the rule string into regex syntax
        rule = "[^.]+[.][^.]+[.]" + rule.replace( ".", "[.]" ) ;
        
        Pattern p = Pattern.compile( rule );
        
        wild.add( p );
        
        return ;
      }
    
    // Full-blown regex rules
    if ( rule.startsWith( "~" ) )
      {
        rule = rule.substring( 1 );
        
        Pattern p = Pattern.compile( rule );
        
        wild.add( p );
        
        return ;
      }
    
    // Exact and exclude rules.
    Set<String> rules = exact;
    
    if ( rule.startsWith( "!" ) )
      {
        if ( rule.length() == 1 ) return ;
        
        rules = exclude;
        
        rule = rule.substring( 1 );
      }
    
    rules.add( IDN.toASCII( rule ) );    
  }
  
  /**
   * Adds rules from the given Reader.  Rules are expected to conform
   * to syntax in Mozilla's effective_tld_names.txt document.
   */
  public void addRules( Reader r )
    throws IOException
  {
    BufferedReader reader = new BufferedReader( r );

    String line;
    while ( (line = reader.readLine() ) != null )
      {
        line = line.trim();
        if ( line.length() == 0 || line.startsWith( "//" ) ) continue; 
        
        this.addRule( line );
      }
  }

  /**
   * Return the domain of the given url, according to the rules added
   * to the IDNHelper.
   */
  public String getDomain( URL u )
  {
    return getDomain( u.getHost( ) );
  }

  /**
   * Return the domain of the given host string, according to the
   * rules added to the IDNHelper.  The input host string is expected
   * to be a valid fully-qualified hostname, such as those returned by
   * URL.getHost().
   *
   * Returns <code>null</code> if domain cannot be determined.
   */
  public String getDomain( String host )
  {
    try
      {
        host = IDN.toASCII( host, IDN.ALLOW_UNASSIGNED );
      }
    catch ( Exception e )
      {
        host = null;
      }

    if ( host == null ) return null;

    int i;
    while ( (i = host.indexOf( '.' ) ) != -1 )
      {
        String test = host.substring( i + 1 );

        if ( exact.contains( test ) )
          {
            return host;
          }

        if ( exclude.contains( test ) )
          {
            return test;
          }
        
        if ( exclude.contains( host ) )
          {
            return host;
          }

        for ( Pattern p : wild )
          {
            Matcher m = p.matcher( host );

            if ( m.matches( ) )
              {
                if ( m.groupCount() > 0 )
                  {
                    return m.group( 1 );
                  }

                return host;
              }
          }
        
        host = test;
      }

    return null;    
  }

  /**
   * Constructs a new IDNHelper object, populating it with rules from
   * given Reader.  Rules are expected to be in the same form as
   * Mozilla's effective_tld_names.dat file.
   */
  public static IDNHelper build( Reader reader )
    throws IOException
  {
    IDNHelper helper = new IDNHelper( );

    helper.addRules( reader );

    return helper;
  }

  /**
   * Command-line test driver.
   */
  public static void main( String[] args )
    throws Exception
  {
    if ( args.length < 2 || args[0].equals( "-h" ) || args[0].equals( "--help" ) )
      {
        usage();
        System.exit( 0 );
      }

    Reader reader = new InputStreamReader( new FileInputStream( args[0] ), "utf-8" );

    IDNHelper helper = build( reader );

    for ( int i = 1; i < args.length ; i++ )
      {
        if ( args[i].equals("-") )
          {
            BufferedReader r = new BufferedReader( new InputStreamReader( System.in, "utf-8" ) );
            
            String line;
            while ( ( line = r.readLine() ) != null )
              {
                line = line.trim();
                if ( line.length() == 0 || line.startsWith( "#" ) || line.startsWith( "//" ) ) continue; 
                
                URL u = new URL( line );
                
                System.out.println( helper.getDomain( u.getHost( ) ) + "\t" + line );
              }
          }
        else
          {
            URL u = new URL( args[i] );
            
            System.out.println( helper.getDomain( u.getHost( ) ) + "\t" + args[i] );
          }
      }
    

  }

  public static void usage( )
  {
    System.out.println( "IDNHelper <rules> <url>..." );
    System.out.println( "  Load rules and emit domain for given URLs" );
    System.out.println( "  If <url> is '-' then URLs will be read from stdin." );
  }

}
