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
import java.util.zip.*;


public class TestMultipleGZIPInputStream
{
  public static void main( String[] args )
    throws Exception
  {
    MultipleGZIPInputStream is = new MultipleGZIPInputStream( new FileInputStream( args[0] ) );
    
    byte[] decoded = new byte[1024*4];

    while ( true )
      {
        try
          {
            int c;
            while ( ( c= is.read( decoded ) ) != -1 )
              {
                System.out.write( decoded, 0, c );
              }
            
            System.exit( 0 );
          }
        catch ( IOException e )
          {
            System.err.print( "Corrupt GZIP file at position: " + is.getRawPosition( ) + "...");
            if ( ! is.recover( ) )
              {
                System.err.println( "failed to recover." );
                System.exit( 1 );
              }
            System.err.println( "recovered at position: " + is.getRawPosition( ) );
          }
      }
  }
}
