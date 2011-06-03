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
package org.archive.bacon.cdx;

import java.io.*;

/**
 * Simple utility class which wraps a PushbackInputStream, keeping
 * track of the byte position in the underlying stream.
 */
public class PositionTrackingPushbackInputStream extends InputStream
{
  private PushbackInputStream in;
  private long position;
  private long mark;

  public PositionTrackingPushbackInputStream( PushbackInputStream in )
  {
    this.in = in;
  }

  public long getPosition( )
  {
    return position;
  }

  public int available() 
    throws IOException
  {
    return this.in.available( );
  }

  public void close()
    throws IOException
  {
    this.in.close( );
  }

  public void mark( int readlimit )
  {
    if ( this.in.markSupported( ) )
      {
        this.mark = this.position;
      }
    this.in.mark( readlimit );
  }

  public boolean markSupported( ) 
  {
    return this.in.markSupported( );
  }

  public void reset( ) 
    throws IOException
  {
    this.in.reset( );

    if ( this.in.markSupported( ) )
      {
        this.position = this.mark;
      }
  }

  public int read( )
    throws IOException
  {
    int c = this.in.read();

    if ( c != -1 ) position++;

    return c;
  }
  
  public int read( byte[] b )
    throws IOException
  {
    int c = this.in.read( b );

    if ( c != -1 ) position += c;
    
    return c;
  }

  public int read( byte[] b, int off, int len )
    throws IOException
  {
    int c = this.in.read( b, off, len );

    if ( c != -1 ) position += c;
    
    return c;
  }

  public long skip( long n )
    throws IOException
  {
    long c = this.in.skip( n );

    position += c;

    return c;
  }

  public void unread( int b )
    throws IOException
  {
    this.in.unread( b );

    position--;
  }
  
  public void unread( byte[] b )
    throws IOException
  {
    this.in.unread( b );

    position -= b.length;
  }
  
  public void unread( byte[] b, int offset, int len )
    throws IOException
  {
    this.in.unread( b, offset, len );

    position -= len;
  }

}
