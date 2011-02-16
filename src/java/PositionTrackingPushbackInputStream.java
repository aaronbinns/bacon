
import java.io.*;
import java.util.*;


/**
 *
 */
public class PositionTrackingPushbackInputStream extends InputStream
{
  private PushbackInputStream in;
  private long position;

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
  }


  public int read( )
    throws IOException
  {
    int c = this.in.read();

    if ( c != -1 ) position++;

    return c;
  }
  
  /**
   * 
   */
  public int read( byte[] b )
    throws IOException
  {
    int c = this.in.read( b );

    if ( c != -1 ) position += c;
    
    return c;
  }

  /**
   * Will keep reading from multiple SingleGZIPInputStreams until the
   * buffer is full or we reach EOF.
   */
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
