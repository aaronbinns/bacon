
import java.io.*;
import java.util.*;


/**
 * InputStream that can decompress multiple catenated gzip members as
 * a single stream.  Like just about every gzip library in the
 * universe except the built-in Java one.
 */
public class MultipleGZIPInputStream extends InputStream
{
  public static final int    DEFAULT_SIZE = 1024 * 4;
  public static final byte[] GZIP_MAGIC   = { (byte) 0x1f, (byte) 0x8b };

  private PositionTrackingPushbackInputStream in;
  private int bufsize;
  private byte[] oneByteBuffer = { 0 };
  private long position;

  SingleGZIPInputStream gis = null;
  
  public MultipleGZIPInputStream( InputStream in )
  {
    this( in, DEFAULT_SIZE );
  }
  
  public MultipleGZIPInputStream( InputStream in, int bufsize )
  {
    this.in = new PositionTrackingPushbackInputStream( new PushbackInputStream( in, bufsize ) );
    this.bufsize = bufsize;
  }
  
  public int available( )
    throws IOException
  {
    return 0;
  }
  
  public void close( )
    throws IOException
  {
    this.in.close( );
  }

  public void mark( int readlimit )
  {
    // Do nothing, mark not supported
  }

  public boolean markSupported( )
  {
    return false;
  }

  public void reset( )
    throws IOException
  {
    throw new IOException( "reset() not supported." );
  }

  /**
   * 
   */
  public int read( )
    throws IOException
  {
    int c = this.read( oneByteBuffer, 0, oneByteBuffer.length );

    if ( c == -1 ) return -1;

    return oneByteBuffer[0];
  }
  
  /**
   * 
   */
  public int read( byte[] b )
    throws IOException
  {
    return this.read( b, 0, b.length );
  }

  /**
   * Will keep reading from multiple SingleGZIPInputStreams until the
   * buffer is full or we reach EOF.
   */
  public int read( byte[] b, int off, int len )
    throws IOException
  {
    if ( len == 0 ) return 0;

    int totalRead = 0;
    while ( len > totalRead )
      {
        if ( this.gis == null )
          {
            if ( eof( ) ) return totalRead == 0 ? -1 : totalRead;
           
            // System.err.println( "Start new GZIP record at: " + this.in.getPosition() );
 
            this.gis = new SingleGZIPInputStream( this.in, this.bufsize );
          }

        int c = 0;
        try
          {
            c = this.gis.read( b, off + totalRead, len - totalRead );
          }
        catch ( IOException e )
          {
            // System.err.println( "Error position: " + this.in.getPosition( ) + " remaining bytes: " + this.gis.getRemaining( ) + " totalRead: " + totalRead );

            if ( totalRead == 0 ) 
              {
                throw e;
              }

            return totalRead + c;
          }
        
        if ( c < 0 )
          {
            // System.err.println( "End compressed data at: " + (this.in.getPosition() - this.gis.getRemaining()) + " (" + this.in.getPosition( ) + " - " + this.gis.getRemaining( ) + ")" );
 
            this.in.unread( this.gis.getRemainingBytes( ) );
            
            this.gis = null;

            continue ;
          }
        
        totalRead += c;
      }

    return totalRead;
  }

  /**
   * Will keep skipping through multiple SingleGZIPInputStreams until
   * the requested number of bytes are skipped, or we reach EOF.
   */
  public long skip( long n )
    throws IOException
  {
    if ( n == 0 ) return -1;

    long totalSkipped = 0;
    while ( n > totalSkipped )
      {
        if ( this.gis == null )
          {
            if ( eof( ) ) return totalSkipped == 0 ? -1 : totalSkipped;
            
            this.gis = new SingleGZIPInputStream( this.in, this.bufsize );
          }
        
        long c = this.gis.skip( n - totalSkipped );
        
        if ( c < 0 )
          {
            this.in.unread( this.gis.getRemainingBytes( ) );
            
            this.gis = null;

            continue ;
          }

        totalSkipped += c;
      }

    return totalSkipped;
  }

  /**
   * Peek ahead one byte to see if we are at EOF.
   */
  public boolean eof( )
    throws IOException
  {
    int peek = this.in.read();

    if ( peek == -1 ) return true;

    this.in.unread( peek );
    
    return false;
  }

  /**
   * Returns the "raw" position, that is, the byte position in the
   * underlying uncompressed input stream.  Also takes into account
   * any bytes buffered in the Inflater.
   */
  public long getRawPosition( )
  {
    long pos = this.in.getPosition( );

    // If gis != null, then there may be uncompressed bytes loaded
    // into its buffer.
    if ( this.gis != null ) pos -= this.gis.getRemaining( );
    
    return pos;
  }

  public boolean recover( )
    throws IOException
  {
    this.in.unread( this.gis.getRemainingBytes( ) );
    
    this.gis = null;

    return scanToNext( );
  }

  public boolean scanToNext( )
    throws IOException
  {
    int b0, b1;
    while ( ( b0 = this.in.read() ) != -1 )
      {
        do 
          {
            if ( GZIP_MAGIC[0] == (byte) b0 )
              {
                b1 = this.in.read( );

                if ( b1 == -1 ) return false;
                
                if ( GZIP_MAGIC[1] == (byte) b1 )
                  {
                    this.in.unread( GZIP_MAGIC );
                    return true;
                  }
                
                b0 = b1;
              }
            else
              {
                break ;
              }
          } while ( true );
      }

    return false;
  }
}
