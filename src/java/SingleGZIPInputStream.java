

import java.io.*;
import java.util.*;
import java.util.zip.*;

/**
 * Simple extension of Java GZIPInputStream that exposes the remaning
 * bytes in the decompression buffer after decompression has
 * completed.
 */
public class SingleGZIPInputStream extends GZIPInputStream
{
  public static final byte[] EMPTY = { };

  public SingleGZIPInputStream( InputStream in )
    throws IOException
  {
    super( in );
  }

  public SingleGZIPInputStream( InputStream in, int size )
    throws IOException
  {
    super( in, size );
  }

  /**
   * Return the bytes in the decompression buffer which are after the
   * end of the GZIP record.  If we are at the end of the underlying
   * input stream, then we return an empty byte[].
   */
  public byte[] getRemainingBytes( )
  {
    int n = inf.getRemaining();
    
    // If there are more than 8 bytes remaining in the decompression
    // buffer, then those first 8 are the GZip record tailer which we
    // just skip over.
    if ( n > 8 )
      {
        return Arrays.copyOfRange( buf, len - n + 8, len );
      }

    // Otherwise, there are 8 or fewer bytes, then we just ignore them
    // and return and empty byte array.
    return EMPTY;
  }

  public int getRemaining( )
  {
    int n = inf.getRemaining( );

    return n;
  }
}
