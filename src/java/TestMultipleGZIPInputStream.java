
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
