
import java.io.*;
import java.util.zip.*;


public class TestSingleGZIPInputStream
{
  public static void usage( )
  {
    System.out.println( TestSingleGZIPInputStream.class.getSimpleName() + " [OPTION]... <gzip file>" );
    System.out.println( );
    System.out.println( "\t-d <size>   Size of decode buffer"  );
    System.out.println( "\t-i <size>   Size of inflate buffer" );
    System.exit(1);
  }

  public static void parseArgs( String[] args )
  {
    try
      {
        for ( int i = 0 ; i < args.length ; i++ )
          {
            if ( "-d".equals( args[i] ) )
              {
                decodeSize = Integer.parseInt( args[++i] );

                if ( decodeSize <= 0 ) usage();
              }
            else if ( "-i".equals( args[i] ) )
              {
                inflateSize = Integer.parseInt( args[++i] );

                if ( inflateSize <= 0 ) usage();
              }
            else if ( "-".equals( args[i] ) )
              {
                is = System.in;

                return ;
              }
            else if ( args[i].startsWith( "-" ) )
              {
                usage();
              }
            else
              {
                is = new FileInputStream( args[i] );

                return ;
              }
          }
      }
    catch ( NumberFormatException nfe )
      {
        usage();
      }
    catch ( ArrayIndexOutOfBoundsException oob ) 
      {
        usage();
      }
    catch ( FileNotFoundException fnf )
      {
        System.err.println( "Error: " + fnf.getMessage( ) );
        System.exit(2);
      }
  }
  
  public static int decodeSize  = 1024 * 4;
  public static int inflateSize = 1024;
  public static InputStream is  = System.in;
  
  public static void main( String[] args )
    throws Exception
  {
    parseArgs( args );

    SingleGZIPInputStream gis = new SingleGZIPInputStream( is, inflateSize );
    
    byte[] buf = new byte[decodeSize];

    int c;
    while ( (c = gis.read( buf ) ) != -1 )
      {
        System.out.write( buf, 0, c );
      }
    
    System.out.println( "Num remaining bytes: " + gis.getRemaining() );
  }
}