
import java.io.*;
import java.net.*;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;

 
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

        return this.helper.getDomain( u );
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