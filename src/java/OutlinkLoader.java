
import java.io.*;
import java.util.*;

import org.apache.hadoop.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;

import org.apache.pig.*;
import org.apache.pig.data.*;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.executionengine.ExecException;

import org.apache.nutch.parse.*;

public class OutlinkLoader extends LoadFunc
{
  private RecordReader<Text,Writable> reader;
  
  String    from     = null;
  Outlink[] outlinks = null;
  int       pos      = 0;
  
  public InputFormat getInputFormat( )
    throws IOException
  {
    return new SequenceFileInputFormat<Text,Writable>( );
  }

  public Tuple getNext( )
    throws IOException
  {
    try 
      {
        if ( from == null )
          {
            if ( ! reader.nextKeyValue( ) )
              {
                return null;
              }
            
            Writable value = this.reader.getCurrentValue();
            
            if ( value instanceof ParseData )
              {
                ParseData pd = (ParseData) value;
                
                this.from     = pd.getContentMeta( ).get( "url" );
                this.outlinks = pd.getOutlinks( );
                this.pos      = 0;

                // Is it possible that the record's metadata has a
                // null url property?  Just in case, use "".
                if ( this.from == null ) this.from = "";
              }
            else
              {
                // Weird!
              }
          }

        if ( this.from != null )
          {
            if ( this.pos < outlinks.length )
              {
                Tuple tuple = TupleFactory.getInstance( ).newTuple( );
                
                Outlink outlink = this.outlinks[this.pos++];

                String toUrl  = outlink.getToUrl( );
                String anchor = outlink.getAnchor( );

                if ( toUrl  == null ) toUrl  = "";
                if ( anchor == null ) anchor = "";
                
                tuple.append( from   );
                tuple.append( toUrl  );
                tuple.append( anchor );
                    
                if ( this.pos >= outlinks.length )
                  {
                    // Reset the state variables to trigger reading of
                    // the next record.
                    this.from     = null;
                    this.outlinks = null;
                    this.pos      = 0;
                  }
                
                return tuple;
              }
          }

        return null;
      }
    catch ( InterruptedException e ) 
      {
        // From the Pig example/howto code.
        int errCode = 6018;
        String errMsg = "Error while reading input";
        throw new ExecException(errMsg, errCode,PigException.REMOTE_ENVIRONMENT, e);
      }
  }

  public void prepareToRead( RecordReader reader, PigSplit split )
    throws IOException
  {
    this.reader = reader;
  }

  public void setLocation( String location, Job job )
    throws IOException
  {
    // Can we do this?
    //   
    // MultipleInputs.addInputPath( conf, new Path( p, "parse_data" ), SequenceFileInputFormat.class, Map.class );
    // MultipleInputs.addInputPath( conf, new Path( p, "parse_text" ), SequenceFileInputFormat.class, Map.class );

    // For now, let's assume the user gave the full path tot he parse_data subdir.
    FileStatus[] files = FileSystem.get( job.getConfiguration( ) ).globStatus( new Path( location ) );

    for ( FileStatus file : files )
      {
        FileInputFormat.addInputPath( job, file.getPath( ) );
      }
  }

}
