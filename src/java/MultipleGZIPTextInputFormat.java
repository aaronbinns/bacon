
import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.conf.Configuration;


public class MultipleGZIPTextInputFormat extends TextInputFormat
{
  public RecordReader<LongWritable,Text> createRecordReader(InputSplit split, TaskAttemptContext context) 
  {
    return new RecordReader<LongWritable,Text>( )
      {
        MultipleGZIPInputStream mgis;
        LineReader reader;
        
        long start, pos, end;

        LongWritable key   = null;
        Text         value = null;

        public void initialize( InputSplit genericSplit, TaskAttemptContext context )
          throws IOException
        {
          Configuration job = context.getConfiguration();

          FileSplit split = (FileSplit) genericSplit;
          Path      path  = split.getPath( );

          this.mgis   = new MultipleGZIPInputStream( path.getFileSystem( job ).open( path ) );
          this.reader = new LineReader( this.mgis );
          
          this.start = split.getStart();
          this.end   = start + split.getLength();
          this.pos   = start;
        }

        public synchronized void close( )
          throws IOException
        {
          if ( this.reader != null )
            {
              this.reader.close();
            }
        }
        
        public LongWritable getCurrentKey() 
        {
          return this.key;
        }
        
        public Text getCurrentValue() 
        {
          return this.value;
        }
        
        public float getProgress() 
        {
          if ( this.start == this.end ) 
            {
              return 0.0f;
            } 

          return Math.min( 1.0f, (this.pos - this.start) / (float) (this.end - this.start) );
        }
        
        public boolean nextKeyValue() 
          throws IOException
        {
          if ( key   == null ) key   = new LongWritable();
          if ( value == null ) value = new Text();
          
          int c = 0;

          c = reader.readLine( value, Integer.MAX_VALUE, Integer.MAX_VALUE );

          if ( c == 0 )
            {
              key   = null;
              value = null;
              return false;
            }
          
          this.pos = this.mgis.getRawPosition( );
          
          return true;
        }
      };
  }

  protected boolean isSplitable( JobContext context, Path file ) 
  {
    return false;
  }

}