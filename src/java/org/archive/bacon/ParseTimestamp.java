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
package org.archive.bacon;

import java.io.*;
import java.net.*;
import java.util.*;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.WrappedIOException;

/**
 * Parse both WARC and ARC format timestamps.
 */ 
public class ParseTimestamp extends EvalFunc<String>
{

  public ParseTimestamp( )
    throws IOException
  {
    
  }

  public String exec( Tuple input )
    throws IOException 
  {
    if ( input == null || input.size() == 0 ) return null;

    try
      {
        String date = (String) input.get(0);

        int len = date.length();

        String format = null;

        if ( len == 12 ) format = "YYYYMMddHHmm";
        if ( len == 14 ) format = "YYYYMMddHHmmss";
        if ( len == 20 ) format = "YYYY-MM-dd'T'HH:mm:ss'Z'";

        if ( format == null ) return null;  // Unknown format.

        // Set the time to default or the output is in UTC
        DateTimeZone.setDefault(DateTimeZone.UTC);
        
        // See http://joda-time.sourceforge.net/api-release/org/joda/time/format/DateTimeFormat.html
        DateTimeFormatter parser = DateTimeFormat.forPattern(format);
        DateTime result = parser.parseDateTime(date);
        
        return result.toString();
      }
    catch ( Exception e )
      {
        // If we have any problems parsing the date, just return null;
        return null;
      }
  }

  @Override
  public Schema outputSchema(Schema input)
  {
    return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.CHARARRAY));
  }
  
  @Override
  public List<FuncSpec> getArgToFuncMapping() throws FrontendException
  {
    List<FuncSpec> funcList = new ArrayList<FuncSpec>();
    Schema s = new Schema();
    s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
    funcList.add(new FuncSpec(this.getClass().getName(), s));
    return funcList;
  }
  
}
