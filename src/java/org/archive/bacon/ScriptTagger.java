/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.archive.bacon;

import java.io.*;
import java.text.*;
import java.util.*;
import static java.lang.Character.UnicodeBlock.*;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.FuncSpec;

/**
 * Experimental Pig EvalFunc which takes a bag of tokens (strings) and
 * returns a bag of (sub)tokens tagged with the Unicode script it
 * belongs to.
 *
 * Tokens are split across script boundaries, but are not otherwise
 * tokenized.  For example, if the input is a bag of tokens:
 *
 *  {(foo),(ウェ),(baz)}
 *
 * The output will be a bag with the same tokens, tagged with their scripts:
 *
 *  {(foo,LATIN),(ウェ:CJK),(baz:LATIN)}
 *
 * However, if a token is comprised of multiple scripts, the
 * tokens will be split on the script boundary, for example
 * the single token comprised of multiple scripts
 *
 *  {(fooウェbar)}
 *
 * returns:
 *
 *  {(foo,LATIN),(ウェ:CJK),(baz:LATIN)}
 *
 * When detecting scripts, non-letters are ignored.  Thus, one can
 * pass in a large block of mixed text and get back a bag of tokens
 * split on the script boundaries, for example:
 * (from http://languagelog.ldc.upenn.edu/nll/?p=3246#comment-128736)
 *  
 *  "From my own observation, biànyi is seldom used in Chinese daily conversations. When we speak 便宜, we just mean "cheap."
 *
 * returns:
 *
 *  {('"From my own observation, biànyi is seldom used in Chinese daily conversations. When we speak ',LATIN),
 *   ('便宜, ',CJK),
 *   ('we just mean "cheap."',LATIN)}
 *
 * NOTE on Unicode scripts and Java: 
 *
 *   http://stackoverflow.com/questions/4237488/obtaining-unicode-characters-of-a-language-in-java/4242383#4242383
 * 
 * The short answer is that Java 6 only supports Unicode 4.0 and has
 * fairly crappy support at that.  Java 6 doesn't actually provide a
 * "what script is this codepoint" API, so we kludge something using
 * Unicode blocks and grouping them together according to our own
 * decision about what goes together.  See the getScript() method for
 * specifics.
 */
public class ScriptTagger extends EvalFunc<DataBag> 
{
  TupleFactory mTupleFactory = TupleFactory.getInstance();
  BagFactory   mBagFactory   = BagFactory.getInstance();
  
  @Override
  public DataBag exec( Tuple input ) throws IOException
  {
    try
      {
        if ( input == null || input.size() < 1 ) return null;
        
        DataBag tokens = (DataBag) input.get(0);
        
        if ( tokens == null ) return null;
        
        DataBag output = mBagFactory.newDefaultBag();

        for ( Tuple t : tokens  )
          {
            String token = (String) t.get(0);

            token = token.trim();
            
            if ( token.length() == 0 ) continue;

            // Normalize the Unicode string to NFC form.
            token = Normalizer.normalize( token, Normalizer.Form.NFC );

          loop: 
            while ( true )
              {
                int cpLength = token.codePointCount( 0, token.length() );

                if ( cpLength < 1 ) break ;

                int codePoint = token.codePointAt( 0 );
                
                String script = getScript(codePoint);
                
                for ( int cpi = 1 ; cpi < cpLength; cpi++ )
                  {
                    codePoint = token.codePointAt( token.offsetByCodePoints(0,cpi) );

                    if ( ! Character.isLetter( codePoint ) ) continue ;
                    
                    String nextScript = getScript( codePoint );
                    
                    if ( ! script.equals( nextScript ) )
                      {
                        String subtoken = token.substring( 0, token.offsetByCodePoints(0,cpi) );
                        
                        // Add tagged subtoken to output
                        Tuple tout = mTupleFactory.newTuple( 2 );
                        tout.set( 0, subtoken );
                        tout.set( 1, script );

                        output.add( tout );

                        script = nextScript;
                        token = token.substring( token.offsetByCodePoints(0,cpi) );

                        continue loop;
                      }
                  }
                
                Tuple tout = mTupleFactory.newTuple( 2 );
                tout.set( 0, token );
                tout.set( 1, script );

                output.add( tout );

                break ;
              }
          }

        return output;
      }
    catch (ExecException ee) 
      {
        throw ee;
      }
  }

  String getScript( int codePoint )
  {
    Character.UnicodeBlock block = Character.UnicodeBlock.of( codePoint );

    if ( block == BASIC_LATIN
         || block == LATIN_1_SUPPLEMENT 
         || block == LATIN_EXTENDED_A 
         || block == LATIN_EXTENDED_ADDITIONAL 
         || block == LATIN_EXTENDED_B 
         || block == IPA_EXTENSIONS )
      {
        return "LATIN";
      }

    if ( block == ARABIC 
         || block == ARABIC_PRESENTATION_FORMS_A 
         || block == ARABIC_PRESENTATION_FORMS_B )
      {
        return "ARABIC";
      }

    if ( block == CJK_COMPATIBILITY 
         || block == CJK_COMPATIBILITY_FORMS 
         || block == CJK_COMPATIBILITY_IDEOGRAPHS 
         || block == CJK_COMPATIBILITY_IDEOGRAPHS_SUPPLEMENT 
         || block == CJK_RADICALS_SUPPLEMENT 
         || block == CJK_SYMBOLS_AND_PUNCTUATION 
         || block == CJK_UNIFIED_IDEOGRAPHS 
         || block == CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A 
         || block == CJK_UNIFIED_IDEOGRAPHS_EXTENSION_B 
         // These are Japanese-only, but we map them all into one generic CJK "script"
         || block == KATAKANA 
         || block == KATAKANA_PHONETIC_EXTENSIONS 
         || block == HIRAGANA
         )
      {
        return "CJK";
      }

    if ( block == CYRILLIC 
         || block == CYRILLIC_SUPPLEMENTARY )
      {
        return "CYRILLIC";
      }

    if ( block == GREEK || block == GREEK_EXTENDED )
      {
        return "GREEK";
      }

    return block.toString();
  }
  
  /*
   *
   */
  @SuppressWarnings("deprecation")
  @Override
  public Schema outputSchema(Schema input)
  {
    try
      {
        Schema.FieldSchema tokenFs  = new Schema.FieldSchema("token",  DataType.CHARARRAY);
        Schema.FieldSchema scriptFs = new Schema.FieldSchema("script", DataType.CHARARRAY);
        Schema tupleSchema = new Schema();
        tupleSchema.add(tokenFs);
        tupleSchema.add(scriptFs);
        
        Schema.FieldSchema tupleFs;
        tupleFs = new Schema.FieldSchema("token_script", tupleSchema, DataType.TUPLE);
        
        Schema bagSchema = new Schema(tupleFs);
        bagSchema.setTwoLevelAccessRequired(true);
        Schema.FieldSchema bagFs = new Schema.FieldSchema("bag_of_tokenTuples",bagSchema, DataType.BAG);
        
        return new Schema(bagFs); 
      }
    catch (FrontendException e)
      {
        // throwing RTE because above schema creation is not expected
        // to throw an exception and also because superclass does not
        // throw exception
        throw new RuntimeException("Unable to compute ScriptTagger schema.");
      }   
  }
  
  /* Omit this from our ScriptTaggerr so that the multi-input calls can be mapped to it.
  public List<FuncSpec> getArgToFuncMapping() throws FrontendException 
  {
    List<FuncSpec> funcList = new ArrayList<FuncSpec>();
    Schema s = new Schema();
    s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
    funcList.add(new FuncSpec(this.getClass().getName(), s));
    return funcList;
  }
  */
}

