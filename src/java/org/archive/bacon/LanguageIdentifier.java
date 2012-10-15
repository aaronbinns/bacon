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
import java.net.*;
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

import com.cybozu.labs.langdetect.*;

/**
 * Simple Pig EvalFunc wrapper around language detection code by 
 * Nakatani Shuyo (https://code.google.com/p/language-detection/).
 *
 * The source of that project is integrated into the bacon project
 * because the original code, as written, relied on the static factory
 * pattern, which required one and only one instance of the
 * DetectorFactory to be initialized.  Furthermore, it would complain
 * if you even tried to initialize more than one DetectorFactory.
 *
 * These requirements baked into the code made it vary difficult to
 * use from this Pig eval function, since an eval function is
 * instantiated for each use in a Pig script.
 *
 * So, I copied the source code from code.google.com and integrated it
 * into this project, changing the DetectorFactory to be a plain old
 * Java object that can be instantiated as many times as needed.
 */
public class LanguageIdentifier extends EvalFunc<DataBag> 
{
  TupleFactory mTupleFactory = TupleFactory.getInstance();
  BagFactory   mBagFactory   = BagFactory.getInstance();

  DetectorFactory detectorFactory;

  /** 
   * Load the language profiles into the DetectorFactory.
   */
  public LanguageIdentifier( )
    throws IOException
  {
    BufferedReader reader = new BufferedReader( new InputStreamReader( LanguageIdentifier.class.getClassLoader().getResourceAsStream( "langdetect-profiles.json" ), "utf-8" ) );

    List<String> profiles = new LinkedList<String>();
    String profile;
    while ( (profile = reader.readLine() ) != null )
      {
        profiles.add( profile );
      }

    try
      {
        this.detectorFactory = new DetectorFactory( );

        // The language-detection library samples randomly chosen
        // n-grams from the input text.  To ensure that we get the
        // same output for the same input, we use a fixed seed for the
        // random-number generator.
        this.detectorFactory.setSeed(19711113);
        this.detectorFactory.loadProfile( profiles );
      }
    catch ( LangDetectException lde )
      {
        throw new IOException( lde );
      }
  }
  

  /**
   * Compute language probabilities for the given input text.
   */
  @Override
  public DataBag exec( Tuple input ) throws IOException
  {
    try
      {
        if ( input == null || input.size() < 1 ) return null;
        
        String text = (String) input.get(0);
        
        if ( text == null ) return null;
        
        DataBag output = mBagFactory.newDefaultBag();

        // Use smoothing factor suggested by project test/demo code.
        Detector detector = detectorFactory.create(0.5);
        
        detector.append(text);
        
        for ( Language lang : detector.getProbabilities() )
          {
            Tuple t = mTupleFactory.newTuple( 2 );
            t.set( 0, lang.lang );
            t.set( 1, lang.prob );
            
            output.add( t );
          }
        
        return output;
      }
    catch ( LangDetectException lde )
      {
        throw new IOException( lde );
      }
  }


}
