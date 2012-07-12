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

--
-- Simple demonstration of using JSONStorage to read/write data in JSON format.
--
-- In contrast to 'json-1.pig', in this example we package the data
-- into a Pig Map before serializing out as JSON.  Subsequently, we
-- unpack the Map when we read it back in.
--
-- Compare '/tmp/json-2.json' with '/tmp/json-1.json' from the first
-- example.  This one has the variable names, such as 'n', 's', and so
-- forth.
--

%default INPUT  'test/json.txt'
%default OUTPUT '/tmp/json-2.json'

REGISTER build/bacon-*.jar
REGISTER lib/json-*

text = LOAD '$INPUT' AS (url:chararray,digest:chararray,message:chararray);

text = FOREACH text GENERATE
                               (1/0) AS n:int,
                      'hello world!' AS s:chararray,
                                   1 AS i:int, 
                                 -1L AS l:long, 
                                2.71 AS f:float,
                                3.14 AS d:double,
                 TOTUPLE(url,digest) AS key:(url:chararray,digest:chararray),
 org.archive.bacon.Tokenize(message) AS words:{T:(word:chararray)};

dump text;

text = FOREACH text GENERATE TOMAP( 'n', n,
                                    's', s,
                                    'i', i,
                                    'l', l,
                                    'f', f,
                                    'd', d,
                                  'key', key,
                                'words', words );

rmf $OUTPUT
STORE text INTO '$OUTPUT' USING org.archive.bacon.io.JSONStorage();

text2 = LOAD '$OUTPUT' USING org.archive.bacon.io.JSONStorage() AS (m:[]);

text2 = FOREACH text2 GENERATE m#'n' AS n:int,
                               m#'s' AS s:chararray,
                               m#'i' AS i:int,
                               m#'l' AS l:long,
                               m#'f' AS f:float,
                               m#'d' AS d:double,
                             m#'key' AS key:(url:chararray,digest:chararray),
                           m#'words' AS words:{T:(word:chararray)};

dump text2;
