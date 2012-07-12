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
-- In this example, we don't create a Pig Map, so the JSON that is
-- written out does not have any property names matching the variable
-- names in this script.
-- 
-- That is, if you look at '/tmp/json-1.json' you'll see that the JSON
-- objects have properties named '$0', '$1', and so forth.
--

%default INPUT  'test/json.txt'
%default OUTPUT '/tmp/json-1.json'

REGISTER build/bacon-*.jar
REGISTER lib/json-*.jar

text = LOAD '$INPUT' AS (url:chararray,digest:chararray,message:chararray);

text = FOREACH text GENERATE
                               (1/0) AS n:int,
                      'hello world!' AS s:chararray,
                                   1 AS i:int, 
                                 -1L AS l:long, 
                                2.71 AS f:float,
                                3.14 AS d:double,
                 TOTUPLE(url,digest) AS key,
 org.archive.bacon.Tokenize(message) AS words;

dump text;

rmf $OUTPUT
STORE text INTO '$OUTPUT' USING org.archive.bacon.io.JSONStorage();

text2 = LOAD '$OUTPUT' USING org.archive.bacon.io.JSONStorage() AS (
      n:int, 
      s:chararray,
      i:int, 
      l:long, 
      f:float,
      d:double,
      key:(url:chararray,digest:chararray),
      words:{T:(word:chararray)});

dump text2;
