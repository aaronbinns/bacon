/*
 * Copyright 2013 Internet Archive
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
-- Read a line of text, tokenize it on whitespace, then catenate it back together.
--      
REGISTER build/bacon-*.jar

DEFINE cat      org.archive.bacon.Catenate();
DEFINE tokenize org.archive.bacon.Tokenize();

data = LOAD 'test/catenate-2.txt' USING TextLoader() AS (line:chararray);

data = FOREACH data GENERATE tokenize(line,'\\s+') AS tokens;

DUMP data;

data = FOREACH data GENERATE cat(' ',tokens);

DUMP data;
