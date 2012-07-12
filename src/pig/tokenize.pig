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
-- Simple example of Tokenize() UDF.
--
-- Tokenize the input on whitespace and ',' characters.  Then count
-- the number of instances of each token, ignoring empty tokens
--

REGISTER build/bacon-*.jar;

DEFINE tokenize org.archive.bacon.Tokenize();

lines     = LOAD 'test/tokenize.txt' AS (line:chararray);
tokenBags = FOREACH lines GENERATE tokenize(line,'\\s+|[,]') as tbag;
tokens    = FOREACH tokenBags GENERATE FLATTEN( tbag ) AS token;
tokens    = FILTER tokens BY token != '';
tgroups   = GROUP tokens BY token;
tcounts   = FOREACH tgroups GENERATE group as token, COUNT(tokens) as count;

dump tcounts;
