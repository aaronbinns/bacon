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
-- Simple example of StringLength() UDF.
--
-- Demonstrates how the total length of complex Pig objects can be
-- calculated by calling StringLength() on the root object.
--
-- For example, if the datum is a tuple of two strings, then the
-- length of that datum is the sum of the lengths of the two strings.
--
-- Or, the StringLength() of a bag is the sum of the lengths of all
-- the strings in that bag.

REGISTER build/bacon-*.jar;

DEFINE strlen   org.archive.bacon.StringLength();
DEFINE tokenize org.archive.bacon.Tokenize();

text = LOAD 'test/string_length.txt' AS (one:chararray,two:chararray,three:chararray);

lengths = FOREACH text GENERATE strlen(one), strlen(TOTUPLE(one,two)), strlen(tokenize(three,'[^\\p{L}]+')), strlen(TOTUPLE(one,TOTUPLE(one,two),tokenize(three,'[^\\p{L}]+')));

dump lengths;
