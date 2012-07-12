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
-- Simple demonstration of Tokenize and NGram UDFs.  Tokenizes sample
-- text on space and generates a sliding window of 3-grams.
--

REGISTER build/bacon-*.jar;

DEFINE tokenize org.archive.bacon.Tokenize();
DEFINE ngram    org.archive.bacon.NGram();

text = LOAD 'test/ngram.txt' AS (words:chararray);

ngrams = FOREACH text GENERATE ngram( tokenize( words, '[ ]' ), 3 ) as grams;

dump ngrams;
