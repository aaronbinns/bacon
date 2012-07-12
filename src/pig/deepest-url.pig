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
-- Generate a frequency distribution of num-hops-from-seed for a
-- Heritrix crawl log.  Each line in the crawl log has a 'path'
-- column, the length of which is the number of hops from the seed to
-- that URL.
--

log = LOAD 'test/sample_crawl_log.txt' USING PigStorage() AS (line:chararray);

-- The Heritrix crawl log uses a variable amount of spaces to separate
-- columns.
--
-- So, we load the line using the default PigStorage() delimiter:
-- '\t', which should not match anything on the line, thus giving us
-- the entire line as one long string.
--
-- We then split that line on whitespace (any amount) and label the
-- resulting columns appropriately.
log = FOREACH log GENERATE STRSPLIT(line,'\\s+') as line;
log = FOREACH log GENERATE (chararray)line.$0 as timestamp, 
                           (chararray)line.$1 as status,
                           (chararray)line.$2 as bytes,
                           (chararray)line.$3 as url,
                           (chararray)line.$4 as path,
                           (chararray)line.$5 as via,
                           (chararray)line.$6 as type,
                           (chararray)line.$7 as thread,
                           (chararray)line.$8 as elapsed,
                           (chararray)line.$9 as digest,
                           (chararray)line.$10 as source,
                           (chararray)line.$11 as annotations;

-- If the path is '-', the it is a seed.  We replace '-' with '' so
-- that seeds have a distance 0.
log = FOREACH log GENERATE url, (path == '-' ? '' : path) as path;

-- Calculate the distance-from-seed by the length of the path string.
log = FOREACH log GENERATE url, SIZE(path) as pathlen:long;

-- Only keep distinct lines (just in case there are dups).
log = DISTINCT log;

-- Group the log lines by the path length (i.e. distance-from-seed).
log = GROUP log BY pathlen;

-- Count the number of log lines with that path length.
log = FOREACH log GENERATE group as pathlen, COUNT(log) as num;

-- Sort 'em
log = ORDER log BY num;

-- Dump 'em
DUMP log;
