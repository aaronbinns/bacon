--
-- Find the URL in a Heritrix crawl log that is the farthest from a
-- seed.  Just measure the lengh of the 'path' field and find the max
-- value.  Also assume that there can be many at the maximum depth.
--
log = LOAD 'test/sample_crawl_log.txt' USING PigStorage() AS (line:chararray);

-- The Heritrix crawl log uses a variable amount of spaces to separate
-- columns.
--
-- So, we load the line using the default PigStorage() delimiter:
-- '\t', which should not match anything on the line, thus giving us
-- the entire line as one long string, which we call 'cols'.
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