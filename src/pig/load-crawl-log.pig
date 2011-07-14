--
-- Simple Pig demonstration/example script showing how to tokenize
-- input rows that use variable-length whitespace delimiters.
--
-- Since PigStorage only allows for single character delimiters,
-- we load the entire line into a string, then tokenize that string
-- ourselves, and map the tokens into the appropriate columns.
--
log = LOAD 'test/sample_crawl_log.txt' USING PigStorage() AS (line:chararray);

log = FOREACH log GENERATE STRSPLIT(line,'\\s+') as cols;
log = FOREACH log GENERATE cols.$0 as timestamp, 
                           cols.$1 as status,
                           cols.$2 as bytes,
                           cols.$3 as url,
                           cols.$4 as path,
                           cols.$5 as via,
                           cols.$6 as type,
                           cols.$7 as thread,
                           cols.$8 as elapsed,
                           cols.$9 as digest,
                           cols.$10 as source,
                           cols.$11 as annotations ;

dump log;
