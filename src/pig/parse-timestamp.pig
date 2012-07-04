/*
 * Tokenize the input on whitespace and ',' characters.  Then count
 * the number of instances of each token, ignoring empty tokens
 */
REGISTER build/bacon-*.jar;

DEFINE date org.archive.bacon.ParseTimestamp();

timestamps = LOAD 'test/parse_timestamp.txt' AS (line:chararray);

dates = FOREACH timestamps GENERATE date(line);

dump dates;