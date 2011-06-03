/*
 * Tokenize the input on whitespace and ',' characters.  Then count
 * the number of instances of each token, ignoring empty tokens
 */
REGISTER bacon.jar;
DEFINE tokenize org.archive.bacon.Tokenize();

lines     = LOAD 'test/tokenize.txt' AS (line:chararray);
tokenBags = FOREACH lines GENERATE tokenize(line,'\\s+|[,]') as tbag;
tokens    = FOREACH tokenBags GENERATE FLATTEN( tbag ) AS token;
tokens    = FILTER tokens BY token != '';
tgroups   = GROUP tokens BY token;
tcounts   = FOREACH tgroups GENERATE group as token, COUNT(tokens) as count;

dump tcounts;
