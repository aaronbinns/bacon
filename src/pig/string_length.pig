--
-- Pig sample/test script for ScriptTagger() UDF.
--
REGISTER build/bacon-*.jar;

DEFINE strlen   org.archive.bacon.StringLength();
DEFINE tokenize org.archive.bacon.Tokenize();

text = LOAD 'test/string_length.txt' AS (one:chararray,two:chararray,three:chararray);

lengths = FOREACH text GENERATE strlen(one), strlen(TOTUPLE(one,two)), strlen(tokenize(three,'[^\\p{L}]+')), strlen(TOTUPLE(one,TOTUPLE(one,two),tokenize(three,'[^\\p{L}]+')));

dump lengths;
