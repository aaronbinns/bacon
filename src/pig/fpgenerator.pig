
REGISTER build/bacon*.jar

DEFINE fp org.archive.bacon.FPGenerator;

text = LOAD 'test/fpgenerator.txt' AS (url:chararray,digest:chararray);

fingerprints = FOREACH text GENERATE fp(CONCAT(url,digest));

dump fingerprints;
