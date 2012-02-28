%default INPUT  'test/from-json.json'
-- %default OUTPUT '/tmp/json-1.json'

REGISTER build/bacon-*.jar
REGISTER lib/json-20090211.jar

text = LOAD '$INPUT' USING TextLoader AS (line:chararray);

text = FOREACH text GENERATE org.archive.bacon.FromJSON(line) AS m:[];

dump text;
