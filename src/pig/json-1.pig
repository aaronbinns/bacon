%default INPUT  'test/json.txt'
%default OUTPUT '/tmp/json-1.json'

REGISTER build/bacon-*.jar
REGISTER lib/json-20090211.jar

text = LOAD '$INPUT' AS (url:chararray,digest:chararray,message:chararray);

text = FOREACH text GENERATE
                               (1/0) AS n:int,
                      'hello world!' AS s:chararray,
                                   1 AS i:int, 
                                 -1L AS l:long, 
                                2.71 AS f:float,
                                3.14 AS d:double,
                 TOTUPLE(url,digest) AS key,
 org.archive.bacon.Tokenize(message) AS words;

dump text;

rmf $OUTPUT
STORE text INTO '$OUTPUT' USING org.archive.bacon.io.JSONStorage();

text2 = LOAD '$OUTPUT' USING org.archive.bacon.io.JSONStorage() AS (
      n:int, 
      s:chararray,
      i:int, 
      l:long, 
      f:float,
      d:double,
      key:(url:chararray,digest:chararray),
      words:{T:(word:chararray)});

dump text2;
