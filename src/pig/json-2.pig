%default INPUT  'test/json.txt'
%default OUTPUT '/tmp/json-2.json'

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
                 TOTUPLE(url,digest) AS key:(url:chararray,digest:chararray),
 org.archive.bacon.Tokenize(message) AS words:{T:(word:chararray)};

dump text;

text = FOREACH text GENERATE TOMAP( 'n', n,
                                    's', s,
                                    'i', i,
                                    'l', l,
                                    'f', f,
                                    'd', d,
                                  'key', key,
                                'words', words );

rmf $OUTPUT
STORE text INTO '$OUTPUT' USING org.archive.bacon.io.JSONStorage();

text2 = LOAD '$OUTPUT' USING org.archive.bacon.io.JSONStorage() AS (m:[]);

text2 = FOREACH text2 GENERATE m#'n' AS n:int,
                               m#'s' AS s:chararray,
                               m#'i' AS i:int,
                               m#'l' AS l:long,
                               m#'f' AS f:float,
                               m#'d' AS d:double,
                             m#'key' AS key:(url:chararray,digest:chararray),
                           m#'words' AS words:{T:(word:chararray)};

dump text2;
