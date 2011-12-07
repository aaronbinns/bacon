
REGISTER build/bacon-*.jar
REGISTER lib/json-20090211.jar

text = LOAD 'test/json.txt' AS (url:chararray,digest:chararray,inlinks:long,message:chararray);

text = FOREACH text GENERATE TOMAP( 'url', url, 
                                    'digest', digest, 
                                    'inlinks', inlinks, 
                                    'message', org.archive.bacon.Tokenize( message ),
                                    'well', TOTUPLE( 'x', TOTUPLE( 'y', 'z' ) ),
                                    'empty', null,
                                    'ha ha!', TOMAP( 'somelist', TOTUPLE('a','b','c','d'), 'submap', TOMAP( 'foo','bar' ) ) );

STORE text INTO '/tmp/json.txt' USING org.archive.bacon.io.JSONStorage('false');
