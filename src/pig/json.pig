%default INPUT  'test/json.txt'
%default OUTPUT '/tmp/test.json.gz'

REGISTER build/bacon-*.jar
REGISTER lib/json-20090211.jar

text = LOAD '$INPUT' AS (url:chararray,digest:chararray,inlinks:long,message:chararray);

text = FOREACH text GENERATE TOMAP( 'url', url, 
                                    'digest', digest, 
                                    'inlinks', inlinks, 
                                    'message', org.archive.bacon.Tokenize( message ),
                                    'well', TOTUPLE( 'x', TOTUPLE( 'y', 'z' ) ),
                                    'empty', null,
                                    'ha ha!', TOMAP( 'somelist', TOTUPLE('a','b','c','d'), 'submap', TOMAP( 'foo','bar' ) ) );

dump text;

rmf $OUTPUT
STORE text INTO '$OUTPUT' USING org.archive.bacon.io.JSONStorage();

text2 = LOAD '$OUTPUT' USING org.archive.bacon.io.JSONStorage() AS (m:map []);

dump text2;
