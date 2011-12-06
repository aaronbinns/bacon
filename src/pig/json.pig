
REGISTER build/bacon-*.jar
REGISTER lib/json-20090211.jar

text = LOAD 'test/json.txt' AS (url:chararray,digest:chararray,inlinks:long);

text = FOREACH text GENERATE TOMAP( 'url', url, 'digest', digest, 'inlinks', inlinks, 'submap', TOMAP( 'foo','bar' ) );

STORE text INTO '/tmp/json.txt' USING org.archive.bacon.io.JSONStorage();
