--
--
REGISTER build/bacon-*.jar

-- In our Hadoop cluster, these properties are set in the global Hadoop config.
-- But, if you run this test on your local machine, you can use these two lines
-- to control the compression settings for the sequence file.
SET mapred.output.compress         'true'
SET mapred.output.compression.type 'BLOCK'

data = LOAD 'test/sequencefilestorage.txt' AS (key:chararray,value:double);

STORE data INTO '/tmp/sequencefilestorage' USING org.archive.bacon.io.SequenceFileStorage( 'org.apache.hadoop.io.Text', 
                                                                                           'org.apache.hadoop.io.DoubleWritable' );

data = FOREACH data GENERATE null, value;

STORE data INTO '/tmp/sequencefilestorage2' USING org.archive.bacon.io.SequenceFileStorage( 'org.apache.hadoop.io.Text', 
                                                                                            'org.apache.hadoop.io.DoubleWritable' );

data = FOREACH data GENERATE value;

STORE data INTO '/tmp/sequencefilestorage3' USING org.archive.bacon.io.SequenceFileStorage( 'org.apache.hadoop.io.Text', 
                                                                                            'org.apache.hadoop.io.DoubleWritable' );

data = FOREACH data GENERATE null, null;

STORE data INTO '/tmp/sequencefilestorage4' USING org.archive.bacon.io.SequenceFileStorage( 'org.apache.hadoop.io.Text', 
                                                                                            'org.apache.hadoop.io.BytesWritable' );
