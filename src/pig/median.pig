REGISTER 'build/bacon-*.jar'

data = LOAD 'test/median.txt' AS (value:long);

data = GROUP data ALL;
data = FOREACH data GENERATE org.archive.bacon.Median( data ) AS (t:(v:long));
data = FOREACH data GENERATE t.v as v2:long;

dump data;
