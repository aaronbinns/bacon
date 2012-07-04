

REGISTER build/bacon-*.jar;

DEFINE pathexpand org.archive.bacon.url.PathExpand();

data = LOAD 'test/path-expand.txt' AS (url:chararray);
data = FOREACH data GENERATE pathexpand(url) as paths:{ path:tuple(path:chararray) };

dump data;
