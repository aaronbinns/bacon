
REGISTER bacon.jar;
DEFINE rebag org.archive.bacon.ReBag();
lines  = LOAD 'test/rebag.txt' AS (line:chararray);
tuples = FOREACH lines  GENERATE STRSPLIT(line,'[ ]');
bags   = FOREACH tuples GENERATE rebag($0);
dump bags;
