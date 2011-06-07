
REGISTER bacon.jar

text = LOAD 'test/ngram.txt' AS (words:chararray);

grams0 = FOREACH text GENERATE org.archive.bacon.NGram( words, '[ ]', 0 ) as grams;
grams1 = FOREACH text GENERATE org.archive.bacon.NGram( words, '[ ]', 1 ) as grams;
grams2 = FOREACH text GENERATE org.archive.bacon.NGram( words, '[ ]', 2 ) as grams;
grams3 = FOREACH text GENERATE org.archive.bacon.NGram( words, '[ ]', 3 ) as grams;
grams4 = FOREACH text GENERATE org.archive.bacon.NGram( words, '[ ]', 4 ) as grams;
grams5 = FOREACH text GENERATE org.archive.bacon.NGram( words, '[ ]', 5 ) as grams;
grams6 = FOREACH text GENERATE org.archive.bacon.NGram( words, '[ ]', 6 ) as grams;
grams7 = FOREACH text GENERATE org.archive.bacon.NGram( words, '[ ]', 7 ) as grams;

dump grams0;
dump grams1;
dump grams2;
dump grams3;
dump grams4;
dump grams5;
dump grams6;
dump grams7;
