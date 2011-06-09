
REGISTER bacon.jar

DEFINE tokenize org.archive.bacon.Tokenize();
DEFINE ngram    org.archive.bacon.NGram();

text = LOAD 'test/ngram.txt' AS (words:chararray);

ngrams = FOREACH text GENERATE ngram( tokenize( words, '[ ]' ), 3 ) as grams;
dump ngrams;

/*
grams1 = FOREACH text GENERATE ngram( tokenize( words, '[ ]' ), 1 ) as grams;
grams2 = FOREACH text GENERATE ngram( tokenize( words, '[ ]' ), 2 ) as grams;
grams3 = FOREACH text GENERATE ngram( tokenize( words, '[ ]' ), 3 ) as grams;
grams4 = FOREACH text GENERATE ngram( tokenize( words, '[ ]' ), 4 ) as grams;
grams5 = FOREACH text GENERATE ngram( tokenize( words, '[ ]' ), 5 ) as grams;
grams6 = FOREACH text GENERATE ngram( tokenize( words, '[ ]' ), 6 ) as grams;
grams7 = FOREACH text GENERATE ngram( tokenize( words, '[ ]' ), 7 ) as grams;

dump grams1;
dump grams2;
dump grams3;
dump grams4;
dump grams5;
dump grams6;
dump grams7;
*/