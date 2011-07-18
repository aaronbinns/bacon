--
-- Pig sample/test script for ScriptTagger() UDF.
--
REGISTER bacon.jar

DEFINE script_tag org.archive.bacon.ScriptTagger();
DEFINE tokenize   org.archive.bacon.Tokenize();

text = LOAD 'test/script_tagger.txt' AS (words:chararray);

-- Approach 1: Tokenize the string before passing to ScriptTagger.
--             If there are no mutli-script tokens, then the number of
--             output tokens will be the same as the input.
--             Multi-script tokens will be split into a new token for
--             each script.
tokens = FOREACH text GENERATE FLATTEN(script_tag(tokenize(words,'[^\\p{L}]+'))) as (token:chararray,script:chararray);

scripts = GROUP tokens BY script;
scripts = FOREACH scripts 
{
        tokens = DISTINCT tokens;
        GENERATE group as script:chararray, tokens.(token);
}

DUMP scripts;

-- Approach 2: Pass in entire string and get back only as many tokens
--             as there are scripts.  Any non-letter characters, such
--             as punctuation, digits, etc. are passed through
--             untouched and are ignored for script identification.
--             Thus, a string w/o any characters will yield an empty
--             result.
--             NOTE: Since ScriptTagger() requires a bag as input,
--             we use TOBAG() to wrap the string in a bag.
tokens = FOREACH text GENERATE FLATTEN(script_tag(TOBAG(words))) as (token:chararray,script:chararray);

scripts = GROUP tokens BY script;
scripts = FOREACH scripts 
{
        tokens = DISTINCT tokens;
        GENERATE group as script:chararray, tokens.(token);
}

DUMP scripts;
