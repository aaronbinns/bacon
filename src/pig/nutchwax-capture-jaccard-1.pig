--
-- Read from a NutchWAX segment and output in the form to be used for
-- a Jaccard similarity comparison across multiple captures fo the
-- same URL.
--
%default INPUT=''
%default OUTPUT=''

REGISTER bacon.jar
REGISTER nutchwax-0.13.jar

DEFINE tokenize  org.archive.bacon.Tokenize();
DEFINE stringize org.archive.bacon.Stringize();

pages = LOAD '$INPUT'
        USING org.archive.bacon.nutchwax.ContentLoader()
        AS (url:chararray,digest:chararray,content:chararray);

-- We could add '\\p{N}' to the regexp to include numbers.
-- For now, we just keep letters.
pages = FOREACH pages GENERATE url, digest, tokenize(LOWER(content), '\\s+|[^\\p{L}]+') as tokens;
pages = FOREACH pages
{
        tokens = FILTER tokens BY stringize(token) != '';
        tokens = DISTINCT tokens;
        GENERATE url, digest, FLATTEN(tokens) as token, COUNT(tokens) AS num_tokens;
}

-- Store in format:
--   url    :chararray
--   digest :chararray
--   token  :chararray
--   num_tokens:long
--
-- This is the input format for nutchwax-capture-jaccard-2.pig
-- 
rmf $OUTPUT;
STORE pages INTO '$OUTPUT';
