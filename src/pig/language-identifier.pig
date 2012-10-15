--
-- Pig sample/test script for LanguageIdentifier() UDF.
--
--
-- The input files contain text in all the languages the
-- language-detection library claims to support.  I just went to
-- Wikipedia and grabbed text from the page for Wikipedia in each
-- language.  In some cases, that page had little to no text, so I
-- just browsed around until I found some.
--
-- Example:
-- en\tWikipedia is a free, collaboratively edited, ...
-- es\tWikipedia es una enciclopedia librenota y políglota ...
-- ...
--
-- For right-to-left languages, I put each in it's own file.  The only
-- reason for doing this is because my text editor gets a little weird
-- if there is a lot of left-to-right and right-to-left mixed together
-- in one file.  From Pig's point-of-view, there's no reason why they
-- cannot all be in one file together.
--
-- The purpose of this test is to verify that largish chunks of text
-- in the target language can correctly be identified with high
-- probability.  What we expect to see in the output is something
-- like:
--
--   (en,{(en,0.9999969086362959)})
--   (es,{(es,0.9999969758693009)})
--
-- which shows the target language, followed by the probabilities
-- computed by the language-detection library.  We expect them to
-- match and also have a probability close to 1.0.
--
REGISTER build/bacon-*.jar
REGISTER lib/jsonic-*.jar

DEFINE LANG org.archive.bacon.LanguageIdentifier();

text = LOAD 'test/language_identifier*.txt' USING PigStorage() AS (lang:chararray,text:chararray);

langs = FOREACH text GENERATE lang, LANG(text) AS probs;
langs = ORDER langs BY lang;

dump langs;
