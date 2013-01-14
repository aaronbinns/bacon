--
-- Pig sample/test script which uses a combination of ScriptTagger()
-- and LanguageIdentifier() UDFs to detect multiple languages
-- appearing in the same document.
--
-- The basic idea is by first segmenting the text by script (Latin,
-- Arabic, CJK, etc.)  we can detect multiple languages in a single
-- document if they use different scripts.  For example, if a page has
-- a mix of English and Japanese, by segmenting the text into 'LATIN'
-- and 'CJK' buckets first, we have a better chance of the
-- LanguageIdentifier() function identifying each language, rather
-- than trying to find *one* single language for the entire text.
-- 
-- This approach seems to work well for documents that do contain
-- multiple languages which differ by script.  However, it does not
-- help us detect multiple languages which all use the same script.
-- Again, it works well for a document with both English and Japanese,
-- but not for a document with, say, both English and Spanish, since
-- those languages both use the Latin script.
--

REGISTER build/bacon-*.jar
REGISTER lib/jsonic-*.jar

DEFINE script org.archive.bacon.ScriptTagger();
DEFINE langid org.archive.bacon.LanguageIdentifier();

-- The input is a file where each line is treated as a separate document.
documents = LOAD 'test/script-language-identifier.txt' AS (line:chararray);

-- Segment the document by script.  The ScriptTagger() UDF requires a
-- bag as input, so we wrap the single 'line' value in a bag.
documents = FOREACH documents GENERATE script(TOBAG(line)) AS segments:{t:tuple(segment:chararray,script:chararray)};

-- For each document, run the LanguageIdentifier() UDF on each script segment.
languages = FOREACH documents
            {
              segment_language = FOREACH segments GENERATE segment, script, langid(segment);
              GENERATE segment_language;
            };

-- Output to the console.
dump languages;
