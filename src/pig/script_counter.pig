--
-- Sample Pig script which demonstrates how to count the number characters
-- in different Unicode scripts, then total them across pages and scripts.
--
REGISTER bacon.jar;

-- Default tokenizing delimiter.  Note the double-escaping of the \ character.
-- Delmit/tokenize on all non-alphabetic characters.
%default DELIM '[\\\\P{L}]+';

DEFINE tokenize      org.archive.bacon.Tokenize();
DEFINE strlen        org.archive.bacon.StringLength();
DEFINE script_tag    org.archive.bacon.ScriptTagger();

pages = LOAD 'test/script_counter.txt' AS (url:chararray,digest:chararray,words:chararray);

pages = DISTINCT pages;
pages = FOREACH  pages GENERATE TOTUPLE(url,digest) AS id, tokenize( words, '$DELIM' ) as tokens;

-- Tag the tokens with their Unicode script, splitting multi-script
-- tokens into more tokens as needed.
pages = FOREACH pages GENERATE id, script_tag(tokens) AS tags;

-- Calculate the length of all the tokens on each page.
pages = FOREACH pages GENERATE id, strlen(tags.token) AS pagelen, tags;

-- Flatten out the tags and measure the size of each token.
pages = FOREACH pages GENERATE id, pagelen, FLATTEN(tags) as (token:chararray,script:chararray);
pages = FOREACH pages GENERATE id, pagelen, strlen(token) as tokenlen:long, script;

-- Group the pages by script.
pages = GROUP pages BY (id,pagelen,script);

pages = FOREACH pages GENERATE group.id      as id,
                               group.pagelen as pagelen,
                               group.script  as script,
                               (long)SUM(pages.tokenlen) as scriptlen:long;

-- We dump the pages here and get output of the following:
--
--  ((http://example.com/2,sha1:23456),304,LATIN,189)
--  ((http://example.com/2,sha1:23456),304,ARABIC,115)
--
-- Where the 304 is the page length in characters and 189 is the # of
-- character in LATIN.
DUMP pages;


-- Group by script, and total the number of pages in that script,
--  the total lengths of all the pages the script appears on,
--  and the total length of all the tokens in that script.
script_counts = GROUP pages BY script;
script_counts = FOREACH script_counts GENERATE group as script, SIZE(pages) as numpages, SUM(pages.scriptlen) as scriptlen, SUM(pages.pagelen) as totallen ;

DUMP script_counts;
