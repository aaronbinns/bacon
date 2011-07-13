--
-- Demonstration Pig script to identify the various Unicode scripts on
-- a page, and count the percentage of the page in each script.
--

REGISTER bacon.jar

DEFINE tokenize   org.archive.bacon.Tokenize();
DEFINE script_tag org.archive.bacon.ScriptTagger();

-- Load the simulated pages, with a URL as a unique key.
pages = LOAD 'test/page_scripts.txt' AS (url:chararray,page:chararray);

-- Tokenize the page, this way we can count the number of words on the page, 
-- number of tokens (words) in each script, yielding the percentage.
pages = FOREACH pages GENERATE url, tokenize(LOWER(page),'\\s+|[^\\p{L}]+') as tokens;

-- Generate the (token,script) tags.
pages = FOREACH pages GENERATE url, script_tag(tokens) AS tags;

-- Keep just the script values, we no longer need the actual tokens.
pages = FOREACH pages GENERATE url, tags.script AS scripts;

-- Count the number of tags on the page, and flatten out the scripts bag;
pages = FOREACH pages GENERATE url, SIZE(scripts) as length:long, FLATTEN(scripts) as script:chararray;

-- Now, group the pages so we can count the number of each script value.
pages = GROUP pages BY (url,length,script);

-- Count the number of tags for each script.
pages = FOREACH pages 
{
        GENERATE group.url, group.length, group.script, COUNT(pages) as count;
}

-- Dump it out to the screen.
DUMP pages;
