--
-- NutchWAX test which loads NutchWAX segments and associated cdx
-- files, recontructs the full document, with all the metadata,
-- content and all capture dates.
--
-- Weird records are filtered out and put into a separate relation.
-- Similarly, records that are only revisit dates are put into their
-- own relation as well.
--

-- Specify an input path all the way to a NutchWAX segment's parse_data directory.
%default SEGMENT='segment'
%default CDX='cdx'
%default PAGES='pages'
%default WEIRDNESS='weirdness'
%default ONLY_DATES='only_dates'

REGISTER bacon.jar
REGISTER lib/nutchwax-0.13.jar

meta  = LOAD '$SEGMENT' 
        USING org.archive.bacon.nutchwax.MetadataLoader( )
        AS ( url:chararray,
             digest:chararray,
             title:chararray,
             length:long,
             date:chararray,
             type:chararray,
             collection:chararray,
             boiled:chararray,
             links:{ link:tuple(url:chararray,anchor:chararray) } );

content = LOAD '$SEGMENT'
          USING org.archive.bacon.nutchwax.ContentLoader( )
          AS ( url:chararray,
               digest:chararray,
               content:chararray );

cdx = LOAD '$CDX' AS (url:chararray,date:chararray,fullurl:chararray,type:chararray,code:chararray,digest:chararray,x:chararray,offeset:chararray,file:chararray);
cdx = FOREACH cdx GENERATE url, date, fullurl, type, code, CONCAT('sha1:',digest) as digest, x, offeset, file;

/* Build list of all (url,digest,date), merging dates from the meta and the cdx. */
dates_meta = FOREACH meta GENERATE url, digest, date;
dates_cdx  = FOREACH cdx  GENERATE url, digest, date;
dates_all  = UNION dates_meta, dates_cdx;
dates_all  = DISTINCT dates_all;

/* Take the date out of the meta. */
meta = FOREACH meta GENERATE url, digest, title, length, type, collection, boiled, links;

/* 
 * If a page was captured multiple times with the same hash, then
 * there will be multiple instances of it, so we DISTINCT the set.
 */
meta    = DISTINCT meta;
content = DISTINCT content;

/* Combine the dates, meta and content; connected by the url+digest */
pages = GROUP dates_all BY (url,digest), meta BY (url,digest), content BY (url,digest);

/* Guard against weirdness. */
SPLIT pages INTO weirdness IF COUNT(meta) != COUNT(content) or COUNT(meta) > 1 or COUNT(content) > 1, only_dates IF COUNT(meta) == 0, pages IF COUNT(meta) == 1;

pages = FOREACH pages 
{
  m = LIMIT meta 1;
  c = LIMIT content 1;
  GENERATE FLATTEN(group), 
           FLATTEN(m.title), 
           FLATTEN(m.length),
           dates_all.date, 
           FLATTEN(m.type),
           FLATTEN(m.collection),
           FLATTEN(m.boiled),
           FLATTEN(m.links),
           FLATTEN(c.content);
}


rmf $PAGES;
STORE pages INTO '$PAGES';

rmf $WEIRDNESS;
STORE weirdness INTO '$WEIRDNESS';

rmf $ONLY_DATES;
STORE only_dates INTO '$ONLY_DATES';
