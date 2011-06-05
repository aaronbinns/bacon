--
-- Find the closest capture of a link.
-- 
%default INPUT  'test/closest-capture.txt'
%default OUTPUT ''

edges1 = LOAD '$INPUT' AS (src:chararray,tstamp:long,dest:chararray);
edges2 = LOAD '$INPUT' AS (src:chararray,tstamp:long,dest:chararray);

edges = JOIN edges1 BY dest, edges2 BY src;

edges = FOREACH edges GENERATE 
                       edges1::src    AS src,
                       edges1::tstamp AS tstamp,
                       edges1::dest   AS dest,
                       (edges2::tstamp - edges1::tstamp) AS difftime;

edges = GROUP edges BY (src,tstamp,dest);

edges = FOREACH edges
{
        pos_diffs = FILTER edges BY difftime >= 0;
        neg_diffs = FILTER edges BY difftime < 0;
        GENERATE FLATTEN(group), MIN(pos_diffs.difftime), MAX(neg_diffs.difftime);
}

dump edges;
