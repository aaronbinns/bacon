--
-- Calculate the Jaccard similarity of all the combinations of
-- captures for each URL.  Input data must be in the specified format.
--
-- See the 'capture-jaccard-prep-*.pig' scripts which produce that
-- format from various sources.
--
%default INPUT=''
%default OUTPUT=''

pages1 = LOAD '$INPUT' AS (url:chararray,
                           digest:chararray,
                           token:chararray,
                           num_tokens:long );
pages2 = LOAD '$INPUT' AS (url:chararray,
                           digest:chararray,
                           token:chararray,
                           num_tokens:long );

edges1 = FOREACH pages1 GENERATE TOTUPLE(url,digest) as v1, TOTUPLE(url,token) as v2, num_tokens as v1_out;
edges2 = FOREACH pages2 GENERATE TOTUPLE(url,digest) as v1, TOTUPLE(url,token) as v2, num_tokens as v1_out;

-- The rest is taken from:
--   The Data Chef: Structural Similarity With Apache Pig
--   http://thedatachef.blogspot.com/2011/05/structural-similarity-with-apache-pig.html
-- With a few modifications of my own.

--
-- Compute the sizes of the intersections of outgoing adjacency lists
--
edges_joined  = JOIN edges1 BY v2, edges2 BY v2;

-- Remove nodes joined to themsevles.
edges_joined  = FILTER edges_joined BY edges1::v1 != edges2::v1;

intersection  = FOREACH edges_joined {
                  --
                  -- results in:
                  -- (X, Y, |X| + |Y|)
                  -- 
                  added_size = edges1::v1_out + edges2::v1_out;
                  GENERATE
                    edges1::v1 AS v1,
                    edges2::v1 AS v2,
                    added_size AS added_size
                  ;
                };

-- The above produces pairs of v1 nodes being compared, and for each
-- pair, they are represented twice: (A,B) and (B,A).  Since we only
-- need to compare them once, we keep only one pair by just filtering
-- out ones where the v1 is > v2.

-- NOTE: We have to compare the digest fields since the url field will be the same.
intersection = FILTER intersection BY v1.digest < v2.digest;

intersect_grp   = GROUP intersection BY (v1, v2);
intersect_sizes = FOREACH intersect_grp {
                    --
                    -- results in:
                    -- (X, Y, |X /\ Y|, |X| + |Y|)
                    --
                    intersection_size = (double)COUNT(intersection);
                    GENERATE
                      FLATTEN(group)               AS (v1, v2),
                      intersection_size            AS intersection_size,
                      MAX(intersection.added_size) AS added_size -- hack, we only need this one time
                    ;
                  };

similarities = FOREACH intersect_sizes {
                 --
                 -- results in:
                 -- (X, Y, |X /\ Y|/|X U Y|)
                 --
                 similarity = ((double)intersection_size)/((double)added_size-(double)intersection_size);
                 GENERATE
                   v1         AS v1,
                   v2         AS v2,
                   similarity AS similarity
                 ;
               };

similarities = ORDER similarities BY similarity;

rmf $OUTPUT;
STORE similarities INTO '$OUTPUT';

/*
*/
