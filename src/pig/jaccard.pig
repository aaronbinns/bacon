%default GRAPH 'test/jaccard.txt';

REGISTER bacon.jar;
DEFINE tokenize  org.archive.bacon.Tokenize();
DEFINE stringize org.archive.bacon.Stringize();

-- Load the graph, which is just a list of (site,description) pairs.
-- The site is v1 and tokenized description is v2 for the Jaccard similarity function.
edges1 = LOAD '$GRAPH' AS (v1:chararray, v2:chararray);
edges1 = FOREACH edges1 GENERATE v1, tokenize( LOWER(v2), '\\s*[,.:]\\s*|\\s+' ) AS v2;
edges1 = FOREACH edges1 {
         v2 = DISTINCT v2;
         -- We have to stringize() the token due to type-casting weirdness in Pig.
         v2 = FILTER v2 BY stringize(token) != '';
         GENERATE v1 as v1, v2 as v2, COUNT(v2) as v1_out;
}
edges1 = FOREACH edges1 GENERATE v1, FLATTEN(v2) as v2, v1_out;

-- Load the graph again and produce the exact same set of edges.
-- Once Pig allows for self-joining a relation, we don't need this.
edges2 = LOAD '$GRAPH' AS (v1:chararray, v2:chararray);
edges2 = FOREACH edges2 GENERATE v1, tokenize( LOWER(v2), '\\s*[,.:]+\\s*|\\s+' ) AS v2;
edges2 = FOREACH edges2 {
         v2 = DISTINCT v2;
         -- We have to stringize() the token due to type-casting weirdness in Pig.
         v2 = FILTER v2 BY stringize(token) != '';
         GENERATE v1 as v1, v2 as v2, COUNT(v2) as v1_out;
}
edges2 = FOREACH edges2 GENERATE v1, FLATTEN(v2) as v2, v1_out;

-- The rest is taken from:
--   The Data Chef: Structural Similarity With Apache Pig
--   http://thedatachef.blogspot.com/2011/05/structural-similarity-with-apache-pig.html
-- With a few modifications of my own.

/* We don't need this part since we compute the aug_edges and aug_dups above.
--
-- Augment the edges with the sizes of their outgoing adjacency lists. Note that
-- if a self join was possible we would only have to do this once.
--
grouped_edges = GROUP edges BY v1;
aug_edges     = FOREACH grouped_edges GENERATE FLATTEN(edges) AS (v1, v2), COUNT(edges) AS v1_out;
 
grouped_dups  = GROUP edges_dup BY v1;
aug_dups      = FOREACH grouped_dups GENERATE FLATTEN(edges_dup) AS (v1, v2), COUNT(edges_dup) AS v1_out;
*/

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
                    edges2::v1  AS v2,
                    added_size    AS added_size
                  ;
                };
-- The above produces pairs of v1 nodes being compared, and for each
-- pair, they are represented twice: (A,B) and (B,A).  Since we only
-- need to compare them once, we keep only one pair by just filtering
-- out ones where the v1 is > v2.
intersection = FILTER intersection BY v1 < v2;

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
                 similarity = (double)intersection_size/((double)added_size-(double)intersection_size);
                 GENERATE
                   v1         AS v1,
                   v2         AS v2,
                   similarity AS similarity
                 ;
               };

similarities = ORDER similarities BY similarity;

DUMP similarities;
