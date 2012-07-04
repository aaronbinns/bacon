/*
 * Sample Pig script to demonstrate how the NGram() Eval UDF can be used to 
 * generate pairs of a (sorted) bag.
 *  
 * Suppose we have:
 *   A 1
 *   A 5
 *   A 7
 *   A 16
 *
 * And we want to generate a list of diffs between pairs, e.g.
 *   1,5  => 4
 *   5,7  => 2
 *   7,16 => 9
 * 
 * Once we group and sort the (name,num) tuples by the num, then we
 * generate ngrams from the sorted bag to generate the sequential       
 * pairs: (1,5) (5,7) (7,16)
 * Then we simply iterate through the pairs and diff the nums.
 */

REGISTER build/bacon-*.jar;

DEFINE ngram org.archive.bacon.NGram();

nodes = LOAD 'test/pairwise.txt' AS (name:chararray,num:long);

-- Trivial grouping of the nodes into a bag.
nodes = GROUP nodes by name;

-- Order the nodes by the num, generate the 2-grams, then flatten to
-- create the pairs as tuples.
pairs = FOREACH nodes 
{
  nums = ORDER nodes BY num;
  GENERATE group as name, 
           FLATTEN( ngram( nums, 2 ) ) as (one:tuple(name:chararray,num:long), 
                                           two:tuple(name:chararray,num:long));
}

-- Iterate through the pairs and diff the nums.
diffs = FOREACH pairs
{
  GENERATE name, ( two.num - one.num );
}

dump diffs;
