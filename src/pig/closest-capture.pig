/*
 * Copyright 2011 Internet Archive
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

--
-- Find the closest capture of a linked resource.
--

edges1 = LOAD 'test/closest-capture.txt' AS (src:chararray,tstamp:long,dest:chararray);
edges2 = LOAD 'test/closest-capture.txt' AS (src:chararray,tstamp:long,dest:chararray);

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
