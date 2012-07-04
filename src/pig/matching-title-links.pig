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

/*
 * Find links where the anchor text matches the title of the linked-to page.
 */

REGISTER build/bacon-*.jar;

/* Load the link graph in the form the same as the example table above. */
meta  = LOAD 'segments/*/parse_data' USING MetadataLoader AS (url:chararray,title:chararray,length:long,date:chararray,type:chararray,collection:chararray);
links = LOAD 'segments/*/parse_data' USING OutlinkLoader AS (from:chararray,to:chararray,anchor:chararray);

/* Eliminate empty titles and anchors */
meta  = FILTER meta  BY title  != '';
links = FILTER links BY from   != to AND anchor != '';

/* Generate domains for the from and to urls */
links = FOREACH links GENERATE from, DOMAIN( from ) as fromdomain, to, DOMAIN( to ) as todomain, anchor ;

/* Eliminate intra-domain links */
links = FILTER links BY fromdomain != todomain ;

/* Join the links to the metadata records by URL */
results = JOIN meta BY url, links BY to;

/* Now only keep those where the title matches the anchor text */
results = FILTER results BY meta::title == links::anchor;

/* Only keep the link info for the output */
results = FOREACH results GENERATE links::fromdomain, links::todomain ; 

/* Group the links by from-to pairs */
results = GROUP results BY (links::fromdomain, links::todomain);

/* Count number of links in each from-to pair */
results = FOREACH results GENERATE group, COUNT( $1 ) as count;

/* Sort 'em */
results = ORDER results BY count;

DUMP results;
