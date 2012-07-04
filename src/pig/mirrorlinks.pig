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
 * Find pairs of pages which link directly to each other using the
 * same anchor text.
 */

REGISTER build/bacon-*.jar;

/* Load the link graph in the form the same as the example table above. */
links1 = LOAD 'segments/*/parse_data' USING OutlinkLoader AS (from:chararray,to:chararray,anchor:chararray);

/* Discard self-links within a page. */
links1 = FILTER links1 BY from != to AND anchor != '';

/* Duplicate the link graph. */
links2 = FOREACH links1 GENERATE from, to, anchor;

/* Join the link graphs, producing 2-link chains. */
results = JOIN links1 BY to, links2 BY from ;

/* Keep 2-link chains that have the same endpoints, e.g. A->B,B->A, and the same anchor/link text. */
results = FILTER results BY links1::from == links2::to AND links1::anchor == links2::anchor; 

/* Since each pair will appear twice, only keep one. */
results = FILTER results BY links1::from < links2::from ;

/* Throw away redundant columns. */
results = FOREACH results GENERATE links1::from, links1::to, links1::anchor ; 

/* Only keep unique results. */
results = DISTINCT results;

DUMP results;
