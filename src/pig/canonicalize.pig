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
 * Emit the canonicalized URL for each URL.
 */

REGISTER build/bacon-*.jar;
REGISTER lib/wayback-core-*.jar;

DEFINE canonicalize org.archive.bacon.url.Canonicalize();

/* Load the link graph in the form the same as the example table above. */
cdx = LOAD 'test/canonicalize.txt' AS (fullurl:chararray);

/* Canonicalize the full URLs */
curls = FOREACH cdx GENERATE canonicalize( fullurl ) as curl, fullurl;

/* Get use the unique curl,fullurl pairs */
curls = DISTINCT curls;

/* Group by the canonicalized URL */
curls = GROUP curls BY curl;

/* If we only want to see the curls that have more than one distinct original url, filter where
 * the number of tuples in the group is > 2 
 */
-- curls = FILTER curls BY COUNT(curls) > 2;

DUMP curls;
