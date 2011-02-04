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
 * Emit the domain of each link.
 */

REGISTER bacon.jar ; 

/* Load the link graph in the form the same as the example table above. */
links1 = LOAD 'segments/*/parse_data' USING OutlinkLoader AS (from:chararray,to:chararray,anchor:chararray);

/* Transform links to just be at the domain level */
links2 = FOREACH links GENERATE DOMAIN( from ), DOMAIN( to );

DUMP links2;
