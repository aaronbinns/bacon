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

REGISTER build/bacon-*.jar;

DEFINE truncate org.archive.bacon.Truncate();

text = LOAD 'test/truncate.txt' AS (line:chararray);

-- The following two lines can be used to verify that the Pig
-- interpreter will not allow us to call truncate() with invalid
-- arguments.  If you uncomment either of them, you should get an
-- error from Pig when the script is parsed.
--text = FOREACH text GENERATE truncate( line        ) as line;
--text = FOREACH text GENERATE truncate( line, 'foo' ) as line;

-- Test that a null or negative length is ignored.
text = FOREACH text GENERATE truncate( line, null ) as line;
text = FOREACH text GENERATE truncate( line, -1 )   as line;

dump text;

-- Test that a null line is safely passed through
empty = FOREACH text GENERATE truncate( null, 100 ) as line;

dump empty;
