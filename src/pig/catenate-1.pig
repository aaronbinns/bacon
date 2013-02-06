/*
 * Copyright 2013 Internet Archive
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
-- 
--      
REGISTER build/bacon-*.jar

DEFINE cat org.archive.bacon.Catenate();

data = LOAD 'test/catenate-1.txt' AS (one:chararray,two:chararray,three:chararray);

data = FOREACH data GENERATE one,two,three,TOTUPLE(one,two) AS onetwo, TOBAG(TOTUPLE(one,two,three)) AS onetwothree;

-- DUMP data;
data = FOREACH data GENERATE cat(null), cat(one), cat(' ',one,two), cat(' ',onetwo), cat(' ',onetwothree), cat(' ',onetwothree,onetwo,one,two,three);

DUMP data;
