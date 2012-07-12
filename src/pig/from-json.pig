/*
 * Copyright 2012 Internet Archive
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
-- Simple demonstration of UDF which converts a JSON string into a Pig Map.
--      

REGISTER build/bacon-*.jar
REGISTER lib/json-20090211.jar

text = LOAD 'test/from-json.json' USING TextLoader AS (line:chararray);

text = FOREACH text GENERATE org.archive.bacon.FromJSON(line) AS m:[];

dump text;
