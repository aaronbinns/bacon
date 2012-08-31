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

DEFINE put org.archive.bacon.MapPutValue();

data = LOAD 'test/map-put-value.txt' AS (m:[]);

dump data;

data = FOREACH data GENERATE put(m,'name','Ximm');

dump data;
