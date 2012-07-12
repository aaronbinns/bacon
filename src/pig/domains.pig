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
-- Emit the domain of each URL.
--

REGISTER build/bacon-*.jar ;

DEFINE HOST   org.archive.bacon.url.Host();
DEFINE DOMAIN org.archive.bacon.url.Domain();

data = LOAD 'test/domains.txt' AS (url:chararray);

data = FOREACH data GENERATE url, HOST(url), DOMAIN(HOST(url));

dump data;
