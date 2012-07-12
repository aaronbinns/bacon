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
-- Emit 64-bit fingerprint of strings containing URL+digest
--      

REGISTER build/bacon-*.jar

DEFINE fp org.archive.bacon.FPGenerator;

text = LOAD 'test/fpgenerator.txt' AS (url:chararray,digest:chararray);

fingerprints = FOREACH text GENERATE fp(CONCAT(url,digest));

dump fingerprints;
