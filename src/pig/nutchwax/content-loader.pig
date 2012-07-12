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
-- Simple test for ContentLoader
--

-- Specify an input path all the way to a NutchWAX segment's parse_text directory.
%default INPUT=''
%default OUTPUT=''

REGISTER build/bacon-*.jar
REGISTER lib/nutchwax-0.13.jar

pages = LOAD '$INPUT' 
        USING org.archive.bacon.nutchwax.ContentLoader() 
        AS (url:chararray,digest:chararray,content:chararray);

dump pages;
