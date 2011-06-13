--
-- Simple test for ContentLoader
--

-- Specify an input path all the way to a NutchWAX segment's parse_text directory.
%default INPUT=''
%default OUTPUT=''

REGISTER bacon.jar
REGISTER lib/nutchwax-0.13.jar

pages = LOAD '$INPUT' 
        USING org.archive.bacon.nutchwax.ContentLoader() 
        AS (url:chararray,digest:chararray,content:chararray);

dump pages;
