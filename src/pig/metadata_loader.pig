--
-- Simple test for MetadataLoader
--

-- Specify an input path all the way to a NutchWAX segment's parse_data directory.
%default INPUT=''
%default OUTPUT=''

REGISTER bacon.jar
REGISTER lib/nutchwax-0.13.jar

pages = LOAD '$INPUT' 
        USING org.archive.bacon.nutchwax.MetadataLoader() 
        AS 
          (url:chararray,
           digest:chararray,
           title:chararray,
           length:long,
           date:chararray,
           type:chararray,
           collection:chararray);

dump pages;
