Pleiades Data Dumps
===================

Locations, names, and places are written each morning to separate tables in CSV
format. The first row of each file contains column names. All files are UTF-8
encocded. The "pid" column of locations and names tables can be joined to the
"id" column of a places table.

Common columns
--------------

Columns shared by all tables:

id: string
  Unique identifier for a location in the context of a place

title: string
  Dublin Core title of the location resource

description: string
  Dublin Core description of the location resource

uid: string
  Framework UID of resource for project use

path: string
  When appended to "http://pleiades.stoa.org/" forms the resource's URL

creators: string
  Dublin Core creators of the resource

created: string
  Dublin Core creation date of the resource in RFC 3339 format

modified: string
  Dublin Core modification date of the resource in RFC 3339 format

timePeriods: string
  One or more of 'A' (1000-550 BC), 'C' (550-330 BC), 'H' (330-30 BC), 
  'R' (AD 30-300), 'L' (AD 300-640)

Location Columns
----------------

Location tables have additional columns:

pid: string
  Unique identifier for the place container within the site

geometry: string
  Geometry and coordinates in GeoJSON format

Name Columns
------------

Name tables have additional columns:

pid: string
  Unique identifier for the place container within the site

nameAttested: string
  Attested spelling of ancient name, not necessarily the same as the "title"

Place Columns
-------------

Place tables have additional columns:

featureTypes: string
  Comma-separated list of feature type IDs such as "settlement, temple". In order to access the corresponding vocabulary terms and definitions, one may dereference these IDs by first prefixing the string "https://pleiades.stoa.org/vocabularies/place-types/" and then either (1) retrieving the corresponding HTML over the web or by looking up the resulting URI strings in http://atlantides.org/downloads/pleiades/rdf/place-types.ttl

