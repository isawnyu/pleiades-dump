===================
Pleiades Data Dumps
===================

Introduction
============

Data about the locations, names, and places of Pleiades is regularly read from
the site catalog and written to CSV format files. The descriptions of the
records in these files and the code that writes them is contained in this
package.

Every object in Pleiades has a short name within the context of its container.
For places, these short names are typically numeric and their container is the
object at ``/places``. Location and name records contain both their own short
names (``id``) and the short name of their parent place (``pid``). This allows
the tables to be joined.

Downloads
=========

CSV files are available from http://atlantides.org/downloads/pleiades/dumps/.

Data Description
================

Locations, names, and places are dumped to separate files. All files are UTF-8
encocded. The first row contains column names. Columns will not be removed or
reformatted in future revisions of the dump files, but new columns may be
added and columns may be reordered.

.. attention::
   The order of columns is more or less arbitrary. When reading, please use a
   dictionary based reader such as Python's csv.DictReader, or convert the
   rows immediately into key/value pairs like this (Python example)::

     # rows is a sequence of lines from the CSV file.
     header = rows[0]
     records = list( dict(zip(header, row)) for row in rows[1:] )

Common columns
--------------

The following columns shared by all three tables:

authors: string
  As in the suggested citation for Pleiades places: the primary creator(s)
  followed by all contributors.

bbox: numeric
  The geographic bounding box of the resource: minimum longitude, minimum 
  latitude, maximum longitude, maximum latitude.

created: string
  Dublin Core creation date of the resource in RFC 3339 format

creators: string
  Dublin Core creators of the resource

currentVersion: numeric
  The current version of the object.

description: string
  Dublin Core description of the location resource

id: string
  Unique identifier for a location in the context of a place

locationPrecision: string
  If 'precise', the representative point is an attested location, if 'rough'
  it is the centroid of a bounding box.

maxDate: numeric
  The maximum date (decimal CE year) of any attested time period.

minDate: numeric
  The minimum date (decimal CE year) of any attested time period.

modified: string
  Dublin Core modification date of the resource in RFC 3339 format

path: string
  When appended to "http://pleiades.stoa.org/" forms the resource's URL

reprLat: numeric
  Latitude in decimal degrees N of the equator of the object's representative 
  point.

reprLatLong: string
  Comma separated decimal latitude,longitude for a representative point. The
  individual values are also provided in reprLat and reprLong columns.

reprLong: numeric
  Longitude in decimal degrees E of Greenwich of the object's representative 
  point.

tags: string
  Comma-separated list of object tags.

title: string
  Dublin Core title of the location resource

timePeriods: string
  One or more of 'A' (1000-550 BC), 'C' (550-330 BC), 'H' (330-30 BC), 
  'R' (AD 30-300), 'L' (AD 300-640)

timePeriodsKeys: string
  Comma-separated sequence of time periods keys such as "archaic,classical".
  These keys maybe be concatenated with the string 
  "http://pleiades.stoa.org/time-periods/" to produce resolvable
  Pleiades URIs.

timePeriodsRange: string
  Comma separated numeric values a,b where a is the minimum year of any
  attested time period (see minDate) and b is the maximum year of any attested
  time period (see maxDate).

uid: string
  Framework UID of resource for project use

Location Columns
----------------

Location tables have additional columns:

geometry: string
  Geometry and coordinates in GeoJSON format

featureTypes: string
  Comma-separated list of feature types such as "settlement, temple"

pid: string
  Unique identifier for the place container within the site

Name Columns
------------

Name tables have additional columns:

avgRating: numeric
  The average of all user ratings of the significance of this name relative
  to its peers.

nameAttested: string
  Attested spelling of ancient name, not necessarily the same as the "title"

nameLanguage: string
  Short identifier for language and writing system associated with the 
  attested spelling. See 
  http://pleiades.stoa.org/vocabularies/ancient-name-languages.

nameTransliterated: string
  Transliteration of the attested name to Roman characters following the
  Classical Atlas Project scheme.

numRatings: numeric
  The number of user ratings (see also 'avgRating' above).

pid: string
  Unique identifier for the place container within the site

Place Columns
-------------

Place tables have additional columns:

connectsWith: string
  Comma-separated list of numeric pids that the place connects to. Generally,
  this has a sense of connecting up-scale: settlements to regions, rivers to
  seas, etc.

featureTypes: string
  Comma-separated list of feature types such as "settlement, temple"

hasConnectionWith: string
  Comma-separated list of numeric pids for places that are connected to this
  one.  Generally, this has a sense of connections from down-scale: regions
  have connections with settlements, seas have connections with rivers.

