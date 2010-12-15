
import codecs
import cStringIO
import csv
import datetime
import logging
import sys

from Products.CMFCore.utils import getToolByName

log = logging.getLogger('pleiades.dump')

timePeriods = {
    'A': (-1000, -550),
    'C': (-550, -330),
    'H': (-330, -30),
    'R': (30, 300),
    'L': (300, 640)
    }
class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getencoder(encoding)

    def _encode(self, s):
        try:
            return s.encode('utf-8')
        except:
            return t
        
    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder(data)[0]
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

def location_precision(rec, catalog):
    v = catalog._catalog.getIndex('location_precision').getEntryForObject(
        rec.getRID(), default=['unlocated'])
    try:
        return v[0]
    except IndexError:
        return 'unlocated'
    
places_schema = dict(
    id=lambda x, y: x.id,
    title=lambda x, y: x.Title,
    description=lambda x, y: x.Description,
    uid=lambda x, y: x.UID,
    path=lambda x, y: x.getPath().replace('/plone', ''),
    creators=lambda x, y: ', '.join(x.listCreators),
    created=lambda x, y: x.created.HTML4(),
    modified=lambda x, y: x.modified.HTML4(),
    featureTypes=lambda x, y: ', '.join(x.getFeatureType),
    timePeriods=lambda x, y: ''.join(
        v[0].upper() for v in getattr(x, 'getTimePeriods', [])),
    locationPrecision=location_precision
    )

names_schema = dict(
    id=lambda x, y: x.id,
    title=lambda x, y: x.Title,
    description=lambda x, y: x.Description,
    uid=lambda x, y: x.UID,
    path=lambda x, y: x.getPath().replace('/plone', ''),
    creators=lambda x, y: ', '.join(x.listCreators),
    created=lambda x, y: x.created.HTML4(),
    modified=lambda x, y: x.modified.HTML4(),
    attested=lambda x, y: x.getNameAttested or x.Title,
    timePeriods=lambda x, y: ''.join(
        v[0].upper() for v in getattr(x, 'getTimePeriods', [])),
    )

def dump_catalog(context, portal_type, schema):
    catalog = getToolByName(context, 'portal_catalog')
    results = catalog(portal_type=portal_type)
    writer = UnicodeWriter(sys.stdout)
    keys = sorted(schema.keys())
    writer.writerow(keys)
    for b in results:
        writer.writerow([schema[k](b, catalog) for k in keys])

