#!/usr/bin/python

import csv, fcntl, os, sys, urlparse

os.chdir('/home/pi/acu-rite')

csvPath = 'data.jsv'

lockPath = csvPath + '.uploadlock'

try:
    fcntl.lockf(open(lockPath, 'w'), fcntl.LOCK_EX | fcntl.LOCK_NB)
except:
    print 'Unable to lock %s, aborting' % lockPath
    exit(0)

deviceInfo = {'product': 'AcuRite',
              'serialNumber': 'ArWeatherStationSouthOaklandPittsburgh',
              'lat': 40.429877,
              'lon': -79.954665}

def exec_ipynb(url):
    import json, re, urllib2
    nb = (urllib2.urlopen(url) if re.match(r'https?:', url) else open(url)).read()
    exec '\n'.join([''.join(cell['input']) for cell in json.loads(nb)['worksheets'][0]['cells'] if cell['cell_type'] == 'code']) in globals()

exec_ipynb('python-utils/esdr-library.ipynb')

esdr = Esdr('esdrAuth.json')

feed = esdr.get_or_create_feed_from_device_info(deviceInfo)

lastEsdrTime = 0
if 'channelBounds' in feed:
    lastEsdrTime = min([info['maxTimeSecs'] for info in feed['channelBounds']['channels'].values()])

print 'ESDR last time of update: %f' % lastEsdrTime

csvIn = open(csvPath, 'r+')
fcntl.lockf(csvIn, fcntl.LOCK_EX)

# Read header
header = json.loads(csvIn.readline())

# Do some binary searching
# ...

first = 0
last = os.path.getsize(csvPath) - 1

# Binary search to find first entry > lastEsdrTime

while first < last:
    guess = (first + last) / 2
    csvIn.seek(guess)
    csvIn.readline()
    actual = csvIn.tell()
    line = csvIn.readline()
    timestamp = 1e10
    if line == '':
        # This line is too late
        last = guess - 1
    else:
        timestamp = json.loads(line)[0]
        if timestamp <= lastEsdrTime:
            # This line is too early;  skip ahead to newline
            first = actual + len(line) - 1
        else:
            # This line might be OK; set last to previous newline
            last = guess - 1

csvIn.seek(first)
csvIn.readline()
added = 0

upload = {'channel_names': header[1:],
          'data': []}

while True:
    line = csvIn.readline()
    if line == '':
        break
    row = json.loads(line)
    upload['data'].append(row)

print 'Uploading %d rows' % (len(upload['data']))
esdr.upload(feed, upload)
print 'Uploaded %d rows' % (len(upload['data']))


