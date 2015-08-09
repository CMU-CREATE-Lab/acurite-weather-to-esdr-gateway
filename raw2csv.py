#!/usr/bin/python

import csv, fcntl, json, os, sys, urlparse

rawPath = '/home/pi/acu-rite/raw.log'
csvPath = '/home/pi/acu-rite/data.jsv'

# Parse acu-rite decimal, which starts with A for positive, or - for negative.
# ndigits is # of digits before the decimal point
def parseDecimal(string, ndigits):
    p = ''
    if string[0] == 'A':
        pass
    elif string[0] == '-':
        p += '-'
    else:
        raise Exception('Unparsable decimal %s' % string)

    p += string[1 : 1 + ndigits]
    p += '.'
    p += string[1 + ndigits :]
    return float(p)

# Adapted from https://github.com/kevinkey619/AcuLink-Bridge-Reader/blob/master/AcuLink_Bridge_Reader_CSharp/frmMain.cs
def parsePressure(fields):
    if not 'C1' in fields:
        return None
    c1 = int(fields['C1'][0], 16)
    c2 = int(fields['C2'][0], 16)
    c3 = int(fields['C3'][0], 16)
    c4 = int(fields['C4'][0], 16)
    c5 = int(fields['C5'][0], 16)
    c6 = int(fields['C6'][0], 16)
    c7 = int(fields['C7'][0], 16)
    a = int(fields['A'][0], 16)
    b = int(fields['B'][0], 16)
    c = int(fields['C'][0], 16)
    d = int(fields['D'][0], 16)
    pr = int(fields['PR'][0], 16)
    tr = int(fields['TR'][0], 16)
    
    d1 = pr
    d2 = tr
    if d2 >= c5:
        dut = d2 - c5 - ((d2 - c5) / 2**7) * ((d2 - c5) / 2**7) * a / 2**c
    else:
        dut = d2 - c5 - ((d2 - c5) / 2**7) * ((d2 - c5) / 2**7) * b / 2**c
        
    off = (c2 + (c4 - 1024) * dut / 2**14) * 4;

    sens = c1 + c3 * dut / 2**10

    x = sens * (d1 - 7168) / 2**14 - off

    p = x * 10 / 2**5 + c7

    t = 250 + dut * c6 / 2**16 - dut / 2**d

    return round(p / 338.6, 2)
                                
def parse(raw):
    fields = urlparse.parse_qs(raw)
    fieldNames = []
    ret = []

    fieldNames.append('windspeed')
    if 'windspeed' in fields:
        ret.append(round(parseDecimal(fields['windspeed'][0], 3) * 2.23694, 2))
    else:
        ret.append(None)
    
    fieldNames.append('winddir')
    if 'winddir' in fields:
        ret.append('57319BFDCEA80264'.find(fields['winddir'][0]) * 22.5)
    else:
        ret.append(None)
        
    fieldNames.append('temperature')
    if 'temperature' in fields:
        ret.append(round(parseDecimal(fields['temperature'][0], 3) * 1.8 + 32, 2))
    else:
        ret.append(None)

    fieldNames.append('humidity')
    if 'humidity' in fields:
        ret.append(parseDecimal(fields['humidity'][0], 3))
    else:
        ret.append(None)

    fieldNames.append('pressure')
    ret.append(parsePressure(fields))

    # TODO: figure out correct # of digits
    fieldNames.append('rainfall')
    if 'rainfall' in fields:
        ret.append(parseDecimal(fields['rainfall'][0], 3) / 2.54)
    else:
        ret.append(None)

    fieldNames.append('battery')
    if 'battery' in fields:
        ret.append(fields['battery'][0])
    else:
        ret.append(None)

    fieldNames.append('rssi')
    if 'rssi' in fields:
        ret.append(int(fields['rssi'][0]))
    else:
        ret.append(None)

    return (fieldNames, ret)

# Find last line of CSV
lastCsvTime = 0

csvIn = None
csvIsValid = False

# Find last entry of CSV file
size = 0
try:
    size = os.path.getsize(csvPath)
except:
    pass

if size > 0:
    csvIsValid = True
    csvIn = open(csvPath, 'r')
    csvIn.seek(max(0, size - 1000))
    csvIn.readline()
    while True:
        line = csvIn.readline()
        if line == '':
            break
        lastCsvTime = json.loads(line)[0]
    csvIn.close()

csvOut = open(csvPath, 'a')

if not csvIsValid:
    csvOut.write(json.dumps(['time'] + parse('')[0]) + '\n')

##################

rawIn = open(rawPath, 'r+')
fcntl.lockf(rawIn, fcntl.LOCK_EX)
first = 0
last = os.path.getsize(rawPath) - 1

# Binary search to find first entry > lastCsvTime

while first < last:
    guess = (first + last) / 2
    rawIn.seek(guess)
    rawIn.readline()
    actual = rawIn.tell()
    line = rawIn.readline()
    timestamp = 1e10
    if line == '':
        # This line is too late
        last = guess - 1
    else:
        timestamp = float(line.split(',')[0])
        if timestamp <= lastCsvTime:
            # This line is too early;  skip ahead to newline
            first = actual + len(line) - 1
        else:
            # This line might be OK; set last to previous newline
            last = guess - 1

rawIn.seek(first)
rawIn.readline()
rawReader = csv.reader(rawIn)
added = 0

while True:
    try:
        row = rawReader.next()
    except csv.Error:
        continue
    except StopIteration:
        break

    t = float(row[0])
    csvOut.write(json.dumps([t] + parse(row[1])[1]) + '\n')
    added += 1

rawIn.close()

print 'Converted %d records from %s to %s' % (added, rawPath, csvPath)


