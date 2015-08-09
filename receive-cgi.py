#!/usr/bin/python

import cgi, datetime, fcntl, sys, time

out = open('/home/pi/acu-rite/raw.log', 'a')
fcntl.lockf(out, fcntl.LOCK_EX)

if out.tell() == 0:
    out.write('"time","rawPost"\n')
body = sys.stdin.read()

out.write('%f,"%s"\n' % (time.time(), body))
out.close()

print "Content-Type: text/plain\n\n"
print "OK"


