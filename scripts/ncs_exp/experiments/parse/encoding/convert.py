#!/usr/bin/python

import sys

avgs = []
mins = []
maxs = []
p95 = []
p99 = []

def print_row( arr ):
	length = len( arr )
	for i in range( length ):
		if i > 0:
			sys.stdout.write( '\t' )
		sys.stdout.write( '%s' %( arr[ i ] ) )
	sys.stdout.write( '\n' )


for line in sys.stdin:
	count = 0
	for i in line.split():
		if count % 5 == 0:
			avgs.append( i )
		elif count % 5 == 1:
			mins.append( i )
		elif count % 5 == 2:
			maxs.append( i )
		elif count % 5 == 3:
			p95.append( i )
		elif count % 5 == 4:
			p99.append( i )
		count += 1

print_row( avgs )
print_row( mins )
print_row( maxs )
print_row( p95 )
print_row( p99 )

