########################################################################
#
# Generate a histogram of distribution for YCSB
#
########################################################################

import os, sys

# base unit in bytes, width of the histogram buckets
bucket_size = 1
# max number of histogram buckets
bucket_num = 500

# distribution of value sizes, height of specific histogram buckets
# e.g. [value size - 1] [ratio of requests]
bucket_dist = {
	49: 2,
	99: 1,
	199: 1
}

# generate a tab-delimited histogram input file in the following format
# ```
# BlockSize	[bucket_size]
# 0	[ratio of requests]
# 1	[ratio of requests]
# ...
# [bucket_num - 1]	[ratio of requests]
# ```

outf = open("./hist.txt", "w")

print >>outf, "BlockSize\t%d" % (bucket_size)
for i in range(500):
	value = 0
	if i in bucket_dist:
		value = bucket_dist[i]
	print >>outf, "%d\t%d" % (i, value)

outf.close()
