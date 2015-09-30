import os
import sys

IFILE="../coding.perf.out"
#IFILE="./diff.chunk.size.out"
target = 8
sample_size = 4096

class Performance:
	def __init__(self):
		self._results = {}

	def insert(self, code, cs, k, tp, result):
		if k not in self._results:
			self._results[k] = {}
		if cs not in self._results[k]:
			self._results[k][cs] = {}
		if code not in self._results[k][cs]:
			self._results[k][cs][code] = {}
		self._results[k][cs][code][tp] = result

	def add(self, lines):
		code, cs, k, singleFail = "", "", "", False
		for l in lines:
			if l[0] == "=":
				# decode the header line
				st = l.index(' ')+1
				ed = l.index(',')
				cs = int(l[ st : ed ])
				st = ed + 1
				ed = l.index(',',ed + 1)
				k = int(l[ st : ed ])
				st = ed + 1
				ed = l.index(' ',ed + 1)
				code = l[ st : ed ]
				singleFail = False
				continue
			# process result
			if "encode" in l:
				ed = l.rfind(' ')
				st = l.rfind(' ', 0, ed - 1) + 1
				self.insert(code,cs,k,"encode",l[ st : ed ])
			elif "fail disk" in l and singleFail == False:
				singleFail = True
				ed = l.rfind(' ')
				st = l.rfind(' ', 0, ed - 1) + 1
				self.insert(code,cs,k,"fail",l[ st : ed ])
			elif "fail disk" in l:
				ed = l.rfind(' ')
				st = l.rfind(' ', 0, ed - 1) + 1
				self.insert(code,cs,k,"fail2",l[ st : ed ])


	def getResults(self):
		return self._results

	def printResults(self):
		csstr = "#"
		for code in self._results[target][sample_size]:
			csstr += " "+code
		print csstr
		for cs in self._results[target]:
			out = str(cs)
			for code in self._results[target][cs]:
				if code == "raid5":
					self._results[target][cs][code]["fail2"] = "-"
				out += " " + self._results[target][cs][code]["encode"] + " " + self._results[target][cs][code]["fail"]+ " " + self._results[target][cs][code]["fail2"]
			print out

perf = Performance()
buf = []

for l in open(IFILE):
	if l[0] == "=":
		if len(buf) > 0:
			perf.add(buf)
		buf = []
	buf.append(l)

if len(buf) > 0:
	perf.add(buf)

perf.printResults()
