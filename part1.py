#!/usr/bin/python
# --*-- coding:utf-8 --*--
import re
import sys
from operator import itemgetter
from collections import defaultdict


### mapper.py ### 
logfile = "/root/mapreduce-test/mapreduce-test-data/access.log"
f = open(logfile, 'r')
fs = f.read()

mapper_results_file = "/root/mapreduce-test/mapreduce-test-data/mapper_results.txt"
mapper_results = open(mapper_results_file, "w")

dict_ip_count = {}

pat = re.compile('(?P<ip>\d+.\d+.\d+.\d+).*?\d{4}:(?P<hour>\d{2}):\d{2}.*? ')
for line in fs.splitlines():
    match = pat.search(line)
    if match:
    	mapper_results.write('%s\t%s' % ('[' + match.group('hour') + ':00' + ']' + match.group('ip'), 1) + "\n")


mapper_results.close()

### reducer.py ###
reduc_f = open(mapper_results_file, "r")
reduc_fs = reduc_f.read()

top = defaultdict(list)

for reduc_line in reduc_fs.splitlines():
    reduc_line = reduc_line.strip().split('\t')
    hr_ip, count = line
    hr,ip = hr_ip.split(' ')
    try:
        hr = int(hr)
        count = int(count)
        top[hr].append([ip,count])

    except ValueError:
        pass



 for i in range(24):
    t3 = sorted(top[i],key=lambda x:x[1], reverse=True)[0:3]
    print '%s\t%s' % (i,t3)











