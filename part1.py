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



for line in sys.stdin:
    line = line.strip()
    ip, num = line.split('\t')
    try:
        num = int(num)
        dict_ip_count[ip] = dict_ip_count.get(ip, 0) + num

    except ValueError:
        pass
top = defaultdict(list)
sorted_dict_ip_count = sorted(dict_ip_count.items(), key=itemgetter(1), reverse=True)
for hr_ip,count in sorted_dict_ip_count:
    hr = hr_ip[1:3]
    hr=int(hr)
    ip=hr_ip[7:]
    count=int(count)
    top[hr].append([ip,count])
    




for i in range(24):
    top_3 = sorted(top[i],key=lambda x:x[1],reverse=True)[:3]
    print '%s\t%s' % (i,top_3)










