#!/usr/bin/python
# --*-- coding:utf-8 --*--
import re
import sys
from operator import itemgetter
from heapq import nlargest


### mapper.py ### 
logfile = "/root/mapreduce-test/mapreduce-test-data/access.log"
f = open(logfile, 'r')
fs = f.read()

mapper_results_file = "/root/mapreduce-test/mapreduce-test-data/mapper_results.txt"
mapper_results = open(mapper_results_file, "w")

dict_ip_count = {}

start_time_input = raw_input("Start Time [ex. 00]: ")
stop_time_input = raw_input("Stop Time [ex. 01]: ")

pat = re.compile('(?P<ip>\d+.\d+.\d+.\d+).*?\d{4}:(?P<hour>\d{2}):\d{2}.*? ')
for line in fs.splitlines():
    match = pat.search(line)
    if match:
    	#print '%s\t%s' % ('[' + match.group('hour') + ':00' + ']' + match.group('ip'), 1)
    	if start_time_input <= match.group('hour') <= stop_time_input:
        	mapper_results.write('%s\t%s' % ('[' + match.group('hour') + ':00' + ']' + match.group('ip'), 1) + "\n")


mapper_results.close()

reduc_f = open(mapper_results_file, "r")
reduc_fs = reduc_f.read()


for reduc_line in reduc_fs.splitlines():
    reduc_line = reduc_line.strip()
    ip, num = reduc_line.split('\t')
    try:
        num = int(num)

        dict_ip_count[ip] = dict_ip_count.get(ip, 0) + num

    except ValueError:
        pass


sorted_dict_ip_count = sorted(dict_ip_count.items(), key=itemgetter(1), reverse=True)
ThreeHighest = nlargest(3, sorted_dict_ip_count, key = sorted_dict_ip_count.get) 
for ip, count in ThreeHighest:
    print '%s\t%s' % (ip, count)