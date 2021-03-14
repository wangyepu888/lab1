#!/usr/bin/python
# --*-- coding:utf-8 --*--
import re
import sys
from operator import itemgetter


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


for reduc_line in reduc_fs.splitlines():
    reduc_line = reduc_line.strip()
    ip, num = reduc_line.split('\t')
    try:
        num = int(num)

        dict_ip_count[ip] = dict_ip_count.get(ip, 0) + num

    except ValueError:
        pass


sorted_dict_ip_count = sorted(dict_ip_count.items(), key=itemgetter(1), reverse=True)  

dic={}
L=[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]

for ip, count in sorted_dict_ip_count:   
    for i in range(24):
       j='0'+str(i)
       if ip[1]==j[-2]&ip[2]==j[-1]&L[i]<4:
          dic[ip]=count
          L[i]=L[i]+1
    
for ip, count in dic:
    print '%s\t%s' % (ip, count)
    
'''
代码说明：
就是用了很暴力的方法，把reduce中24小时，每个小时取不超过三个（前三个）
'''
    
    
 '''   
 上面是下面的缩写
dic={}
L=[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,]

for ip, count in sorted_dict_ip_count:
    if ip[1]==0&ip[2]==0&L[0]<4:
        dic[ip]=count
        L[0]=L[0]+1
    if ip[1]==0&ip[2]==1&L[1]<4:
        dic[ip]=count
        L[1]=L[1]+1
    if ip[1]==0&ip[2]==2&L[2]<4:
        dic[ip]=count
        L[2]=L[2]+1
    if ip[1]==0&ip[2]==3&L[3]<4:
        dic[ip]=count
        L[3]=L[3]+1
    if ip[1]==0&ip[2]==4&L[4]<4:
        dic[ip]=count
        L[4]=L[4]+1
    if ip[1]==0&ip[2]==5&L[5]<4:
        dic[ip]=count
        L[5]=L[5]+1
    if ip[1]==0&ip[2]==6&L[6]<4:
        dic[ip]=count
        L[6]=L[6]+1
    if ip[1]==0&ip[2]==7&L[7]<4:
        dic[ip]=count
        L[7]=L[7]+1
    if ip[1]==0&ip[2]==8&L[8]<4:
        dic[ip]=count
        L[8]=L[8]+1 
    if ip[1]==0&ip[2]==9&L[9]<4:
        dic[ip]=count
        L[9]=L[9]+1
    if ip[1]==1&ip[2]==0&L[10]<4:
        dic[ip]=count
        L[10]=L[10]+1
    if ip[1]==1&ip[2]==1&L[11]<4:
        dic[ip]=count
        L[11]=L[11]+1
    if ip[1]==1&ip[2]==2&L[12]<4:
        dic[ip]=count
        L[12]=L[12]+1
    if ip[1]==1&ip[3]==3&L[13]<4:
        dic[ip]=count
        L[13]=L[13]+1
    if ip[1]==1&ip[2]==4&L[14]<4:
        dic[ip]=count
        L[14]=L[14]+1
    if ip[1]==1&ip[2]==5&L[15]<4:
        dic[ip]=count
        L[15]=L[15]+1
    if ip[1]==1&ip[2]==6&L[16]<4:
        dic[ip]=count
        L[16]=L[16]+1
    if ip[1]==1&ip[2]==7&L[17]<4:
        dic[ip]=count
        L[1]=L[7]+1
    if ip[1]==1&ip[2]==8&L[18]<4:
        dic[ip]=count
        L[18]=L[18]+1
    if ip[1]==1&ip[2]==9&L[19]<4:
        dic[ip]=count
        L[19]=L[19]+1
    if ip[1]==2&ip[2]==0&L[20]<4:
        dic[ip]=count
        L[20]=L[20]+1
    if ip[1]==2&ip[2]==1&L[21]<4:
        dic[ip]=count
        L[21]=L[21]+1
    if ip[1]==2&ip[2]==2&L[22]<4:
        dic[ip]=count
        L[22]=L[22]+1
    if ip[1]==2&ip[2]==3&L[23]<4:
        dic[ip]=count
        L[23]=L[23]+1    
    
    
   ''' 
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    