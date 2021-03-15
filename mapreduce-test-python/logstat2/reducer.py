import sys
from operator import itemgetter
from collections import defaultdict


top = defaultdict(list)

for line in sys.stdin:
    line = line.strip().split('\t')
    hr_ip, count = line
    hr,ip = hr_ip.split(' ')
    try:
        hr = int(hr)
        count = int(count)
        top[hr].append([ip,count])

    except ValueError:
        pass



 for i in range(24):
    t3 = sorted(top[i],key=lambda x:x[1], reverse=True)[:3]
    print '%s\t%s' % (i,t3)
