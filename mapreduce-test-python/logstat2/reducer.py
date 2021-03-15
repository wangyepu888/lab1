import sys
from operator import itemgetter
from collections import defaultdict

dict_ip_count = {}

for line in sys.stdin:
    line = line.strip()
    ip, num = line.split('\t')
    try:
        num = int(num)
        dict_ip_count[ip] = dict_ip_count.get(ip, 0) + num

    except ValueError:
        pass

sorted_dict_ip_count = sorted(dict_ip_count.items(), key=itemgetter(1), reverse=True)
t = defaultdict(list)

for ip, count in sorted_dict_ip_count:
    hr = ip[1:3]
    hr = int(hr)
    ip_a = ip[7:]
    count = int(count)
    t[hr].append([ip_a, count])
 for i in range(24):
    t3 = sorted(top[i],key=lambda x:x[1], reverse=True)[:3]
    print '%s\t%s' % (i,t3)