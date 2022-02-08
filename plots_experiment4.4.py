import matplotlib.pyplot as plt; plt.rcdefaults()
import numpy as np
import os
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
formats = []
times = []
datasets = []

try:
    os.mkdir('experiment4.4_figs')
except:
    pass
import re

f = open('experiment4.4_C_compress.txt')
f2 = open('experiment4.4_Python_compress.txt')
f3 = open('experiment4.4_Python_read.txt')
f4 = open('results4.4read_C.txt')		

mylines = []

labels = ['Encoding', 'filter scan' ]
parq = []
pyadapt = []
loc = []
adaptive = []
x = np.arange(len(labels)) 
fig, ax = plt.subplots()
import fileinput

count = 0
i = 0
for line in f:
     if 'COST FUNCTION' in line:
         line = next(f)
         time = (float(re.search('rows in\s(\d+\.*\d*)\sseconds', line).groups(0)[0]))
         adaptive.append(time)
     if 'LOCAL' in line:
         line = next(f)
         time = (float(re.search('rows in\s(\d+\.*\d*)\sseconds', line).groups(0)[0]))
         loc.append(time)

for line in f2:
     if 'parquet' in line:
         time = (float(re.search('(\d+\.*\d*)', line).groups(0)[0]))
         parq.append(time)
     if 'adaptive' in line:
         time = (float(re.search('(\d+\.*\d*)', line).groups(0)[0]))
         pyadapt.append(time)



for line in f3:
     if 'parquet' in line:
         time = (float(re.search('(\d+\.*\d*)', line).groups(0)[0]))
         parq.append(time)
     if 'adaptive' in line:
         time = (float(re.search('(\d+\.*\d*)', line).groups(0)[0]))
         pyadapt.append(time)

times = []
for line in f4:
      if 'real' in line:
          time = (float(re.search('(\d+\.\d*)', line).groups(0)[0]))
          times.append(time)
          
adaptive.append(times[0])
loc.append(times[1])
      
     






#print(parq, pyadapt, loc, adaptive)




width = 0.20
rects1 = ax.bar(x-0.2, parq, width, label='parquet')
rects2 = ax.bar(x, pyadapt, width, label='parquet_adaptive')
rects3 = ax.bar(x + 0.2, loc, width, label='c++local')
rects4 = ax.bar(x + 0.4, adaptive, width, label='c++adaptive')

ax.set_ylabel('Time')
ax.set_title('Execution times')
ax.set_xticks(x, labels)
ax.legend()

ax.bar_label(rects1, padding=3)
ax.bar_label(rects2, padding=3)
ax.bar_label(rects3, padding=3)
ax.bar_label(rects4, padding=3)

fig.tight_layout()
fig.set_size_inches(20, 8)
plt.savefig('experiment4.4_figs/times.png', dpi=100)
#fig1 = plt.figure()
#ax1 = fig1.add_subplot(111)

#x1.plot( x, y1, label='mode 01' )

#fig2 = plt.figure()
#ax2 = fig2.add_subplot(111)
#ax2.plot( x, y2, label='mode 01' )




    
#plt.bar(y_pos, performance, align='center', alpha=0.5)
#plt.xticks(y_pos, objects)
#plt.ylabel('Usage')
#plt.title('Programming language usage')
#plt.savefig('lalakiplot.png')
