import matplotlib.pyplot as plt; plt.rcdefaults()
import numpy as np
import os
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
formats = []
times = []
datasets = []

try:
    os.mkdir('experiment4.1_read_figs')
except:
    pass
import re

f = open('results4.1read.txt')


labels = ['random(θ=1.3)','random(θ=0.7)','random(θ=0)', 'sorted(θ=1.3)','sorted(θ=0.7)','sorted(θ=0)' ]
_local = []
_global = []
indirect = []
adaptive = []
x = np.arange(len(labels)) 
fig, ax = plt.subplots()
import fileinput

count = 0
i = 0



for line in f:
	 if 'LOCAL' in line and 'full scan' in line:
	     for l in f:
	         if 'real' in l:
	             time = (float(re.search('(\d+\.\d+)', l).groups(0)[0]))
	             _local.append(time)
	             break 
	             
	 elif 'GLOBAL' in line and 'full scan' in line:
	     for l in f:
	         if 'real' in l:
	             time = (float(re.search('(\d+\.\d+)', l).groups(0)[0]))
	             _global.append(time)
	             break 
	 elif 'ADAPTIVE' in line and 'full scan' in line:
	     for l in f:
	         if 'real' in l:
	             time = (float(re.search('(\d+\.\d+)', l).groups(0)[0]))
	             adaptive .append(time)
	             break 
	 elif 'INDIRECT' in line and 'full scan' in line:
	     for l in f:
	         if 'real' in l:
	             time = (float(re.search('(\d+\.\d+)', l).groups(0)[0]))
	             indirect.append(time)
	             break 
	 i+=1


	 
width = 0.20
rects1 = ax.bar(x-0.2, adaptive, width, label='adaptive')
rects2 = ax.bar(x, _global, width, label='global')
rects3 = ax.bar(x + 0.2, _local, width, label='local')
rects4 = ax.bar(x + 0.4, indirect, width, label='indirect')

ax.set_ylabel('Time')
ax.set_title('Full scan Times')
ax.set_xticks(x, labels)
ax.legend()

ax.bar_label(rects1, padding=3)
ax.bar_label(rects2, padding=3)
ax.bar_label(rects3, padding=3)
ax.bar_label(rects4, padding=3)

fig.tight_layout()
fig.set_size_inches(20, 8)
plt.savefig('experiment4.1_read_figs/scantimes.png', dpi=100)
#fig1 = plt.figure()
#ax1 = fig1.add_subplot(111)

#x1.plot( x, y1, label='mode 01' )

#fig2 = plt.figure()
#ax2 = fig2.add_subplot(111)
#ax2.plot( x, y2, label='mode 01' )



f = open('results4.1read.txt')


labels = ['random(θ=1.3)','random(θ=0.7)','random(θ=0)', 'sorted(θ=1.3)','sorted(θ=0.7)','sorted(θ=0)' ]
_local = []
_global = []
indirect = []
adaptive = []
x = np.arange(len(labels)) 
fig, ax = plt.subplots()
import fileinput

count = 0
i = 0



for line in f:
	 if 'LOCAL' in line and 'filter' in line:
	     for l in f:
	         if 'real' in l:
	             time = (float(re.search('(\d+\.\d+)', l).groups(0)[0]))
	             _local.append(time)
	             break 
	             
	 elif 'GLOBAL' in line and 'filter' in line:
	     for l in f:
	         if 'real' in l:
	             time = (float(re.search('(\d+\.\d+)', l).groups(0)[0]))
	             _global.append(time)
	             break 
	 elif 'ADAPTIVE' in line and 'filter' in line:
	     for l in f:
	         if 'real' in l:
	             time = (float(re.search('(\d+\.\d+)', l).groups(0)[0]))
	             adaptive .append(time)
	             break 
	 elif 'INDIRECT' in line and 'filter' in line:
	     for l in f:
	         if 'real' in l:
	             time = (float(re.search('(\d+\.\d+)', l).groups(0)[0]))
	             indirect.append(time)
	             break 
	 i+=1

	 
width = 0.20
rects1 = ax.bar(x-0.2, adaptive, width, label='adaptive')
rects2 = ax.bar(x, _global, width, label='global')
rects3 = ax.bar(x + 0.2, _local, width, label='local')
rects4 = ax.bar(x + 0.4, indirect, width, label='indirect')

ax.set_ylabel('Time')
ax.set_title('Filter Times')
ax.set_xticks(x, labels)
ax.legend()

ax.bar_label(rects1, padding=3)
ax.bar_label(rects2, padding=3)
ax.bar_label(rects3, padding=3)
ax.bar_label(rects4, padding=3)

fig.tight_layout()
fig.set_size_inches(20, 8) 
plt.savefig('experiment4.1_read_figs/filtertimes.png', dpi=100)




f = open('results4.1read.txt')


labels = ['random(θ=1.3)','random(θ=0.7)','random(θ=0)', 'sorted(θ=1.3)','sorted(θ=0.7)','sorted(θ=0)' ]
_local = []
_global = []
indirect = []
adaptive = []
x = np.arange(len(labels)) 
fig, ax = plt.subplots()
import fileinput

count = 0
i = 0



for line in f:
	 if 'LOCAL' in line and 'random access' in line:
	     for l in f:
	         if 'real' in l:
	             time = (float(re.search('(\d+\.\d+)', l).groups(0)[0]))
	             _local.append(time)
	             break 
	             
	 elif 'GLOBAL' in line and 'random access' in line:
	     for l in f:
	         if 'real' in l:
	             time = (float(re.search('(\d+\.\d+)', l).groups(0)[0]))
	             _global.append(time)
	             break 
	 elif 'ADAPTIVE' in line and 'random access' in line:
	     for l in f:
	         if 'real' in l:
	             time = (float(re.search('(\d+\.\d+)', l).groups(0)[0]))
	             adaptive .append(time)
	             break 
	 elif 'INDIRECT' in line and 'random access' in line:
	     for l in f:
	         if 'real' in l:
	             time = (float(re.search('(\d+\.\d+)', l).groups(0)[0]))
	             indirect.append(time)
	             break 
	 i+=1

	 
width = 0.20
rects1 = ax.bar(x-0.2, adaptive, width, label='adaptive')
rects2 = ax.bar(x, _global, width, label='global')
rects3 = ax.bar(x + 0.2, _local, width, label='local')
rects4 = ax.bar(x + 0.4, indirect, width, label='indirect')

ax.set_ylabel('Time')
ax.set_title('Random Access Times')
ax.set_xticks(x, labels)
ax.legend()

ax.bar_label(rects1, padding=3)
ax.bar_label(rects2, padding=3)
ax.bar_label(rects3, padding=3)
ax.bar_label(rects4, padding=3)

fig.tight_layout()
fig.set_size_inches(20, 8)
plt.savefig('experiment4.1_read_figs/randomaccess.png', dpi=100)
