import matplotlib.pyplot as plt; plt.rcdefaults()
import numpy as np
import os
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
formats = []
times = []
datasets = []

try:
    os.mkdir('experiment4.1_compression_figs')
except:
    pass
import re

f = open('results4.1compression.txt')

mylines = []

labels = ['random(θ=1.3)','sorted(θ=1.3)','random(θ=0.7)', 'sorted(θ=0.7)','random(θ=0)','sorted(θ=0)' ]
_local = []
_global = []
indirect = []
adaptive = []
x = np.arange(len(labels)) 
fig, ax = plt.subplots()
import fileinput

count = 0
i = 0
for line in fileinput.input():
	 time = (float(re.search('time:\s(\d+\.*\d*)', line).groups(0)[0]))
	 dataset = re.search('\w+\s(\D+\s\d\.*\d*)', line).groups(0)[0]
	 ds = dataset.replace('zipf','θ')
	 ds += '('+str(i)+')'
	 ds = ds.replace(' ','\n')
	 format = re.search('(\w+)', line).groups(0)[0]
	 formats.append(format)
	 if format == 'Local':
	     _local.append(time)
	 if format == 'Global':
	     _global.append(time)
	 if format == 'Adaptive':
	     adaptive.append(time)
	 if format == 'Indirect':
	     indirect.append(time)
	 i+=1
	 
	 
width = 0.20
rects1 = ax.bar(x-0.2, adaptive, width, label='adaptive')
rects2 = ax.bar(x, _global, width, label='global')
rects3 = ax.bar(x + 0.2, _local, width, label='local')
rects4 = ax.bar(x + 0.4, indirect, width, label='indirect')

ax.set_ylabel('Time')
ax.set_title('Compression Times')
ax.set_xticks(x, labels)
ax.legend()

ax.bar_label(rects1, padding=3)
ax.bar_label(rects2, padding=3)
ax.bar_label(rects3, padding=3)
ax.bar_label(rects4, padding=3)

fig.tight_layout()
fig.set_size_inches(20, 8)
plt.savefig('experiment4.1_compression_figs/times.png', dpi=100)
#fig1 = plt.figure()
#ax1 = fig1.add_subplot(111)

#x1.plot( x, y1, label='mode 01' )

#fig2 = plt.figure()
#ax2 = fig2.add_subplot(111)
#ax2.plot( x, y2, label='mode 01' )



labels = ['random(θ=1.3)','sorted(θ=1.3)','random(θ=0.7)', 'sorted(θ=0.7)','random(θ=0)','sorted(θ=0)' ]
_local = []
_global = []
indirect = []
adaptive = []
x = np.arange(len(labels)) 
fig, ax = plt.subplots()


myfiles = ["zipf13rand.diff","zipf13rand.unorderedglob","zipf13rand.localdiff", "zipf13rand.unindirect", "zipf13sorted.diff","zipf13sorted.unorderedglob","zipf13sorted.localdiff", "zipf13sorted.unindirect",
"zipf07rand.diff","zipf07rand.unorderedglob","zipf07rand.localdiff", "zipf07rand.unindirect", "zipf07sorted.diff","zipf07sorted.unorderedglob","zipf07sorted.localdiff", "zipf07sorted.unindirect",
"zipf0rand.diff","zipf0rand.unorderedglob","zipf0rand.localdiff", "zipf0rand.unindirect", "zipf0sorted.diff","zipf0sorted.unorderedglob","zipf0sorted.localdiff", "zipf0sorted.unindirect"
]

  
for _file in myfiles:
	file_stats = os.stat(_file)
	size = file_stats.st_size/(1024*1024)
	formats.append(format)
	if 'local' in _file:
	     _local.append(size)
	elif 'glob' in _file:
	     _global.append(size)
	elif 'diff' in _file:
	     adaptive.append(size)
	elif 'indirect' in _file:
	     indirect.append(size)
	i+=1
	
print(adaptive)
 
width = 0.20
rects1 = ax.bar(x-0.2, adaptive, width, label='adaptive')
rects2 = ax.bar(x, _global, width, label='global')
rects3 = ax.bar(x + 0.2, _local, width, label='local')
rects4 = ax.bar(x + 0.4, indirect, width, label='indirect')

ax.set_ylabel('Size (MB)')
ax.set_title('Compressed Sizes')
ax.set_xticks(x, labels)
ax.legend()

ax.bar_label(rects1, padding=3)
ax.bar_label(rects2, padding=3)
ax.bar_label(rects3, padding=3)
ax.bar_label(rects4, padding=3)

fig.tight_layout()
fig.set_size_inches(20, 8)
plt.savefig('experiment4.1_compression_figs/sizes.png', dpi=100)


    
#plt.bar(y_pos, performance, align='center', alpha=0.5)
#plt.xticks(y_pos, objects)
#plt.ylabel('Usage')
#plt.title('Programming language usage')
#plt.savefig('lalakiplot.png')
