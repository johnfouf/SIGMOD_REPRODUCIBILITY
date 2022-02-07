import matplotlib.pyplot as plt; plt.rcdefaults()
import numpy as np
import os
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
#objects = ('Python', 'C++', 'Java', 'Perl', 'Scala', 'Lisp')
#y_pos = np.arange(len(objects))
#performance = [10,8,6,4,2,1]
mydict = {}
mydict['tpcds'] = {}
mydict['tpcds']['formats'] = []
mydict['tpcds']['times'] = []
mydict['tpcds']['sizes'] = []
mydict['yelp'] = {}
mydict['yelp']['formats'] = []
mydict['yelp']['times'] = []
mydict['yelp']['sizes'] = []
mydict['mag_synthetic'] = {}
mydict['mag_synthetic']['formats'] = []
mydict['mag_synthetic']['times'] = []
mydict['mag_synthetic']['sizes'] = []
mydict['edgar'] = {}
mydict['edgar']['formats'] = []
mydict['edgar']['times'] = []
mydict['edgar']['sizes'] = []

try:
    os.mkdir('experiment4.2_fig7')
except:
    pass
import re

f = open('experiment4.2_compress.txt')
for line in f:
    if not line.find('csv import time'):
             csvtime = (float(re.search('(\d+\.*\d*)', line).groups(0)[0]))
             line = next(f)
             dataset = re.search('reprod_datasets\/(\w+)', line).groups(0)[0]
             size = (float(re.search('size\: (\d+\.*\d*)', line).groups(0)[0]))
             format = re.search('(using only\s*|using\s*)([A-Z]+\s*[A-Z]*)', line).groups(0)[1]
             line = next(f)
             time = (float(re.search('rows in (\d+\.*\d*)', line).groups(0)[0]))
             mydict[dataset]['formats'].append(format)
             mydict[dataset]['times'].append(time)
             mydict[dataset]['sizes'].append(size)




#fig1 = plt.figure()
#ax1 = fig1.add_subplot(111)

#x1.plot( x, y1, label='mode 01' )

#fig2 = plt.figure()
#ax2 = fig2.add_subplot(111)
#ax2.plot( x, y2, label='mode 01' )


markers = ["s","o","+","v","2","^"]

figures = ['fig1','fig2','fig3','fig4' ]
axes = [None]*4

sbplt = 111
cc = 0
for mkey in mydict.keys():
	figures[cc] = plt.figure()
	axes[cc] = figures[cc].add_subplot(sbplt)
	
	for i, c in enumerate(mydict[mkey]['sizes']):
		axes[cc].scatter(mydict[mkey]['times'][i],c, marker=markers[i])             
	#plt.scatter(mydict['edgar']['times'], mydict['edgar']['sizes'])
	#print(len(mydict['edgar']['times']))
	
	offset = mlines.Line2D([], [], color='purple', marker='2', linestyle='None',
							  markersize=5, label='offset')
	differential = mlines.Line2D([], [], color='orange', marker='o', linestyle='None',
							  markersize=5, label='differential')
	cumulative = mlines.Line2D([], [], color='green', marker='+', linestyle='None',
							  markersize=5, label='cumulative size')

	current_total = mlines.Line2D([], [], color = 'red',  marker='v', linestyle='None',
							  markersize=5, label='current total')
						  
	local = mlines.Line2D([], [], marker='s', linestyle='None',
							  markersize=5, label='local')
						  
	cost_function = mlines.Line2D([], [], color='brown', marker='^', linestyle='None',
							  markersize=5, label='cost function')
	axes[cc].legend(handles=[cost_function, local, current_total, differential, offset, cumulative])
	axes[cc].set_xlabel('time')
	axes[cc].set_ylabel('size')
	axes[cc].set_title(mkey)
	plt.savefig('experiment4.2_fig7/'+mkey+'{0}.png'.format(cc))
	cc+=1
    
    
#plt.bar(y_pos, performance, align='center', alpha=0.5)
#plt.xticks(y_pos, objects)
#plt.ylabel('Usage')
#plt.title('Programming language usage')
#plt.savefig('lalakiplot.png')
