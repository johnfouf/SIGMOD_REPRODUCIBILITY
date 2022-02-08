import re
f = open('results4.3read.txt')

print ("value, format, time, I/O, omit pages/offset scan")

for line in f:
	 if 'differential' in line and 'enabled' in line:
	     value = re.search('value\s(\S+)', line).groups(0)[0]
	     l = next(f)
	     iotime = re.search('io time:\s(\d+)', l).groups(0)[0]
	     totaltime = re.search('total_time:\s(\d+)', l).groups(0)[0]
	     scan_omitted = re.search('scan omitted:\s(\d+)', l).groups(0)[0]
	     blocks_omitted = re.search('blocks omitted:\s(\d+)', l).groups(0)[0]
	     print (value+','+'diff,'+totaltime+','+iotime+','+blocks_omitted+'/'+scan_omitted)
	 if  'differential' in line and 'disabled' in line: 
	     value = re.search('value\s(\S+)', line).groups(0)[0]
	     l = next(f)
	     iotime = re.search('io time:\s(\d+)', l).groups(0)[0]
	     totaltime = re.search('total_time:\s(\d+)', l).groups(0)[0]
	     scan_omitted = re.search('scan omitted:\s(\d+)', l).groups(0)[0]
	     blocks_omitted = re.search('blocks omitted:\s(\d+)', l).groups(0)[0]
	     print (value+','+'diff/no omit,'+totaltime+','+iotime+','+blocks_omitted+'/'+scan_omitted)
	 if  'global' in line and 'enabled' in line: 
	     value = re.search('value\s(\S+)', line).groups(0)[0]
	     l = next(f)
	     iotime = re.search('io time:\s(\d+)', l).groups(0)[0]
	     totaltime = re.search('total_time:\s(\d+)', l).groups(0)[0]
	     scan_omitted = '0'
	     blocks_omitted = re.search('blocks omitted:\s(\d+)', l).groups(0)[0]
	     print (value+','+'global,'+totaltime+','+iotime+','+blocks_omitted+'/'+scan_omitted)
	 if  'global' in line and 'disabled' in line: 
	     value = re.search('value\s(\S+)', line).groups(0)[0]
	     l = next(f)
	     iotime = re.search('io time:\s(\d+)', l).groups(0)[0]
	     totaltime = re.search('total_time:\s(\d+)', l).groups(0)[0]
	     scan_omitted = '0'
	     blocks_omitted = re.search('blocks omitted:\s(\d+)', l).groups(0)[0]
	     print (value+','+'global/no omit,'+totaltime+','+iotime+','+blocks_omitted+'/'+scan_omitted)
	 if 'local' in line and 'enabled' in line: 
	     value = re.search('value\s(\S+)', line).groups(0)[0]
	     l = next(f)
	     iotime = re.search('io time:\s(\d+)', l).groups(0)[0]
	     totaltime = re.search('total_time:\s(\d+)', l).groups(0)[0]
	     scan_omitted = re.search('scan omitted:\s(\d+)', l).groups(0)[0]
	     blocks_omitted = re.search('blocks omitted:\s(\d+)', l).groups(0)[0]
	     print (value+','+'local,'+totaltime+','+iotime+','+blocks_omitted+'/'+scan_omitted)
	 if  'local' in line and 'disabled' in line: 
	     value = re.search('value\s(\S+)', line).groups(0)[0]
	     l = next(f)
	     iotime = re.search('io time:\s(\d+)', l).groups(0)[0]
	     totaltime = re.search('total_time:\s(\d+)', l).groups(0)[0]
	     scan_omitted = re.search('scan omitted:\s(\d+)', l).groups(0)[0]
	     blocks_omitted = re.search('blocks omitted:\s(\d+)', l).groups(0)[0]
	     print (value+','+'local/no omit,'+totaltime+','+iotime+','+blocks_omitted+'/'+scan_omitted)
	