2. INSTRUCTIONS TO TEST WITH OTHER DATASETS AND VALIDATE:

arcadecpp/runner executable compresses csv files with C++
    It has a terminal and receives queries in the following format:
    ```
    C infile.csv outfile.diff 0 1000 0,1 CF DEFAULT
    ``` 
	which means: Compress rows from 0 to 1000 and columns 0 and 1 from csv file  infile.csv and write output to outfile.diff 
	CF is a parameter which tells how to decide when to store a local dictionary. Using CF the cost function is used. 
	Other parameters used in experiment 4.2 are OF, LD, DF, CD, CT.
	Parameter DEFAULT leads to the default execution of compression algorithm. 
	If set to DISABLE_THRESHOLD, it disables the threshold of distict values/total values used to decide if dictionary encoding is enabled.
	This is used in experiment 4.2 to compare the cost function to other alternatives enforcing dictionary encoding in any case.   

Compress with Python, local, global, indirect and adaptive encoding:
	```
  from pyimplementation.pyarcade import write
	import pyimplementation.global_encoding as global_encoding
	import pandas as pd
	df = pd.read_csv("inputfile.csv") # edit with csv input file path
	write("outfile.diff", df, row_group_offsets=65535, format="LOCAL")
	global_encoding.write_global(df,"outfile.unorderedglob",0,65535)
	write("outfile.diff", df, row_group_offsets=65535, format="ADAPTIVE")
	global_encoding.write_indirect(df,"outfile.unindirect",0,65535)
```
    Using any csv file, and the above python snippet, it is possible to produce 4 compressed      
    files.

Read executable scans, filter scans, and looks-up specific rows as follows:
   ```
   ./read file.diff 0   -- full scans column 0 of a file and prints the row count
    ./read file.diff 0 "value" 1 1 -- filter scan first column for value, bool values 1 1 means that zone-maps and diff min max values are enabled, otherwise
    ./read file.diff 0 600 -- returns value with row id = 600 (starts from 0)
    ```
    This executable reads files produced with both Python or C++. 
    
    
Using fastparquet interface:
```
	#write
	from pyimplementation.fastparquet import write
	import pandas as pd
	df = pd.read_csv("file.csv")
	write("file.parq", df, row_group_offsets=65535)
	write("file.diffparq", df, row_group_offsets=65535, format = "ADAPTIVE")
	
	#read
	from pyimplementation.fastparquet import ParquetFile
	import pandas as pd
  pf = ParquetFile('inputfile')
  df_full = pf.to_pandas()
  df = pf.to_pandas(['column_name'], filters=[('column_name', '=', 'value')])
  newdf = df[(df.c1 == "value")]
    
```

3. NOTES:

Datasets from yelp and edgar aren’t exactly the same as the ones used in the experiments of the paper. They were re-created and this caused some small differences in experiments 4.2 and 4.3. For example, in section 4.3, differential encoding omits 169 blocks instead of 172 and local/global encoding omits 9 blocks instead of 10.

Files pyimplementation/global_encoding_opt.py, pyimplementation/pyarcade/writer_opt.py contain a more optimized version that was implemented after the submission of the paper. All the claims are still valid, what has changed is that all the compression techniques compress faster, producing exactly the same files, and using these optimisations the calculation of differential dictionaries is even faster. To test with this simply rename the files (i.e., rename writer_opt.py to writer.py and run the experiments).
https://github.com/madgik/arcade contains the last and more stable version of the proposed format implemented in C++. 
