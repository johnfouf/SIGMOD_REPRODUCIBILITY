A) 
Compiler Info: g++ version 10 or newer with c++20 and coroutines, Ubuntu 20.04
Packages/Libraries Needed: zlib (-lz), snappy (-lsnappy), googletest (-lgtest)

B) Datasets
https://drive.google.com/file/d/13voW0OmvHjPhWxPNkQ-GvTCcs41ACf73/view?usp=sharing

C) Hardware Info
C1) Processor Intel(R) Core(TM) i7-4790 CPU @ 3.60GHz, x86_64, 1 socket, 4 cores per socket, 2 threads per core
C2) Caches: L1d cache: 32K, L1i cache: 32K, L2 cache: 256K, L3 cache: 8192K
C3) Memory 16GiB DIMM DDR3 Synchronous 1333 MHz
C4) Secondary Storage: Samsung SSD 850 EVO 250GB, SATA Version is:  SATA 3.1, 6.0 Gb/s (current: 6.0 Gb/s)

D) Experimentation Info
Clone repository and run
```cd SIGMOD_REPRODUCABILITY```

D1) Compilations:
dependencies install for C:
```
sudo apt-get install libboost-all-dev 
sudo apt-get install libgtest-dev
sudo apt-get install libsnappy-dev
sudo apt-get install zlib1g-dev
```
dependencies install for Python3:
```
pip install thrift
pip install numba
pip install msgpack
pip install msgpack_numpy
pip install python-snappy
```

- compile CPP reader: g++ -O3 -std=c++11 -o read read.cpp -lsnappy -lz -Iarcade -Iarcadecpp/msgpack-c-cpp_master/include/
- compile CPP compression runner:

cd arcade

make runner

cd ../

D1) Scripts and how-tos to generate all necessary data or locate datasets

get data from https://drive.google.com/file/d/1CO5gX5d6jmbiLgjpL7xRtwyzt7krxOIS/view?usp=sharing
untar the file and gunzip recursively all including files:
```
tar -xvf reproducibility.tar
gunzip -r reprod_datasets
```

D2) Execution of experiments (prints results and times at standard output)

- For experiment section 4.1 (figures 5 and 6)
       
	```
	python3 experiment4.1_compression.py
	./experiment4.1_read.sh  
	```
	(ran as root as it drops frees pagecache, dentries and inodes per command)
        
	The first command compresses the files and using time command times each execution
	The second command runs filtered scan, full scan, and random access queries and print their results,
	the execution times is shown with  unix time command. 
	In case of full scans, the row count is printed, the column is materialized in memory.
	
- For experiment section 4.2 (figure 7)

    ``` arcadecpp/runner < experiment4.2_compress.queries```
     
     The command prints execution times and file sizes per case. The files are stored at the current directory


- For experiment section 4.3 (tables 2 and 3)
	```
	python3 experiment4.3_compression.py
	./experiment4.3_read.sh 
	```  
	(run as root as it frees pagecache, dentries and inodes per command)
        
	The first command compresses the files, which are stored at the current directory
	The second executable runs filtered scan with 2 values and prints # of blocks and offsets scan that are omit, total and I/O times using gettimeofday function 
	Unix time is also executed per command
	

- For experiment section 4.4 (figure 8)
    for C++ implementation run:
    ```
        arcadecpp/runner < experiment4.4_C_compress.queries
         ./experiment4.4_read_C.sh  (run as root as it frees pagecache, dentries and inodes per command)
    ```
    for Python implementation run
    ```
        python3 experiment4.4_compress_python.py
        ./experiment4.4_read_PYTHON.sh (run as root as it frees pagecache, dentries and inodes per command)
    ```
        
    In both cases:
        The first command compresses the citation file with 2 ways and prints the times
        The second command runs a filtered scan in both files and prints the times and the result count


E) Figures Creation 

- Python's matplotlib should be installed

 
 	```python3 -m pip install matplotlib```
   
- For experiment section 4.1 (figures 5 and 6)
 
 	``` 
	    python3 experiment4.1_compression.py |  python3 plots_experiment4.1_compression.py 
	```
 
 	This will create a new folder in the current directory (experiment4.1_compression_figs) including the figures for section 4.1 (compression times and sizes)
	
	
	```
	./experiment4.1_read.sh > results4.1read.txt 2>&1
	python3 plots_experiment4.1_read.py
	``` 
	
	This will create a new folder in the current directory (experiment4.1_read_figs) including figures for section 4.1 (filters, full scans, random access)
	
- For experiment section 4.2 (figures 7)
    	Run 
       
    	``` arcadecpp/runner < experiment4.2_compress.queries > experiment4.2_compress.txt ```
    
    	to redirect the result into the txt file and then run
    
    	``` python3 plots_experiment4.2.py```
    
    	This will create a new folder in the current directory (experiment4.2_fig7) including the figures for section 4.2
    
    
- For experiment section 4.3 (tables 2 and 3)
       Run
       
        ```
		python3 experiment4.3_compression.py
		./experiment4.3_read.sh > results4.3read.txt
		python3 table_experiment4.3.py
	```
	  
	The last script will print the results to standard output in a comma separated format, so you could also redirect output to a csv file.
    
- For experiment section 4.4 (figure 8)
        Run 
       
    	
	```
	arcadecpp/runner < experiment4.4_C_compress.queries > experiment4.4_C_compress.txt
         ./experiment4.4_read_C.sh > results4.4read_C.txt 2>&1
        python3 experiment4.4_compress_python.py > experiment4.4_Python_compress.txt
        ./experiment4.4_read_PYTHON.sh > experiment4.4_Python_read.txt
	```
	
        

    	to redirect the results into txt files and then run
    
    	``` python3 plots_experiment4.4.py```
    
    	This will create a new folder in the current directory (experiment4.4_figs) including the figure for section 4.4
	
See also IMPORTANT_NOTES for instructions on running the code on different datasets and validate.
