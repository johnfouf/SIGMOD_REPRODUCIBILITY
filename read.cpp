#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>
#include <msgpack.hpp>
#include <iostream>
#include <sstream>
#include <unistd.h>
#include <map>
#include <iterator>
#include <sys/time.h>
#include <math.h>
#include <zlib.h>

//////
#include <cassert>
#include <fstream>
#include <iostream>
#include <memory>


#include "snappy.h"

#include <getopt.h>
#include <string>
#include <memory>
#include <iostream>
#include <string>


#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <vector>
#include <cmath>
#include <iostream>
#include <vector>





using namespace std;

struct D {
    int dictsize;
    int indicessize;
    int numofvals;
    int bytes;
    int lendiff;
    int diff;
    int minmaxsize;
    int previndices;
    int minmaxdiffsize;
};

struct Dglob {
    int minmaxsize;
    int indicessize;
    int bytes;
    int numofvals;
};

struct Dindirect {
    int minmaxsize;
    int indicessize;
    int bytes;
    int numofvals;
    int mapsize;
    int mapbytes;
    int mapcount;
};

struct fileH {
     int numofvals;
     int numofcols;
     int numofblocks;
};

struct fileHglob {
     int numofvals;
     int numofcols;
     int glob_size;
     int numofblocks;
};



int SNAPPY = 0;

size_t result;
  
int compare(int offset, string op, vector <float> predicate, vector <int> predminmax){
    if (op==">")
    for (int i=0; i < predicate.size(); i++)
        if (offset > predicate[i] and offset <= predminmax[i])
            return 1;
            
    if (op == "<")
         for (int i=0; i < predicate.size(); i++)
        if ( offset < predicate[i] and offset >= predminmax[i])
            return 1;
    
    
return 0;

}



int read_glob_mater(int argc, char * argv[] ){
    int totalcount1=0;
    FILE *f1;
    f1 = fopen(argv[1],"rb");
    vector <string> global_dict;
    
    /*load file 1 in memory*/
    fseek(f1, 0, SEEK_END);
    long fsize1 = ftell(f1);
    fseek(f1, 0, SEEK_SET);  
    char *fptr1 = (char*)malloc(fsize1 + 1);
    result =  fread(fptr1, 1, fsize1, f1);
    fclose(f1);
    
    /*read marker of file*/
    char marker[5];
    memcpy( marker, &fptr1[0], 4);
    
    struct fileHglob fileheader1;
    memcpy(&fileheader1,&fptr1[4], sizeof(struct fileHglob));
    vector <vector <int>> index1;
    //cout << fileheader1.numofvals << " " << fileheader1.numofcols << endl;
    
    string* column = new string[fileheader1.numofvals];
    unsigned long initstep1 = 4 + sizeof(struct fileHglob) + (fileheader1.numofblocks+1)*8;
	struct Dglob header1;
	
	int count = 0;

	int join1 = atoi(argv[2]);
	
	char buffer1[fileheader1.glob_size];
    memcpy(buffer1,&fptr1[initstep1],fileheader1.glob_size);
    msgpack::unpacked result1;
    unpack(result1, buffer1,fileheader1.glob_size);
    vector<string> values1;
    result1.get().convert(values1);
    initstep1 += fileheader1.glob_size;
    
    while (totalcount1 < fileheader1.numofvals){
        memcpy(&header1,&fptr1[initstep1], sizeof(struct Dglob));
        int next = sizeof(struct Dglob) + header1.minmaxsize + header1.indicessize;
         if (header1.indicessize == 4){
                int offf;
                memcpy(&offf, &fptr1[initstep1+sizeof(struct Dglob)+header1.minmaxsize], header1.indicessize);
                for (int i=0; i < header1.numofvals; i++){
     		    	column[count] = values1[offf];
     		    	count++;
     		    }
     			initstep1 += next;
            
            }
        else{
        if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dglob)+header1.minmaxsize], header1.indicessize);
				for (int i=0; i < header1.numofvals; i++){
     		    	column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dglob)+header1.minmaxsize], header1.indicessize);
				for (int i=0; i < header1.numofvals; i++){
     		    	column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next ;
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dglob)+header1.minmaxsize], header1.indicessize);
     			for (int i=0; i < header1.numofvals; i++){
     		    	column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next ;
     		}
     	}
        

        totalcount1 += header1.numofvals;
    
    }
    
    
    cout << count << endl;
    return 1;
}


int read_glob(int argc, char * argv[] ){
    int totalcount1=0;
    FILE *f1;
    f1 = fopen(argv[1],"rb");
    vector <string> global_dict;
    
    /*load file 1 in memory*/
    fseek(f1, 0, SEEK_END);
    long fsize1 = ftell(f1);
    fseek(f1, 0, SEEK_SET);  
    char *fptr1 = (char*)malloc(fsize1 + 1);
    result =  fread(fptr1, 1, fsize1, f1);
    fclose(f1);
    
    /*read marker of file*/
    char marker[5];
    memcpy( marker, &fptr1[0], 4);
    
    struct fileHglob fileheader1;
    memcpy(&fileheader1,&fptr1[4], sizeof(struct fileHglob));
    vector <vector <int>> index1;
    //cout << fileheader1.numofvals << " " << fileheader1.numofcols << endl;
    
    //string* column = new string[fileheader1.numofvals];
	unsigned long initstep1 = 4 + sizeof(struct fileHglob);
	struct Dglob header1;
	
	int count = 0;

	int join1 = atoi(argv[2]);
	
	char buffer1[fileheader1.glob_size];
    memcpy(buffer1,&fptr1[initstep1],fileheader1.glob_size);
    msgpack::unpacked result1;
    unpack(result1, buffer1,fileheader1.glob_size);
    vector<string> values1;
    result1.get().convert(values1);
    initstep1 += fileheader1.glob_size;
    
    while (totalcount1 < fileheader1.numofvals){
        memcpy(&header1,&fptr1[initstep1], sizeof(struct Dglob));
        int next = sizeof(struct Dglob) + header1.minmaxsize + header1.indicessize;
         if (header1.indicessize == 4){
                int offf;
                memcpy(&offf, &fptr1[initstep1+sizeof(struct Dglob)+header1.minmaxsize], header1.indicessize);
                for (int i=0; i < header1.numofvals; i++){
     		    	//column[count] = values1[offf];
     		    	count++;
     		    }
     			initstep1 += next;
            
            }
        else{
        if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dglob)+header1.minmaxsize], header1.indicessize);
				for (int i=0; i < header1.numofvals; i++){
     		    	//column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dglob)+header1.minmaxsize], header1.indicessize);
				for (int i=0; i < header1.numofvals; i++){
     		    	//column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next ;
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dglob)+header1.minmaxsize], header1.indicessize);
     			for (int i=0; i < header1.numofvals; i++){
     		    	//column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next ;
     		}
     	}
        

        totalcount1 += header1.numofvals;
    
    }
    
    
    cout << count << endl;
    fclose(f1);
    return 1;
}

int read_indirect_mater(int argc, char * argv[] ){
    int totalcount1=0;
    FILE *f1;
    f1 = fopen(argv[1],"rb");
    vector <string> global_dict;
    
    /*load file 1 in memory*/
    fseek(f1, 0, SEEK_END);
    long fsize1 = ftell(f1);
    fseek(f1, 0, SEEK_SET);  
    char *fptr1 = (char*)malloc(fsize1 + 1);
    result =  fread(fptr1, 1, fsize1, f1);
    fclose(f1);
    
    /*read marker of file*/
    char marker[5];
    memcpy( marker, &fptr1[0], 4);
    
    struct fileHglob fileheader1;
    memcpy(&fileheader1,&fptr1[4], sizeof(struct fileHglob));
    vector <vector <int>> index1;
    //cout << fileheader1.numofvals << " " << fileheader1.numofcols << endl;
    
    string* column = new string[fileheader1.numofvals];
    unsigned long initstep1 = 4 + sizeof(struct fileHglob) + (fileheader1.numofblocks+1)*8;
	struct Dindirect header1;
	
	int count = 0;

	int join1 = atoi(argv[2]);
	
	char buffer1[fileheader1.glob_size];
    memcpy(buffer1,&fptr1[initstep1],fileheader1.glob_size);
    msgpack::unpacked result1;
    unpack(result1, buffer1,fileheader1.glob_size);
    vector<string> values1;
    result1.get().convert(values1);
    initstep1 += fileheader1.glob_size;
    
    while (totalcount1 < fileheader1.numofvals){
        memcpy(&header1,&fptr1[initstep1], sizeof(struct Dindirect));
        //cout << header1.indicessize <<" "<< header1.numofvals << " "<< header1.minmaxsize << " "<< header1.bytes <<" "<< header1.mapsize << " "<< header1.mapbytes <<endl;
        int next = sizeof(struct Dindirect) + header1.minmaxsize + header1.indicessize + header1.mapsize;
        
        /* read mapping - possible bug here, with casting to unsigned int */
        
        if (header1.mapbytes==1){
                unsigned short int map [header1.mapcount];
     			memcpy(map, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize], header1.mapsize);
     			if (header1.indicessize == 4){
                int offf;
                memcpy(&offf, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
                for (int i=0; i < header1.numofvals; i++){
                    if (header1.mapsize > 0)
     		    	    column[count] = values1[map[offf]];
     		    	else
     		    	    column[count] = values1[offf];
     		    	count++;
     		    }
     			initstep1 += next;
            
            }
             else{
                if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
				for (int i=0; i < header1.numofvals; i++){
				    if (header1.mapsize > 0)
     		    	    column[count] = values1[map[offsets1[i]]];
     		    	else
     		    	    column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
				for (int i=0; i < header1.numofvals; i++){
     		    	if (header1.mapsize > 0)
     		    	    column[count] = values1[map[offsets1[i]]];
     		    	else
     		    	    column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next ;
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
     			for (int i=0; i < header1.numofvals; i++){
     		    	if (header1.mapsize > 0)
     		    	    column[count] = values1[map[offsets1[i]]];
     		    	else
     		    	    column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next ;
     		}
     	}
     			}
     		if (header1.mapbytes==0){
     		    unsigned int map [header1.mapcount];
     			memcpy(map, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize], header1.mapsize);
     			if (header1.indicessize == 4){
                int offf;
                memcpy(&offf, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
                for (int i=0; i < header1.numofvals; i++){
     		    	if (header1.mapsize > 0)
     		    	    column[count] = values1[map[offf]];
     		    	else
     		    	    column[count] = values1[offf];
     		    	count++;
     		    }
     			initstep1 += next;
            
            }
        else{
        if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
				for (int i=0; i < header1.numofvals; i++){
     		    	if (header1.mapsize > 0)
     		    	    column[count] = values1[map[offsets1[i]]];
     		    	else
     		    	    column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
				for (int i=0; i < header1.numofvals; i++){
     		    	if (header1.mapsize > 0)
     		    	    column[count] = values1[map[offsets1[i]]];
     		    	else
     		    	    column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next ;
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
     			for (int i=0; i < header1.numofvals; i++){
     		    	if (header1.mapsize > 0)
     		    	    column[count] = values1[map[offsets1[i]]];
     		    	else
     		    	    column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next ;
     		}
     	}
     			}
     		if (header1.mapbytes==2){
     		    unsigned char map [header1.mapcount];
     			memcpy(map, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize], header1.mapsize);
     			if (header1.indicessize == 4){
                int offf;
                memcpy(&offf, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
                for (int i=0; i < header1.numofvals; i++){
     		    	if (header1.mapsize > 0)
     		    	    column[count] = values1[map[offf]];
     		    	else
     		    	    column[count] = values1[offf];
     		    	count++;
     		    }
     			initstep1 += next;
            
            }
        else{
        if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
				for (int i=0; i < header1.numofvals; i++){
     		    	if (header1.mapsize > 0)
     		    	    column[count] = values1[map[offsets1[i]]];
     		    	else
     		    	    column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
				for (int i=0; i < header1.numofvals; i++){
     		    	if (header1.mapsize > 0)
     		    	    column[count] = values1[map[offsets1[i]]];
     		    	else
     		    	    column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next ;
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
     			for (int i=0; i < header1.numofvals; i++){
     		    	if (header1.mapsize > 0)
     		    	    column[count] = values1[map[offsets1[i]]];
     		    	else
     		    	    column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next ;
     		}
     	}
     			
     			}

        /* end of read mapping */
        
        
        
         
        

        totalcount1 += header1.numofvals;
    
    }
    
    cout << count << endl;
    return 1;
}


int read_indirect(int argc, char * argv[] ){
    int totalcount1=0;
    FILE *f1;
    f1 = fopen(argv[1],"rb");
    vector <string> global_dict;
    
    /*load file 1 in memory*/
    fseek(f1, 0, SEEK_END);
    long fsize1 = ftell(f1);
    fseek(f1, 0, SEEK_SET);  
    char *fptr1 = (char*)malloc(fsize1 + 1);
    result =  fread(fptr1, 1, fsize1, f1);
    fclose(f1);
    
    /*read marker of file*/
    char marker[5];
    memcpy( marker, &fptr1[0], 4);
    
    struct fileHglob fileheader1;
    memcpy(&fileheader1,&fptr1[4], sizeof(struct fileHglob));
    vector <vector <int>> index1;
    //cout << fileheader1.numofvals << " " << fileheader1.numofcols << endl;
    
    //string* column = new string[fileheader1.numofvals];
	unsigned long initstep1 = 4 + sizeof(struct fileHglob);
	struct Dindirect header1;
	
	int count = 0;

	int join1 = atoi(argv[2]);
	
	char buffer1[fileheader1.glob_size];
    memcpy(buffer1,&fptr1[initstep1],fileheader1.glob_size);
    msgpack::unpacked result1;
    unpack(result1, buffer1,fileheader1.glob_size);
    vector<string> values1;
    result1.get().convert(values1);
    initstep1 += fileheader1.glob_size;
    
    while (totalcount1 < fileheader1.numofvals){
        memcpy(&header1,&fptr1[initstep1], sizeof(struct Dindirect));
        //cout << header1.indicessize <<" "<< header1.numofvals << " "<< header1.minmaxsize << " "<< header1.bytes <<" "<< header1.mapsize << " "<< header1.mapbytes <<endl;
        int next = sizeof(struct Dindirect) + header1.minmaxsize + header1.indicessize + header1.mapsize;
        
        /* read mapping - possible bug here, with casting to unsigned int */
        unsigned int map [header1.mapcount];
        if (header1.mapbytes==1)
     			memcpy(map, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize], header1.mapsize);
     		if (header1.mapbytes==0)
     			memcpy(map, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize], header1.mapsize);
     		if (header1.mapbytes==2)
     			memcpy(map, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize], header1.mapsize);

        /* end of read mapping */
        
        
        
         if (header1.indicessize == 4){
                int offf;
                memcpy(&offf, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
                for (int i=0; i < header1.numofvals; i++){
     		    	//column[count] = values1[offf];
     		    	count++;
     		    }
     			initstep1 += next;
            
            }
        else{
        if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
				for (int i=0; i < header1.numofvals; i++){
     		    	//column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
				for (int i=0; i < header1.numofvals; i++){
     		    	//column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next ;
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
     			for (int i=0; i < header1.numofvals; i++){
     		    	//column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next ;
     		}
     	}
        

        totalcount1 += header1.numofvals;
    
    }
    
    cout << count << endl;
    fclose(f1);
    return 1;
}

int read_indirect_filt(int argc, char * argv[] ){
    int totalcount1=0;
    FILE *f1;
    f1 = fopen(argv[1],"rb");
    vector <string> global_dict;
    string value = argv[3];
    
    /*read marker of file*/
    char marker[5];
    result = fread( marker, 4, 1, f1);
    
    struct fileHglob fileheader1;
    result = fread(&fileheader1,sizeof(struct fileHglob), 1, f1);
    vector <vector <int>> index1;
    //cout << fileheader1.numofvals << " " << fileheader1.numofcols << endl;
    
    //string* column = new string[fileheader1.numofvals];
    unsigned long initstep1 = 4 + sizeof(struct fileHglob) + (fileheader1.numofblocks+1)*8;
	struct Dindirect header1;
	fseek(f1, initstep1, SEEK_SET);
	int count = 0;
	int fcount = 0;

	int join1 = atoi(argv[2]);
	int* col = new int[fileheader1.numofvals];
	char* buffer1 = new char[fileheader1.glob_size];
	result = fread(buffer1,fileheader1.glob_size, 1, f1);
    msgpack::unpacked result1;
    unpack(result1, buffer1,fileheader1.glob_size);
    vector<string> values1;
    result1.get().convert(values1);
    
    
    int offset = -1;
    for (int k = 0; k < values1.size(); k++){
   			   if (value == values1[k]){
   			     offset = k;
   			     }
   			 }
   		 
   	if (offset == -1){
   	 cout << "0" << endl;
   	 return 0;
   	 }
    
    initstep1 += fileheader1.glob_size;
    
    while (totalcount1 < fileheader1.numofvals){
        result = fread(&header1, sizeof(struct Dindirect), 1, f1);
        //memcpy(&header1,&fptr1[initstep1], sizeof(struct Dindirect));
        //cout << header1.indicessize <<" "<< header1.numofvals << " "<< header1.minmaxsize << " "<< header1.bytes <<" "<< header1.mapsize << " "<< header1.mapbytes <<endl;
        int next = sizeof(struct Dindirect) + header1.minmaxsize + header1.indicessize + header1.mapsize;
        char minmaxbuf[header1.minmaxsize];
   		fseek(f1,initstep1+sizeof(struct Dindirect), SEEK_SET);
    	result =  fread(minmaxbuf,header1.minmaxsize,1,f1);
    	msgpack::unpacked minmax;
    	unpack(minmax, minmaxbuf, header1.minmaxsize);
    	vector<string> minmax1;
    	minmax.get().convert(minmax1);
    	
        vector <int> mymap;
   		bool found_offset = 0;  	
        if (header1.mapbytes==1){
                unsigned short int map [header1.mapcount];
                if (header1.mapsize > 0){
                    fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize,SEEK_SET);
                    result = fread(map,  header1.mapsize, 1, f1);
                    int c = 0;
                    for ( unsigned short int i: map)
                        mymap.push_back(i);
     			    //memcpy(map, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize], header1.mapsize);
     			   }
     			   
     			
     			if (header1.indicessize == 4){
                int offf;
                result = fread(&offf,  header1.indicessize, 1, f1);
                
                if (header1.mapsize > 0){
                    if (mymap[offf] == offset)
                      for (int i=0; i < header1.numofvals; i++){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    count++;
     		        }
     		        else {count += header1.numofvals;}
                }
     		    else {
     		        if (offf == offset)
                      for (int i=0; i < header1.numofvals; i++){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    count++;
     		        }
     		        else {count += header1.numofvals;}
     		    }
     		    
     			initstep1 += next;
            
            }
        else{
        
        if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize,SEEK_SET);
                result = fread(offsets1,  header1.indicessize, 1, f1);
     			//memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
     			if (header1.mapsize>0) {
				  for (int i=0; i < header1.numofvals; i++){
     		    	if (mymap[offsets1[i]] == offset){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    
     		    	 }
     		    	count++;
     		      }
     		    }
     		    else{
     		      for (int i=0; i < header1.numofvals; i++){
     		    	if (offsets1[i] == offset){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    
     		    	 }
     		    	count++;
     		      }
     		    }
     			initstep1 += next;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize,SEEK_SET);
                result = fread(offsets1,  header1.indicessize, 1, f1);
     			//memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
				if (header1.mapsize>0) {
				  for (int i=0; i < header1.numofvals; i++){
     		    	if (mymap[offsets1[i]] == offset){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    
     		    	 }
     		    	count++;
     		      }
     		    }
     		    else{
     		      for (int i=0; i < header1.numofvals; i++){
     		    	if (offsets1[i] == offset){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    
     		    	 }
     		    	count++;
     		      }
     		    }
     			initstep1 += next ;
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize,SEEK_SET);
                result = fread(offsets1,  header1.indicessize, 1, f1);
     			//memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
     			if (header1.mapsize>0) {
				  for (int i=0; i < header1.numofvals; i++){
     		    	if (mymap[offsets1[i]] == offset){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    
     		    	 }
     		    	count++;
     		      }
     		    }
     		    else{
     		      for (int i=0; i < header1.numofvals; i++){
     		    	if (offsets1[i] == offset){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    
     		    	 }
     		    	count++;
     		      }
     		    }
     			initstep1 += next ;
     		}
     	}
     		}
     		if (header1.mapbytes==0){
     		   unsigned int map [header1.mapcount];
     		   //std::unordered_map<int, unsigned int> map;
     		   if (header1.mapsize > 0){
     			    fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize,SEEK_SET);
                    result = fread(map,  header1.mapsize, 1, f1);
                    int c = 0;
                    for ( unsigned int i: map)
                        mymap.push_back(i);
     			   }
     			if (header1.indicessize == 4){
                int offf;
                result = fread(&offf,  header1.indicessize, 1, f1);
     		    if (header1.mapsize > 0){
                    if (mymap[offf] == offset)
                      for (int i=0; i < header1.numofvals; i++){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    count++;
     		        }
     		        else {count += header1.numofvals;}
                }
     		    else {
     		        if (offf == offset)
                      for (int i=0; i < header1.numofvals; i++){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    count++;
     		        }
     		        else {count += header1.numofvals;}
     		    }
     		    
     			initstep1 += next;
            
            }
        else{
        if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize + header1.mapsize,SEEK_SET);
                result = fread(offsets1,  header1.indicessize, 1, f1);
     			if (header1.mapsize>0) {
				  for (int i=0; i < header1.numofvals; i++){
     		    	if (mymap[offsets1[i]] == offset){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	 }
     		    	count++;
     		      }
     		    }
     		    else{
     		      for (int i=0; i < header1.numofvals; i++){
     		    	if (offsets1[i] == offset){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    
     		    	 }
     		    	count++;
     		      }
     		    }
     			initstep1 += next;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize,SEEK_SET);
                result = fread(offsets1,  header1.indicessize, 1, f1);
     			//memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
				if (header1.mapsize>0) {
				  for (int i=0; i < header1.numofvals; i++){
     		    	if (mymap[offsets1[i]] == offset){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    
     		    	 }
     		    	count++;
     		      }
     		    }
     		    else{
     		      for (int i=0; i < header1.numofvals; i++){
     		    	if (offsets1[i] == offset){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    
     		    	 }
     		    	count++;
     		      }
     		    }
     			initstep1 += next ;
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize,SEEK_SET);
                result = fread(offsets1,  header1.indicessize, 1, f1);
     			//memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
     			if (header1.mapsize>0) {
				  for (int i=0; i < header1.numofvals; i++){
     		    	if (mymap[offsets1[i]] == offset){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    
     		    	 }
     		    	count++;
     		      }
     		    }
     		    else{
     		      for (int i=0; i < header1.numofvals; i++){
     		    	if (offsets1[i] == offset){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    
     		    	 }
     		    	count++;
     		      }
     		    }
     			initstep1 += next ;
     		}
     	}
     			}
     		if (header1.mapbytes==2){
     		    unsigned char map [header1.mapcount];
     			if (header1.mapsize > 0){
     			    fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize,SEEK_SET);
                    result = fread(map,  header1.mapsize, 1, f1);
                    int c = 0;
                    for ( unsigned char i: map)
                        mymap.push_back(i);
     			    //memcpy(map, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize], header1.mapsize);
     			   }
     			if (header1.indicessize == 4){
                int offf;
                result = fread(&offf,  header1.indicessize, 1, f1);
     		    if (header1.mapsize > 0){
                    if (mymap[offf] == offset)
                      for (int i=0; i < header1.numofvals; i++){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    count++;
     		        }
     		        else {count += header1.numofvals;}
                }
     		    else {
     		        if (offf == offset)
                      for (int i=0; i < header1.numofvals; i++){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    count++;
     		        }
     		        else {count += header1.numofvals;}
     		    }
     		    
     			initstep1 += next;
            
            }
        else{
        if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize,SEEK_SET);
                result = fread(offsets1,  header1.indicessize, 1, f1);
     			//memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
				if (header1.mapsize>0) {
				  for (int i=0; i < header1.numofvals; i++){
     		    	if (mymap[offsets1[i]] == offset){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    
     		    	 }
     		    	count++;
     		      }
     		    }
     		    else{
     		      for (int i=0; i < header1.numofvals; i++){
     		    	if (offsets1[i] == offset){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    
     		    	 }
     		    	count++;
     		      }
     		    }
     			initstep1 += next;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize,SEEK_SET);
                result = fread(offsets1,  header1.indicessize, 1, f1);
     			//memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
				if (header1.mapsize>0) {
				  for (int i=0; i < header1.numofvals; i++){
     		    	if (mymap[offsets1[i]] == offset){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    
     		    	 }
     		    	count++;
     		      }
     		    }
     		    else{
     		      for (int i=0; i < header1.numofvals; i++){
     		    	if (offsets1[i] == offset){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    
     		    	 }
     		    	count++;
     		      }
     		    }
     			initstep1 += next ;
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize,SEEK_SET);
                result = fread(offsets1,  header1.indicessize, 1, f1);
     			//memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
     			if (header1.mapsize>0) {
				  for (int i=0; i < header1.numofvals; i++){
     		    	if (mymap[offsets1[i]] == offset){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    
     		    	 }
     		    	count++;
     		      }
     		    }
     		    else{
     		      for (int i=0; i < header1.numofvals; i++){
     		    	if (offsets1[i] == offset){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    
     		    	 }
     		    	count++;
     		      }
     		    }
     			initstep1 += next ;
     		}
     	}
     }

     /* end of read mapping */
    totalcount1 += header1.numofvals;
    
    }
    
    delete buffer1;
    cout <<  "result count: " << fcount << endl;
    fclose(f1);
    return 1;
}




int read_diff_materialize(int argc, char * argv[] ){
    int totalcount1=0;
    FILE *f1;
    f1 = fopen(argv[1],"rb");
    vector <string> global_dict;
    vector <string> parquetvalues1;
    /*load file 1 in memory*/
    fseek(f1, 0, SEEK_END);
    long fsize1 = ftell(f1);
    fseek(f1, 0, SEEK_SET);  
    char *fptr1 = (char*)malloc(fsize1 + 1);
    result =  fread(fptr1, 1, fsize1, f1);
    fclose(f1);
    
    /*read marker of file*/
    char marker[5];
    memcpy( marker, &fptr1[0], 4);
    
    struct fileH fileheader1;
    memcpy(&fileheader1,&fptr1[4], sizeof(struct fileH));
    vector <vector <int>> index1;
    //cout << fileheader1.numofvals << " " << fileheader1.numofcols << endl;

    string* column = new string[fileheader1.numofvals];
	unsigned long initstep1 = 4 + sizeof(struct fileH) + (fileheader1.numofblocks+1)*8;
	struct D header1;
	
	int count = 0;

	int join1 = atoi(argv[2]);
 	while (totalcount1 < fileheader1.numofvals){
    	/*read block header*/
   		    int current, next;
   		    
   		    int i1,i2,i3;
   		    
   		    memcpy(&current,&fptr1[initstep1+sizeof(int)*join1],sizeof(int));
   		    memcpy(&next,&fptr1[initstep1+sizeof(int)*(fileheader1.numofcols)],sizeof(int));
            initstep1 += current + sizeof(int)*(fileheader1.numofcols+1);
   		    memcpy(&header1,&fptr1[initstep1], sizeof(struct D));
   		    
   		    if (header1.dictsize == 0){
   		        if (totalcount1 == 0)
   		            parquetvalues1.reserve(fileheader1.numofvals);
    		    char buffer1[header1.indicessize];

    			memcpy(buffer1,&fptr1[initstep1+sizeof(struct D)],header1.indicessize);
    			msgpack::unpacked result1;
    			unpack(result1, buffer1, header1.indicessize);
    			vector<string> values1;
    			result1.get().convert(values1);
    			parquetvalues1.insert( parquetvalues1.end(), values1.begin(), values1.end() );
   				totalcount1 += header1.numofvals;
   				initstep1 += next-current;

    		}
    		else {
            
            vector<string> values1;
            if (SNAPPY){
   			  char buffer1[header1.dictsize];
    		  memcpy(buffer1,&fptr1[initstep1+sizeof(struct D)+ header1.previndices*2 + header1.minmaxsize + header1.minmaxdiffsize],header1.dictsize);
    		  string output;
    		  snappy::Uncompress(buffer1, header1.dictsize, &output);
    		  msgpack::unpacked result1;
    		  unpack(result1, output.data(), output.size());
    		  result1.get().convert(values1);
    		}
    		else {
    		  char buffer1[header1.dictsize];
    		  memcpy(buffer1,&fptr1[initstep1+sizeof(struct D)+ header1.previndices*2 + header1.minmaxsize + header1.minmaxdiffsize],header1.dictsize);
    		  msgpack::unpacked result1;
    		  unpack(result1, buffer1, header1.dictsize);
    		  result1.get().convert(values1);
    		}
    		
    		
   			totalcount1 += header1.numofvals;

   			if (header1.diff == 1){
   			    global_dict.clear();
   			}

   			
   			global_dict.insert(global_dict.end(), values1.begin(), values1.end());

   			int lendict = global_dict.size();
   			if (header1.indicessize == 4){
                int offf;
                
                memcpy(&offf, &fptr1[initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize+header1.minmaxsize + + header1.minmaxdiffsize], header1.indicessize);
                if (offf < lendict)
                for (int i=0; i < header1.numofvals; i++){
     		    	column[count] = global_dict[offf];
     		    	count++;
     		    }
     		    else
     		    for (int i=0; i < header1.numofvals; i++){
     		    	column[count] = values1[offf-lendict];
     		    	count++;
     		    }
     			initstep1 += next - current;
            
            }
            else{
    		if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			if (SNAPPY){
   			       char buffer1[header1.indicessize];
    		       memcpy(buffer1,&fptr1[initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize+header1.minmaxsize + header1.minmaxdiffsize], header1.indicessize);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       
    		       copy(&output[0], &output[0]+(sizeof(unsigned short)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		    }
    		    else 
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize+header1.minmaxsize + header1.minmaxdiffsize], header1.indicessize);
     			
				if (values1.size()>0)
     			for (int i=0; i < header1.numofvals; i++){
     		    	    column[count] = global_dict[offsets1[i]];
     		    	count++;
     		    }
     		    else
				for (int i=0; i < header1.numofvals; i++){
     		    	column[count] = global_dict[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next-current;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			if (SNAPPY){
   			       char buffer1[header1.indicessize];
    		       memcpy(buffer1,&fptr1[initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize+header1.minmaxsize + header1.minmaxdiffsize], header1.indicessize);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       copy(&output[0], &output[0]+(sizeof(unsigned int)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		    }
    		    else
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize+header1.minmaxsize + header1.minmaxdiffsize], header1.indicessize);
				if (values1.size()>0)
     			for (int i=0; i < header1.numofvals; i++){
				    
     		    	    column[count] = global_dict[offsets1[i]];
     		    	    
     		    	count++;
     		    }
     		    else
				for (int i=0; i < header1.numofvals; i++){
     		    	column[count] = global_dict[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next-current ;
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			if (SNAPPY){
   			       char buffer1[header1.indicessize];
    		       memcpy(buffer1,&fptr1[initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize+header1.minmaxsize + header1.minmaxdiffsize], header1.indicessize);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       
    		       copy(&output[0], &output[0]+(sizeof(unsigned char)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		    }
    		    else
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize+header1.minmaxsize + header1.minmaxdiffsize], header1.indicessize);
     			if (values1.size()>0)
     			for (int i=0; i < header1.numofvals; i++){
     		    	    column[count] = global_dict[offsets1[i]];

     		    	    
     		    	count++;
     		    }
     		    else
				for (int i=0; i < header1.numofvals; i++){
     		    	column[count] = global_dict[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next-current ;
     		}
     		}
     		
     		
		}
	}

    cout << count << endl;
    return 0; 


}

int read_diff(int argc, char * argv[] ){
    int totalcount1=0;
    FILE *f1;
    f1 = fopen(argv[1],"rb");
    vector <string> global_dict;
    vector<string> parquetvalues1;
    /*load file 1 in memory*/
    fseek(f1, 0, SEEK_END);
    long fsize1 = ftell(f1);
    fseek(f1, 0, SEEK_SET);  
    char *fptr1 = (char*)malloc(fsize1 + 1);
    result =  fread(fptr1, 1, fsize1, f1);
    fclose(f1);
    
    /*read marker of file*/
    char marker[5];
    memcpy( marker, &fptr1[0], 4);
    
    struct fileH fileheader1;
    memcpy(&fileheader1,&fptr1[4], sizeof(struct fileH));
    vector <vector <int>> index1;
    //cout << fileheader1.numofvals << " " << fileheader1.numofcols << endl;

    //string* column = new string[fileheader1.numofvals];
	unsigned long initstep1 = 4 + sizeof(struct fileH) + (fileheader1.numofblocks+1)*8;
	struct D header1;
	
	int count = 0;

	int join1 = atoi(argv[2]);
 	while (totalcount1 < fileheader1.numofvals){
    	/*read block header*/
   		    int current, next;
   		    
   		    int i1,i2,i3;
   		    
   		    memcpy(&current,&fptr1[initstep1+sizeof(int)*join1],sizeof(int));
   		    memcpy(&next,&fptr1[initstep1+sizeof(int)*(fileheader1.numofcols)],sizeof(int));
            initstep1 += current + sizeof(int)*(fileheader1.numofcols+1);
   		    memcpy(&header1,&fptr1[initstep1], sizeof(struct D));
   		    
   		    if (header1.dictsize == 0){
   		        if (totalcount1 == 0)
   		            parquetvalues1.reserve(fileheader1.numofvals);
    		    char buffer1[header1.indicessize];
    			memcpy(buffer1,&fptr1[initstep1+sizeof(struct D)],header1.indicessize);
    			msgpack::unpacked result1;
    			unpack(result1, buffer1, header1.indicessize);
    			vector<string> values1;
    			result1.get().convert(values1);
    			parquetvalues1.insert( parquetvalues1.end(), values1.begin(), values1.end() );
   				totalcount1 += header1.numofvals;
   				initstep1 += next-current;
    		}
    		else {
    	   vector<string> values1;
    	   if (SNAPPY){
   			  char buffer1[header1.dictsize];
    		  memcpy(buffer1,&fptr1[initstep1+sizeof(struct D)+ header1.previndices*2 + header1.minmaxsize],header1.dictsize);
    		  string output;
    		  snappy::Uncompress(buffer1, header1.dictsize, &output);
    		  msgpack::unpacked result1;
    		  unpack(result1, output.data(), output.size());
    		  result1.get().convert(values1);
    		}
    		else {
   			char buffer1[header1.dictsize];
    		memcpy(buffer1,&fptr1[initstep1+sizeof(struct D)+ header1.previndices*2 + header1.minmaxsize],header1.dictsize);
    		msgpack::unpacked result1;
    		unpack(result1, buffer1, header1.dictsize);
    		result1.get().convert(values1);
    		}
   			totalcount1 += header1.numofvals;
   			if (header1.diff == 1){
   			    global_dict.clear();
   			}
   			
   			int lendict = global_dict.size();
   			if (header1.indicessize == 4){
                int offf;
                memcpy(&offf, &fptr1[initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize+header1.minmaxsize], header1.indicessize);
                if (offf < lendict)
                for (int i=0; i < header1.numofvals; i++){
     		    	//column[count] = global_dict[offf];
     		    	count++;
     		    }
     		    else
     		    for (int i=0; i < header1.numofvals; i++){
     		    	//column[count] = values1[offf-lendict];
     		    	count++;
     		    }
     			initstep1 += next - current;
            
            }
            else{
    		if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			if (SNAPPY){
   			       char buffer1[header1.indicessize];
    		       memcpy(buffer1,&fptr1[initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize+header1.minmaxsize], header1.indicessize);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       
    		       copy(&output[0], &output[0]+(sizeof(unsigned short)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		    }
    		    else 
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize+header1.minmaxsize], header1.indicessize);
				//if (values1.size()>0)
     			for (int i=0; i < header1.numofvals; i++){
				    /*if (offsets1[i] < lendict)
     		    	    column[count] = global_dict[offsets1[i]];
     		    	else
     		    	    column[count] = values1[offsets1[i]-lendict];*/
     		    	count++;
     		    }
     		  /*  else
				for (int i=0; i < header1.numofvals; i++){
     		    	column[count] = values1[offsets1[i]];
     		    	count++;
     		    }*/
     			initstep1 += next-current;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			if (SNAPPY){
   			       char buffer1[header1.indicessize];
    		       memcpy(buffer1,&fptr1[initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize+header1.minmaxsize], header1.indicessize);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       
    		       copy(&output[0], &output[0]+(sizeof(unsigned int)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		    }
    		    else 
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize+header1.minmaxsize], header1.indicessize);
				//if (values1.size()>0)
     			for (int i=0; i < header1.numofvals; i++){
				    /*if (offsets1[i] < lendict)
     		    	    column[count] = global_dict[offsets1[i]];
     		    	else
     		    	    column[count] = values1[offsets1[i]-lendict];
     		    	    */
     		    	count++;
     		    }
     		  /*  else
				for (int i=0; i < header1.numofvals; i++){
     		    	column[count] = values1[offsets1[i]];
     		    	count++;
     		    }*/
     			initstep1 += next-current ;
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			if (SNAPPY){
   			       char buffer1[header1.indicessize];
    		       memcpy(buffer1,&fptr1[initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize+header1.minmaxsize], header1.indicessize);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       
    		       copy(&output[0], &output[0]+(sizeof(unsigned char)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		    }
    		    else 
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize+header1.minmaxsize], header1.indicessize);
     			//if (values1.size()>0)
     			for (int i=0; i < header1.numofvals; i++){
				    /*if (offsets1[i] < lendict)
     		    	    column[count] = global_dict[offsets1[i]];
     		    	else
     		    	    column[count] = values1[offsets1[i]-lendict];
     		    	    */
     		    	count++;
     		    }
     		  /*  else
				for (int i=0; i < header1.numofvals; i++){
     		    	column[count] = values1[offsets1[i]];
     		    	count++;
     		    }*/
     			initstep1 += next-current ;
     		}
     		}
     		
     		global_dict.insert(global_dict.end(), values1.begin(), values1.end());
     		
		}
	}

    cout << count << endl;
    fclose(f1);
    return 0; 


}

int read_glob_filt(int argc, char * argv[] ){
     struct timeval begin = {0,0};
     struct timeval end = {0,0};
     struct timeval beginf = {0,0};
     struct timeval endf = {0,0};
     gettimeofday(&beginf, NULL);
    int boolminmax = atoi(argv[5]);
    int64_t ioduration = 0;
    int64_t tduration = 0;
    int ommits = 0; 
    int totalcount1=0;
    FILE *f1;
    f1 = fopen(argv[1],"rb");
    vector <string> global_dict;
    char marker[5];
    
    gettimeofday(&begin, NULL);
    fseek(f1, 0, SEEK_SET);  
    
    /*read marker of file*/
    
    
    result =  fread(marker, 4, 1, f1);
    
   
    struct fileHglob fileheader1;
    result =  fread(&fileheader1,sizeof(struct fileHglob), 1, f1);
    vector <vector <int>> index1;
    
    int* col = new int[fileheader1.numofvals];
    
    
    unsigned long initstep1 = 4 + sizeof(struct fileHglob) + (fileheader1.numofblocks+1)*8;
	struct Dglob header1;
	fseek(f1,initstep1,SEEK_SET);
	int count = 0;
	int fcount = 0;

	int join1 = atoi(argv[2]);
	string value = argv[3];
	char buffer1[fileheader1.glob_size];
    result =  fread(buffer1,fileheader1.glob_size,1,f1);
   
    gettimeofday(&end, NULL);
    ioduration += end.tv_usec-begin.tv_usec;
     msgpack::unpacked result1;
    unpack(result1, buffer1,fileheader1.glob_size);
    vector<string> values1;
    result1.get().convert(values1);
    initstep1 += fileheader1.glob_size;
    
    int offset = -1;
    for (int k = 0; k < values1.size(); k++){
   			   if (value == values1[k]){
   			     offset = k;
   			     break;
   			     }
   			     offset = -1;
   			 }
   		 
   	if (offset == -1){
   	 cout << "0" << endl;
   	 return 0;
   	 }
    
    
    while (totalcount1 < fileheader1.numofvals){
         gettimeofday(&begin, NULL);
        result =  fread(&header1, sizeof(struct Dglob),1,f1);
         gettimeofday(&end, NULL);
    ioduration += end.tv_usec-begin.tv_usec;
        totalcount1 += header1.numofvals;
        char minmaxbuf[header1.minmaxsize];
         gettimeofday(&begin, NULL);
        result =  fread(minmaxbuf,header1.minmaxsize,1,f1);
         gettimeofday(&end, NULL);
    ioduration += end.tv_usec-begin.tv_usec;
        
    	msgpack::unpacked minmax;
    	unpack(minmax, minmaxbuf, header1.minmaxsize);
    	vector<string> minmax1;
    	minmax.get().convert(minmax1);
        
        //cout << minmax1[0] << " " << minmax1[1] << endl;
        
        int next = sizeof(struct Dglob) + header1.minmaxsize + header1.indicessize;
        if (boolminmax)
        if (value.compare(minmax1[0])<0 or value.compare(minmax1[1])>0){
            initstep1 += next;
            fseek(f1,initstep1,SEEK_SET);
            count += header1.numofvals;
            ommits++;
            continue;
            }
        
        if (header1.indicessize == 4){
                int offf;
                 gettimeofday(&begin, NULL);
                fseek(f1,initstep1+sizeof(struct Dglob)+ header1.minmaxsize,SEEK_SET);
    			result =  fread(&offf,header1.indicessize,1,f1);
    			 gettimeofday(&end, NULL);
    ioduration += end.tv_usec-begin.tv_usec;
                if (offf == offset)
                for (int i=0; i < header1.numofvals; i++){
     		    	col[fcount] = count;
     		    	fcount++;
     		    	count++;
     		    }
     		    else count += header1.numofvals;
     			initstep1 += next;
            
        }
        else{
       
        if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			 gettimeofday(&begin, NULL);
     			fseek(f1,initstep1+sizeof(struct Dglob)+ header1.minmaxsize,SEEK_SET);
    			result =  fread(offsets1,header1.indicessize,1,f1);
    			 gettimeofday(&end, NULL);
    ioduration += end.tv_usec-begin.tv_usec;
				for (int i=0; i < header1.numofvals; i++){
     		    
     		    if (offsets1[i] == offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     			initstep1 += next;
     		}
     		else if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			 gettimeofday(&begin, NULL);
     			fseek(f1,initstep1+sizeof(struct Dglob)+ header1.minmaxsize,SEEK_SET);
    			result =  fread(offsets1,header1.indicessize,1,f1);
    			 gettimeofday(&end, NULL);
    ioduration += end.tv_usec-begin.tv_usec;
				for (int i=0; i < header1.numofvals; i++){
     		    
     		    if (offsets1[i] == offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     			initstep1 += next ;
     		}
     		else if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			gettimeofday(&begin, NULL);
     			fseek(f1,initstep1+sizeof(struct Dglob)+ header1.minmaxsize,SEEK_SET);
    			result =  fread(offsets1,header1.indicessize,1,f1);
    			 gettimeofday(&end, NULL);
    ioduration += end.tv_usec-begin.tv_usec;
     			for (int i=0; i < header1.numofvals; i++){
     		    
     		    if (offsets1[i] == offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     			initstep1 += next ;
     		}

       }
    
    }

   fclose(f1);
   if(-1 == gettimeofday(&endf,NULL)) {
     perror("gettimeofday");
     return -1;
   }
   tduration = endf.tv_usec - beginf.tv_usec;
   cout <<  "result count: " << fcount << " " << " blocks omitted: " << ommits << " io time: "<< ioduration << " total_time: "<< tduration <<endl; 
   return 1;
}

int random_access_glob(int argc, char * argv[] ){
      
    FILE *f1;
    f1 = fopen(argv[1],"rb");
    int row = atoi(argv[3]);
    
    fseek(f1, 0, SEEK_SET);  
    
    /*read marker of file*/
    char marker[5];
    result =  fread(marker, 4, 1, f1);
    
   
    struct fileHglob fileheader1;
    result =  fread(&fileheader1,sizeof(struct fileHglob), 1, f1);
    vector <vector <int>> index1;
    
    /*get blocknum and rowid*/
    long data[fileheader1.numofblocks];
    result =  fread(data,(fileheader1.numofblocks+1)*8,1,f1);
    int blocknum = row/(int)data[fileheader1.numofblocks];
    int rowid = row%(int)data[fileheader1.numofblocks];
	
	int count = 0;
	int fcount = 0;

	int join1 = atoi(argv[2]);
	char* buffer1 = new char[fileheader1.glob_size+1];
    result =  fread(buffer1,fileheader1.glob_size,1,f1);
    msgpack::unpacked result1;
    unpack(result1, buffer1,fileheader1.glob_size);
    vector<string> values1;
    result1.get().convert(values1);
    
    unsigned long initstep1 = data[blocknum];
    struct Dglob header1;
    fseek(f1, initstep1, SEEK_SET);
    result =  fread(&header1, sizeof(struct Dglob),1,f1);
    
    
     if (header1.indicessize == 4){
                int offf;
                fseek(f1,initstep1+sizeof(struct Dglob)+ header1.minmaxsize,SEEK_SET);
    			result =  fread(&offf,header1.indicessize,1,f1);
                cout << values1[offf] << endl;
        }
        else{
       
            if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1;
     			fseek(f1,initstep1+sizeof(struct Dglob)+ header1.minmaxsize + rowid*2,SEEK_SET);
     			result =  fread(&offsets1,2,1,f1);
                cout << values1[offsets1] << endl;
     		}
     		else if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1;
     			fseek(f1,initstep1+sizeof(struct Dglob)+ header1.minmaxsize+ rowid*4,SEEK_SET);
    			result =  fread(&offsets1,4,1,f1);
                cout << values1[offsets1] << endl;	
     		}
     		else if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1;
     			fseek(f1,initstep1+sizeof(struct Dglob)+ header1.minmaxsize+rowid,SEEK_SET);
    			result =  fread(&offsets1,1,1,f1);
                cout << values1[offsets1] << endl;
     			
     		}

       }
    fclose(f1);
    
    return 1;
}


int random_access_indirect(int argc, char * argv[] ){
      
    FILE *f1;
    f1 = fopen(argv[1],"rb");
    int row = atoi(argv[3]);
    
    fseek(f1, 0, SEEK_SET);  
    
    /*read marker of file*/
    char marker[5];
    result =  fread(marker, 4, 1, f1);
    
   
    struct fileHglob fileheader1;
    result =  fread(&fileheader1,sizeof(struct fileHglob), 1, f1);
    vector <vector <int>> index1;
    
    /*get blocknum and rowid*/
    long data[fileheader1.numofblocks];
    result =  fread(data,(fileheader1.numofblocks+1)*8,1,f1);
    int blocknum = row/(int)data[fileheader1.numofblocks];
    int rowid = row%(int)data[fileheader1.numofblocks];
	
	int count = 0;
	int fcount = 0;

	int join1 = atoi(argv[2]);
	char* buffer1 = new char[fileheader1.glob_size+1];
    result =  fread(buffer1,fileheader1.glob_size,1,f1);
    msgpack::unpacked result1;
    unpack(result1, buffer1,fileheader1.glob_size);
    vector<string> values1;
    result1.get().convert(values1);
    
    unsigned long initstep1 = data[blocknum];
    struct Dindirect header1;
    fseek(f1, initstep1, SEEK_SET);
    result =  fread(&header1, sizeof(struct Dindirect),1,f1);
    
    
     if (header1.indicessize == 4){
                int offf;
                fseek(f1,initstep1+sizeof(struct Dindirect)+ header1.minmaxsize + header1.mapsize,SEEK_SET);
    			result =  fread(&offf,header1.indicessize,1,f1);
    			if (header1.mapsize == 0)
                    cout << values1[offf] << endl;
                else{
                    fseek(f1,initstep1+sizeof(struct Dindirect)+ header1.minmaxsize,SEEK_SET);
                    int offf1;
                    result =  fread(&offf1,header1.mapsize,1,f1);
                    cout << values1[offf1] << endl;
                
                }
        }
        else{
       
            if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1;
     			fseek(f1,initstep1+sizeof(struct Dindirect)+ header1.minmaxsize +  header1.mapsize + rowid*2,SEEK_SET);
     			result =  fread(&offsets1,2,1,f1);
     			if (header1.mapsize == 0)
                    cout << values1[offsets1] << endl;
                else{
                    if (header1.mapbytes==1){ // 2 bytes
                        fseek(f1,initstep1+sizeof(struct Dindirect)+ header1.minmaxsize + offsets1*2,SEEK_SET);
                        unsigned short offf1;
                        result =  fread(&offf1,2,1,f1);
                        cout << values1[offf1] << endl;
                    }
                    if (header1.mapbytes==2){ // 1 byte
                        fseek(f1,initstep1+sizeof(struct Dindirect)+ header1.minmaxsize + offsets1*1,SEEK_SET);
                        unsigned char offf1;
                        result =  fread(&offf1,1,1,f1);
                        cout << values1[offf1] << endl;
                    }
                    if (header1.mapbytes==0){ //4 bytes
                        fseek(f1,initstep1+sizeof(struct Dindirect)+ header1.minmaxsize + offsets1*4,SEEK_SET);
                        unsigned int offf1;
                        result =  fread(&offf1,4,1,f1);
                        cout << values1[offf1] << endl;
                    }
                }
     		}
     		else if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1;
     			fseek(f1,initstep1+sizeof(struct Dindirect)+ header1.minmaxsize+ header1.mapsize +  rowid*4,SEEK_SET);
    			result =  fread(&offsets1,4,1,f1);
    			if (header1.mapsize == 0)
                    cout << values1[offsets1] << endl;	
                else{
                    if (header1.mapbytes==1){ // 2 bytes
                        fseek(f1,initstep1+sizeof(struct Dindirect)+ header1.minmaxsize + offsets1*2,SEEK_SET);
                        unsigned short offf1;
                        result =  fread(&offf1,2,1,f1);
                        cout << values1[offf1] << endl;
                    }
                    if (header1.mapbytes==2){ // 1 byte
                        fseek(f1,initstep1+sizeof(struct Dindirect)+ header1.minmaxsize + offsets1,SEEK_SET);
                        unsigned char offf1;
                        result =  fread(&offf1,1,1,f1);
                        cout << values1[offf1] << endl;
                    }
                    if (header1.mapbytes==0){ //4 bytes
                        fseek(f1,initstep1+sizeof(struct Dindirect)+ header1.minmaxsize + offsets1*4,SEEK_SET);
                        unsigned int offf1;
                        result =  fread(&offf1,4,1,f1);
                        cout << values1[offf1] << endl;
                    }
                }
     		}
     		else if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1;
     			fseek(f1,initstep1+sizeof(struct Dindirect)+ header1.minmaxsize+ header1.mapsize + rowid,SEEK_SET);
    			result =  fread(&offsets1,1,1,f1);
    			if (header1.mapsize == 0)
                    cout << values1[offsets1] << endl;
                else{
                    if (header1.mapbytes==1){ // 2 bytes
                        fseek(f1,initstep1+sizeof(struct Dindirect)+ header1.minmaxsize + offsets1*2,SEEK_SET);
                        unsigned short offf1;
                        result =  fread(&offf1,2,1,f1);
                        cout << values1[offf1] << endl;
                    }
                    if (header1.mapbytes==2){ // 1 byte
                        fseek(f1,initstep1+sizeof(struct Dindirect)+ header1.minmaxsize + offsets1,SEEK_SET);
                        unsigned char offf1;
                        result =  fread(&offf1,1,1,f1);
                        cout << values1[offf1] << endl;
                    }
                    if (header1.mapbytes==0){ //4 bytes
                        fseek(f1,initstep1+sizeof(struct Dindirect)+ header1.minmaxsize + offsets1*4,SEEK_SET);
                        unsigned int offf1;
                        result =  fread(&offf1,4,1,f1);
                        cout << values1[offf1] << endl;
                    }
                }
     			
     		}

       }
    
   
    fclose(f1);
    return 1;
}




int random_access_diff(int argc, char * argv[] ){
       
        FILE *f1;
        f1 = fopen(argv[1],"rb");
        int row = atoi(argv[3]);

        
        int fcount = 0;
        int offset = -1;
        int global_len = 0;
        /*read marker of file*/
        char marker[5];
        result =  fread( marker, 4, 1, f1);
        
        struct fileH fileheader1;
        result =  fread(&fileheader1,sizeof(struct fileH), 1, f1);
        vector <vector <int>> index1;

        int* col = new int[fileheader1.numofvals];

        long data[fileheader1.numofblocks];
        result =  fread(data,(fileheader1.numofblocks+1)*8,1,f1);
        int blocknum = row/(int)data[fileheader1.numofblocks];
        int rowid = row%(int)data[fileheader1.numofblocks];
        
        unsigned long initstep1 = data[blocknum];
        struct D header1;
        
        
        int count = 0;
        int join1 = atoi(argv[2]);
        
        int current;
        fseek(f1, initstep1 + sizeof(int)*join1, SEEK_SET);
        result =  fread(&current,sizeof(int),1,f1);
        initstep1 += current + sizeof(int)*(fileheader1.numofcols+1);
        fseek(f1,initstep1, SEEK_SET);
        result =  fread(&header1, sizeof(struct D),1,f1);
        unsigned short int previndex[header1.previndices];
        result =  fread(previndex, header1.previndices * 2, 1, f1);

   
        
        if (header1.dictsize == 0){
                    vector<string> values1;
                    char buffer1[header1.indicessize];
                    fseek(f1,initstep1+sizeof(struct D)+header1.previndices*2, SEEK_SET);
                    result =  fread(buffer1,header1.indicessize,1,f1);
                    msgpack::unpacked result1;
                    unpack(result1, buffer1, header1.indicessize);
                    result1.get().convert(values1);
                    cout << values1[rowid] << endl;
                    return 1;
                
        }
        else if (header1.diff == 0){
            int off;
              if (header1.indicessize == 4){
                fseek(f1,initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize + header1.minmaxsize + header1.minmaxdiffsize,SEEK_SET);
    			result =  fread(&off,header1.indicessize,1,f1);
              }
              else {
              if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
              if (SNAPPY){
                  fseek(f1,initstep1+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize + header1.minmaxdiffsize,SEEK_SET);
                   unsigned short offsets1[header1.numofvals];
   			       char buffer1[header1.indicessize];
   			       result =  fread(buffer1,header1.indicessize,1,f1);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       copy(&output[0], &output[0]+(sizeof(unsigned short)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		       off = offsets1[rowid];
    		    }
    		    else{
                  unsigned short offsets1;
                  fseek(f1,initstep1+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize + header1.minmaxdiffsize+rowid*2,SEEK_SET);
                  result =  fread(&offsets1,2,1,f1);
                  off = offsets1;
                  }
              }
              
              if (header1.bytes==0){ // four byte offsets
                  if (SNAPPY){
                  fseek(f1,initstep1+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize + header1.minmaxdiffsize,SEEK_SET);
                   unsigned int offsets1[header1.numofvals];
   			       char buffer1[header1.indicessize];
   			       result =  fread(buffer1,header1.indicessize,1,f1);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       copy(&output[0], &output[0]+(sizeof(unsigned int)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		       off = offsets1[rowid];
    		    }
    		    else{
                  unsigned int offsets1;
                  fseek(f1,initstep1+sizeof(struct D)+header1.dictsize +header1.previndices*2+ header1.minmaxsize + header1.minmaxdiffsize+rowid*4,SEEK_SET);
                  result =  fread(&offsets1,4,1,f1);
                  off = offsets1;
                  }
              }
              if (header1.bytes==2){ // one byte offsets
              if (SNAPPY){
              fseek(f1,initstep1+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize + header1.minmaxdiffsize,SEEK_SET);
                   unsigned char offsets1[header1.numofvals];
   			       char buffer1[header1.indicessize];
   			       result =  fread(buffer1,header1.indicessize,1,f1);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       copy(&output[0], &output[0]+(sizeof(unsigned char)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		       off = offsets1[rowid];
    		    }
    		    else{
                  unsigned char offsets1;
                  fseek(f1,initstep1+sizeof(struct D)+header1.dictsize +header1.previndices*2+ header1.minmaxsize + header1.minmaxdiffsize+rowid,SEEK_SET);
                  result =  fread(&offsets1,1,1,f1);
                  off = offsets1;
                  }
              }
                }
        
    int temp = 0;
    int prevtemp = 0;
    int rightblock = 0;
    int found = 0;
    int position_in_block;
    int relative_block_num = 0;
    for (int co = 1; co < header1.previndices; co+=2){
        prevtemp = temp;
        temp += previndex[co];

        if (temp <= off)
            continue;
        else {
            found = 1;
            position_in_block = off - prevtemp;
            rightblock = previndex[co-1];
            break;
        }
    }
        /*if (header1.previndices == 0){ //i am in the first block
                found = 1;
                position_in_block = off;
                
            }*/
        if (found == 0){
            position_in_block = off - temp;
            rightblock = blocknum;
        }
        if (blocknum != rightblock){
            unsigned long initstep2 = data[rightblock];
            struct D header2;
            int current2;
            fseek(f1, initstep2 + sizeof(int)*join1, SEEK_SET);
            result =  fread(&current2,sizeof(int),1,f1);
            initstep2 += current2 + sizeof(int)*(fileheader1.numofcols+1);
            fseek(f1,initstep2, SEEK_SET);
            result =  fread(&header2, sizeof(struct D),1,f1);
            vector<string> values1;
            fseek(f1,initstep2+sizeof(struct D)+header2.minmaxsize+header2.minmaxdiffsize+ header2.previndices*2,SEEK_SET);
            if (SNAPPY){
   			             char buffer1[header2.dictsize];
    		             result =  fread(buffer1,header2.dictsize,1,f1);
    		             string output;
    		             snappy::Uncompress(buffer1, header2.dictsize, &output);
    		             msgpack::unpacked result1;
    		             unpack(result1, output.data(), output.size());
    		             result1.get().convert(values1);
    		}
            else{
                char buffer1[header2.dictsize];
                result =  fread(buffer1,header2.dictsize,1,f1);
                msgpack::unpacked result1;
                unpack(result1, buffer1, header2.dictsize);
                result1.get().convert(values1);
            }
            cout << values1[position_in_block] << endl;
        }
        else {
            
            fseek(f1,initstep1+sizeof(struct D)+header1.minmaxsize+header1.minmaxdiffsize+ header1.previndices*2,SEEK_SET);
            vector<string> values1;
            if (SNAPPY){
   			             char buffer1[header1.dictsize];
    		             result =  fread(buffer1,header1.dictsize,1,f1);
    		             string output;
    		             snappy::Uncompress(buffer1, header1.dictsize, &output);
    		             msgpack::unpacked result1;
    		             unpack(result1, output.data(), output.size());
    		             result1.get().convert(values1);
    		}
            else{
                 char buffer1[header1.dictsize];
                 result =  fread(buffer1,header1.dictsize,1,f1);
                 msgpack::unpacked result1;
                 unpack(result1, buffer1, header1.dictsize);
                 result1.get().convert(values1);
            }
            cout << values1[position_in_block] << endl;
        }
   // cout <<  position_in_block << " "<< rightblock << endl;
        
        }
    
        else if (header1.diff == 1){
            int off;
            if (header1.indicessize == 4){
                fseek(f1,initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize + header1.minmaxsize + header1.minmaxdiffsize,SEEK_SET);
    			result =  fread(&off,header1.indicessize,1,f1);
              } else {
            if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
            if (SNAPPY){
            fseek(f1,initstep1+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize + header1.minmaxdiffsize,SEEK_SET);
                   unsigned short offsets1[header1.numofvals];
   			       char buffer1[header1.indicessize];
   			       result =  fread(buffer1,header1.indicessize,1,f1);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       copy(&output[0], &output[0]+(sizeof(unsigned short)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		       off = offsets1[rowid];
    		    }
    		    else{
                unsigned short offsets1;
                fseek(f1,initstep1+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize + header1.minmaxdiffsize+rowid*2,SEEK_SET);
                result =  fread(&offsets1,2,1,f1);
                off = offsets1;
                }
            }
            
            if (header1.bytes==0){ // four byte offsets
            if (SNAPPY){
            fseek(f1,initstep1+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize + header1.minmaxdiffsize,SEEK_SET);
                   unsigned int offsets1[header1.numofvals];
   			       char buffer1[header1.indicessize];
   			       result =  fread(buffer1,header1.indicessize,1,f1);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       copy(&output[0], &output[0]+(sizeof(unsigned int)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		       off = offsets1[rowid];
    		    }
    		    else{
                unsigned int offsets1;
                fseek(f1,initstep1+sizeof(struct D)+header1.dictsize +header1.previndices*2+ header1.minmaxsize + header1.minmaxdiffsize+rowid*4,SEEK_SET);
                result =  fread(&offsets1,4,1,f1);
                off = offsets1;
                }
            }
            if (header1.bytes==2){ // one byte offsets
            if (SNAPPY){
            fseek(f1,initstep1+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize + header1.minmaxdiffsize,SEEK_SET);
                   unsigned char offsets1[header1.numofvals];
   			       char buffer1[header1.indicessize];
   			       result =  fread(buffer1,header1.indicessize,1,f1);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       copy(&output[0], &output[0]+(sizeof(unsigned char)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		       off = offsets1[rowid];
    		    }
    		    else{
                unsigned char offsets1;
                fseek(f1,initstep1+sizeof(struct D)+header1.dictsize +header1.previndices*2+ header1.minmaxsize + header1.minmaxdiffsize+rowid,SEEK_SET);
                result =  fread(&offsets1,1,1,f1);
                off = offsets1;
                }
            }
            }
            fseek(f1,initstep1+sizeof(struct D)+header1.minmaxsize+ header1.previndices*2,SEEK_SET);
            vector<string> values1;
             if (SNAPPY){
   			             char buffer1[header1.dictsize];
    		             result =  fread(buffer1,header1.dictsize,1,f1);
    		             string output;
    		             snappy::Uncompress(buffer1, header1.dictsize, &output);
    		             msgpack::unpacked result1;
    		             unpack(result1, output.data(), output.size());
    		             result1.get().convert(values1);
    		}
            else{
                char buffer1[header1.dictsize];
                result =  fread(buffer1,header1.dictsize,1,f1);
                msgpack::unpacked result1;
                unpack(result1, buffer1, header1.dictsize);
                result1.get().convert(values1);
            }
            cout << values1[off] << endl;
            
            
        }
        fclose(f1);
        return 0;
    }




int read_diff_filt(int argc, char * argv[] ){
     struct timeval begin = {0,0};
     struct timeval end = {0,0};
     struct timeval beginf = {0,0};
     struct timeval endf = {0,0};
 
    gettimeofday(&beginf, NULL);
    std::clock_t start;
    int64_t totalduration = 0;
    int64_t tduration = 0;
    int ommits = 0;
    int ommits2 = 0;
    int boolminmax = atoi(argv[4]); 
    int boolminmax_diff =  atoi(argv[5]);
    int totalcount1=0;
    FILE *f1;
    f1 = fopen(argv[1],"rb");
    string value = argv[3];
    vector<string> parquetvalues1;
    int64_t ioduration = 0;
    double ofduration = 0.0;
    double dictduration = 0.0;
    double processingduration = 0.0;
    int fcount = 0;
    int offset = -1;
    int global_len = 0;
    /*read marker of file*/
    
    char marker[5];
    gettimeofday(&begin, NULL);
    			    
    result =  fread( marker, 4, 1, f1);
    
    struct fileH fileheader1;
    result =  fread(&fileheader1,sizeof(struct fileH), 1, f1);
    vector <vector <int>> index1;
    
    int* col = new int[fileheader1.numofvals];
    
    long data[fileheader1.numofblocks];
    processingduration += (std::clock() - start) / (double) CLOCKS_PER_SEC;
    result =  fread(data,(fileheader1.numofblocks+1)*8,1,f1);
    gettimeofday(&end, NULL);
    ioduration += end.tv_usec-begin.tv_usec;
	unsigned long initstep1 = 4 + sizeof(struct fileH)+(fileheader1.numofblocks+1)*8;
	struct D header1;
	
	
	int count = 0;
	int join1 = atoi(argv[2]);
	
	
 	while (totalcount1 < fileheader1.numofvals){
 	        
   		    int current, next;
   		    gettimeofday(&begin, NULL);
   		    fseek(f1, initstep1 + sizeof(int)*join1, SEEK_SET);
   		    result =  fread(&current,sizeof(int),1,f1);
   		    fseek(f1, initstep1 + sizeof(int)*(fileheader1.numofcols), SEEK_SET);
   		    result =  fread(&next,sizeof(int),1,f1);
            initstep1 += current + sizeof(int)*(fileheader1.numofcols+1);
            fseek(f1,initstep1, SEEK_SET);
   		    result =  fread(&header1, sizeof(struct D),1,f1);
   		    totalcount1 += header1.numofvals;
   		    gettimeofday(&end, NULL);
            ioduration += end.tv_usec-begin.tv_usec;
   		    if (header1.dictsize == 0){
   		        //    parquetvalues1.reserve(fileheader1.numofvals);
    		    char buffer1[header1.indicessize];
    		    fseek(f1,initstep1+sizeof(struct D)+header1.previndices*2, SEEK_SET);
    		    double duration = 0.0;
    		    uLong uncomprLen;
    		    result =  fread(&uncomprLen,4,1,f1);
    			result =  fread(buffer1,header1.indicessize,1,f1);
    			
    			
    			Byte* uncompr;
    			uncompr = (Byte*)calloc((uInt)uncomprLen, 1);
    			uncompress(uncompr, &uncomprLen, (Byte*)buffer1, (uLong)header1.indicessize);
    			msgpack::unpacked result1;
    			unpack(result1, (const char*)uncompr, uncomprLen);
    			vector<string> values1;
    			result1.get().convert(values1);
    			for (string i: values1){
    			    if (value.compare(i)==0){
    			        col[fcount] = count;
    			        fcount++;
    			    }
    			    count++;
    			}
    			//parquetvalues1.insert( parquetvalues1.end(), values1.begin(), values1.end() );
   				//totalcount1 += header1.numofvals;
   				initstep1 += next-current;
    		}
    		else {
    		int check = 0;
    		if (header1.diff == 1){  global_len = 0;}
    		if(offset  == -1 or header1.diff == 1){
    		 int temp = global_len;
    		 global_len += header1.lendiff;
   			 char minmaxbuf[header1.minmaxsize];
   			 gettimeofday(&begin, NULL);
   			 fseek(f1,initstep1+sizeof(struct D)+header1.previndices*2, SEEK_SET);
    		 result =  fread(minmaxbuf,header1.minmaxsize,1,f1);
    		 fseek(f1,initstep1+sizeof(struct D)+header1.previndices*2 + header1.minmaxsize + header1.minmaxdiffsize, SEEK_SET);
    		 gettimeofday(&end, NULL);
             ioduration += end.tv_usec-begin.tv_usec;
    		 msgpack::unpacked minmax;
    		 unpack(minmax, minmaxbuf, header1.minmaxsize);
    		 vector<string> minmax1;
    		 minmax.get().convert(minmax1);
    		 vector<string> values1;

   			 if (header1.lendiff > 0){
   			   if (header1.diff == 0){
    	     
    		    if (boolminmax_diff){
    		        char minmaxdiffbuf[header1.minmaxdiffsize];
    		        gettimeofday(&begin, NULL);
    		        fseek(f1,initstep1+sizeof(struct D)+header1.previndices*2 + header1.minmaxsize, SEEK_SET);
    		        result =  fread(minmaxdiffbuf,header1.minmaxdiffsize,1,f1);
    		        gettimeofday(&end, NULL);
                    ioduration += end.tv_usec-begin.tv_usec;
    		        msgpack::unpacked minmaxdiff;
    		        unpack(minmaxdiff, minmaxdiffbuf, header1.minmaxdiffsize);
    		        vector<string> minmax1diff;
    		        minmaxdiff.get().convert(minmax1diff);

                start = std::clock();
    			if (value.compare(minmax1diff[0])<0 or value.compare(minmax1diff[1])>0){
   		      		count += header1.numofvals;
   		      		//cout << "kaka" << endl;
   		      		//global_len = global_len + header1.lendiff;
   		      		initstep1 += next-current;
   		      		ommits++;
   		      		offset = -1;
   		      	processingduration += (std::clock() - start) / (double) CLOCKS_PER_SEC;
   		      	continue;
   		      	}
   		    	} 
   		    	if (SNAPPY){
   			  char buffer1[header1.dictsize];
    		  result =  fread(buffer1,header1.dictsize,1,f1);
    		  string output;
    		  snappy::Uncompress(buffer1, header1.dictsize, &output);
    		  msgpack::unpacked result1;
    		  unpack(result1, output.data(), output.size());
    		  result1.get().convert(values1);
    		}
    		else {
   			   char buffer1[header1.dictsize];
   			   gettimeofday(&begin, NULL);
   			   
   			    result =  fread(buffer1,header1.dictsize,1,f1);
   			    gettimeofday(&end, NULL);
    			ioduration += end.tv_usec-begin.tv_usec;
    		    msgpack::unpacked result1;
    		    unpack(result1,  buffer1, header1.dictsize);
    		    result1.get().convert(values1);
   
    		    }
   		    	}
   		    else {
   		        check = 1;
   		        start = std::clock();
   		    	if (boolminmax and (value.compare(minmax1[0])<0 or value.compare(minmax1[1])>0)){
   		    	    ommits++;
   		      		count += header1.numofvals;
   		      		//global_len = global_len + header1.lendiff;
   		      		initstep1 += next-current;
   		      		offset = -1;
   		        processingduration += (std::clock() - start) / (double) CLOCKS_PER_SEC;
   		      	continue;
   		    	}
   		    	else {
   		    	if (SNAPPY){
   			  char buffer1[header1.dictsize];
    		  result =  fread(buffer1,header1.dictsize,1,f1);
    		  string output;
    		  snappy::Uncompress(buffer1, header1.dictsize, &output);
    		  msgpack::unpacked result1;
    		  unpack(result1, output.data(), output.size());
    		  result1.get().convert(values1);
    		}
    		else {
   		    	char buffer1[header1.dictsize];
   		    	 gettimeofday(&begin, NULL);
    			
    			
   			    result =  fread(buffer1,header1.dictsize,1,f1);
   			    gettimeofday(&end, NULL);
    			ioduration += end.tv_usec-begin.tv_usec;
    		    msgpack::unpacked result1;
    		    unpack(result1, buffer1, header1.dictsize);
    		    
    		    result1.get().convert(values1);
    		    
    		    }
   		    	}
   		    	}
   		    }
             start = std::clock();
    		 for (int k = 0; k < values1.size(); k++){
   			   if (value == values1[k]){
   			     offset = k + temp;
   			     break;
   			     }
   			     offset = -1;
   			 }
   			 
   		    processingduration += (std::clock() - start) / (double) CLOCKS_PER_SEC;
             if (offset == -1){
                //if (header1.lendiff == 0)
                    ommits2++;
                count += header1.numofvals;
                initstep1 += next-current;
                continue;
             }
            
   			}
   			if (!check){
   			 char minmaxbuf[header1.minmaxsize];
   			 gettimeofday(&begin, NULL);
   
   			 fseek(f1,initstep1+sizeof(struct D)+header1.previndices*2, SEEK_SET);
    		 result =  fread(minmaxbuf,header1.minmaxsize,1,f1);
    		  gettimeofday(&end, NULL);
    ioduration += end.tv_usec-begin.tv_usec;
    		 msgpack::unpacked minmax;
    		 
    		 unpack(minmax, minmaxbuf, header1.minmaxsize);
    		 vector<string> minmax1;

    		 minmax.get().convert(minmax1);
    		 
    		 if (boolminmax)
    		 if (value.compare(minmax1[0])<0 or value.compare(minmax1[1])>0){
    		     start = std::clock();
    		     ommits++;
    		     count += header1.numofvals;
   		      	 initstep1 += next-current;
   		      	 processingduration += (std::clock() - start) / (double) CLOCKS_PER_SEC;
    		     continue;
    		 }
   			}
   			 
   			if (header1.indicessize == 4){
   			    
                int offf;
                 gettimeofday(&begin, NULL);
          
                fseek(f1,initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize + header1.minmaxsize + header1.minmaxdiffsize,SEEK_SET);
    			result =  fread(&offf,header1.indicessize,1,f1);
    			 gettimeofday(&end, NULL);
    ioduration += end.tv_usec-begin.tv_usec;
    			start = std::clock();
                if (offf == offset)
                for (int i=0; i < header1.numofvals; i++){
     		    	col[fcount] = count;
     		    	fcount++;
     		    	count++;
     		    }
     		    else {count += header1.numofvals;}
     			initstep1 += next-current;
     			processingduration += (std::clock() - start) / (double) CLOCKS_PER_SEC;
            
        }
        else{
    		if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			 gettimeofday(&begin, NULL);
     			fseek(f1,initstep1+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize + header1.minmaxdiffsize, SEEK_SET);

     			if (SNAPPY){
   			       char buffer1[header1.indicessize];
   			        gettimeofday(&begin, NULL);
    			    result =  fread(buffer1,header1.indicessize,1,f1);
    			    gettimeofday(&end, NULL);
    			    ioduration += end.tv_usec-begin.tv_usec;
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       
    		       copy(&output[0], &output[0]+(sizeof(unsigned short)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		    }
    		    else{ 
    		   
    		   
    			result =  fread(offsets1,header1.indicessize,1,f1);
    			
    			
    			}
    			gettimeofday(&end, NULL);
    			ioduration += end.tv_usec-begin.tv_usec;
    			start = std::clock();
				for (int i=0; i < header1.numofvals; i++){
     		    
     		    if (offsets1[i] == offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		    }
     		    processingduration += (std::clock() - start) / (double) CLOCKS_PER_SEC;
     			initstep1 += next-current;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			gettimeofday(&begin, NULL);
     			fseek(f1,initstep1+sizeof(struct D)+header1.dictsize +header1.previndices*2+ header1.minmaxsize + header1.minmaxdiffsize,SEEK_SET);
     			if (SNAPPY){
   			       char buffer1[header1.indicessize];
   			       result =  fread(buffer1,header1.indicessize,1,f1);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       
    		       copy(&output[0], &output[0]+(sizeof(unsigned int)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		    }
    		    else {
    			result =  fread(offsets1,header1.indicessize,1,f1);
    			
    		    
    		    }
    		    gettimeofday(&end, NULL);
    			ioduration += end.tv_usec-begin.tv_usec;
    		    start = std::clock();
				for (int i=0; i < header1.numofvals; i++){
     		    
     		    if (offsets1[i] == offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     		processingduration += (std::clock() - start) / (double) CLOCKS_PER_SEC;
     			initstep1 += next-current ;
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			gettimeofday(&begin, NULL);
     			fseek(f1,initstep1+sizeof(struct D)+header1.dictsize +header1.previndices*2+ header1.minmaxsize + header1.minmaxdiffsize,SEEK_SET);
     			if (SNAPPY){
   			       char buffer1[header1.indicessize];
   			       result =  fread(buffer1,header1.indicessize,1,f1);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       
    		       copy(&output[0], &output[0]+(sizeof(unsigned char)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		    }
    		    else {
    		    
    			result =  fread(offsets1,header1.indicessize,1,f1);

    		    
    		    }
    		    gettimeofday(&end, NULL);
    			ioduration += end.tv_usec-begin.tv_usec;
    		    start = std::clock();
     			for (int i=0; i < header1.numofvals; i++){
     		    
     		    if (offsets1[i] == offset){
     		        //col[fcount].Aacccerowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     		processingduration += (std::clock() - start) / (double) CLOCKS_PER_SEC;
     			initstep1 += next-current ;
     		}
     		}
		}
	}
	fclose(f1);
	if(-1 == gettimeofday(&endf,NULL)) {
     perror("gettimeofday");
     return -1;
   }
	tduration = endf.tv_usec - beginf.tv_usec;
   cout <<  "result count: " << fcount << " " << " blocks omitted: " << ommits << " offset scan omitted: "<< ommits2 << " io time: "<< ioduration << " total_time: "<< tduration  <<endl;
     
    return 0;
}







int main(int argc, char * argv[] ){
  if (strstr(argv[1], "snappy"))
      SNAPPY = 1;
  if (argc == 3){
      if (strstr(argv[1], "diff"))
         return read_diff_materialize(argc,argv);
      if (strstr(argv[1], "glob"))
         return read_glob_mater(argc,argv);
      if (strstr(argv[1], "indirect"))
         return read_indirect_mater(argc,argv);
      
      }
  if (argc == 4){
      if (strstr(argv[1], "diff"))
         return random_access_diff(argc,argv);
      if (strstr(argv[1], "glob"))
         return random_access_glob(argc,argv);
      if (strstr(argv[1], "indirect"))
         return random_access_indirect(argc,argv);
  }
  if (argc == 6){
      if (strstr(argv[1], "diff"))
         return read_diff_filt(argc,argv);
      if (strstr(argv[1], "glob"))
         return read_glob_filt(argc,argv);
      if (strstr(argv[1], "indirect"))
         return read_indirect_filt(argc,argv);
     
      }

}
