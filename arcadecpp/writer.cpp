#include <numeric>
#include <fstream>
#include "bloom/bloom_filter.hpp"
#include "arcade.h"
#include <thread>
#include "msgpack.hpp"
#include <zlib.h>
using namespace std;
using namespace Arcade;

#define CHECK_ERR(err, msg) { \
    if (err != Z_OK) { \
        fprintf(stderr, "%s error: %d\n", msg, err); \
        exit(1); \
    } \
}
using namespace std;
using namespace Arcade;

int extractColumn(vector<std::string> &dataset, uint64_t colIndex, vector<std::string> &column) {
    char gDelimiter = ',';
    for (int i = 0; i < dataset.size(); i++) {
        uint64_t col = 0;
        int start = 0;
        int end = dataset[i].find(gDelimiter);
        while (col < colIndex && end != std::string::npos) {
            start = end + 1;
            end = dataset[i].find(gDelimiter, start);
            if (end == std::string::npos)
                end = dataset[i].size() - 1;
            ++col;
        }
        column[i] = (col == colIndex ? dataset[i].substr(start, end - start) : "");

    }

    return 1;
}


template<class InputIt1, class OutputIt>
OutputIt calc_diff(InputIt1 first1, InputIt1 last1,
                   unordered_map<string, int> &glob,
                   OutputIt d_first) {
    while (first1 != last1) {
        if (glob.find(*first1) == glob.end()) {
            *d_first++ = *first1++;
        }
        else
            *first1++;
    }
    return d_first;
}





/*TODO refactor optimise this function, this can be much faster */
int compress_batch(vector<string> &vals, FILE *f1, bloom_filter *filter, bool &isdictionary,vector<int> &blocksize, vector<int> &sizediff,
                   unordered_map<string, int> &lookup,
                   vector<short> &diffvals, int blocknum, int BLOCKSIZE, bool SNAPPY, unsigned long &global_dict_memory, int &globaldictlen,
                   int &permanent_decision, double &duration5, char* costf, char* dthreshold) {
                   
    
    
    int CACHE_SIZE = 8192000;
    
    bool shortvals = 0;
    struct D header;
    header.numofvals = vals.size();
    header.minmaxdiffsize = 0;
    vector<string> minmax(2);
    vector<string> minmaxdiff(2);
    vector<string> vec = vals;
    
    int kk = 0;
    int ss = 0;
    for (int k=0; k<vec.size(); k++){
        ss += vec[k].size();
        kk++;
        if (kk >= vec.size()*10/100) break;
    }
    
    int avgl = 0;
    if (kk>0)
    avgl = ss/kk;
    if (avgl<20) shortvals = 1;
    
    std::sort(vec.begin(), vec.end());
    vec.erase(std::unique(vec.begin(), vec.end()), vec.end());
    
    int distinct_count = vec.size();
    minmax[0] = vec[0];
    minmax[1] = vec[distinct_count - 1];
    int diffdict = 1;
    if (!strcmp(dthreshold,"DISABLE_THRESHOLD"))
        diffdict  = 1;
    else if (vec.size() * 1.0 / header.numofvals > 0.80 or ((!strcmp(costf,"LD")) and vec.size() * 1.0 / header.numofvals > 0.50)) {
        isdictionary = false;
        header.dictsize = 0;
        header.previndices = 0;
        sizediff.clear();
        diffvals.clear();
        lookup.clear();
        std::stringstream buffermm;
        msgpack::pack(buffermm, minmax);
        string stmm = buffermm.str();
        header.minmaxsize = 0;//stmm.size();
        std::stringstream buffermm1;
        msgpack::pack(buffermm1, vals);
        string st = buffermm1.str();
        header.indicessize = st.size();
        header.lendiff = 0;
        header.bytes = 0; //TODO perhaps edit this and read to support minmax to parquet pages
        Byte *buff1;
        uLong ss = header.indicessize;
        int uncompressed = (int)ss;
        buff1 = (Byte*)calloc((uInt)ss, 1);
        int err;
        err = compress(buff1, &ss, (const Bytef*)st.c_str(), header.indicessize);
        CHECK_ERR(err, "compress");
        //fwrite(&stmm[0], header.minmaxsize ,1 , f1 );
        header.indicessize = int(ss);
        fwrite(&header, sizeof(header), 1, f1);
        fwrite(&uncompressed, 4, 1, f1);
        fwrite(buff1, ss, 1, f1);

        return 1;
    }
    
    
    if  (!strcmp(costf,"LD"))
        diffdict = 0;
    int globdsize = lookup.size();
    
    if ((global_dict_memory > CACHE_SIZE*2 and !strcmp(costf,"CF")) or globdsize == 0){
        diffdict = 0;
    }
    
    if ((!strcmp(costf,"CD") and (global_dict_memory > CACHE_SIZE or (globdsize>200000 and shortvals)))  ) {diffdict = 0;}
//    if ((global_dict_memory > CACHE_SIZE and !strcmp(costf,"CD")) or globdsize == 0){
//        diffdict = 0;
//    }
    
    vector<string> diff;

    if (permanent_decision == 1 and diffdict == 1) {

        calc_diff(vec.begin(), vec.end(), lookup, std::inserter(diff, diff.begin()));
        if ((diff.size() * 1.0) / distinct_count > 0.9 and globdsize > 0) {
            permanent_decision = 0;
        }
    }
    if (diff.size() > 0) {
        minmaxdiff[0] = diff[0];
        minmaxdiff[1] = diff[diff.size() - 1];
    }
    else {
        minmaxdiff[0] = " ";
        minmaxdiff[1] = " ";
    }

    string st = "";
    string stloc = "";


    std::clock_t start;
    start = std::clock();
    int DF = 0;
    
    if ((!strcmp(costf,"CD") and (global_dict_memory > CACHE_SIZE or (globdsize>200000 and shortvals)))  ) {diffdict = 0;}
    else if (strcmp(costf,"CD") and permanent_decision == 1 and diffdict == 1 and DF == 0 ) {
        if (globdsize > 0 and diff.size() * 1.0 / distinct_count > 0.99) diffdict = 0;

        else if ((globdsize >= 256 and distinct_count < 256) or (globdsize >= 65536 and distinct_count < 65536) or
                 (globdsize < 256 and (globdsize + diff.size()) > 255) or
                 (globdsize < 65536 and (globdsize + diff.size()) > 65535)) {
            if  (!strcmp(costf,"OF"))
                diffdict = 0;
            else {
            std::stringstream buffer;
            msgpack::pack(buffer, diff);
            st = buffer.str();
            global_dict_memory += st.size();
            int diffdictdump = st.size();
            int diffcount = sizediff.size();
            int diffavg = 0;
            if (diffcount > 0)
                diffavg = accumulate(sizediff.begin(), sizediff.end(), 0) / diffcount;
            else
                diffavg = diffdictdump;

            int lenvals3 = header.numofvals;
            int locs = 0;
            if (distinct_count < 256) locs = 1;
            else if (distinct_count < 65536) locs = 2;
            else locs = 4;
            int diffs = 0;
            if (globdsize + diff.size() < 256) diffs = 1;
            else if (globdsize + diff.size() < 65536) diffs = 2;
            else diffs = 4;

            std::stringstream bufferloc;
            msgpack::pack(bufferloc, vec);
            stloc = bufferloc.str();
            //stloc = hps::to_string(vec);
            int sizelocal = stloc.size() + lenvals3 * locs;
            int sizeofdiff = diffdictdump + lenvals3 * diffs;
             if  (!strcmp(costf,"CT")){
                    if (sizelocal<sizeofdiff){
                        diffdict = 0;
                        }
            }
            else {
            int pblocks = ((CACHE_SIZE*2 - global_dict_memory) / (diffdictdump));
            if (diffcount <= 1){
                if (sizelocal < sizeofdiff)
                    diffdict  = 0;
            }																																	
            else if (pblocks * (diffs * BLOCKSIZE) -  ceil(pblocks*1.0/(diffcount)) * (accumulate(blocksize.begin(), blocksize.end(), 0)) > 0) {
            //cout << pblocks*(diffs*BLOCKSIZE) + sizeofdiff  << " " << (pblocks/diffcount)*((diffcount+pblocks%diffcount)*(locs*BLOCKSIZE + diffavg)+ sizelocal) << endl;
            //if (pblocks*(diffs*BLOCKSIZE) + sizeofdiff - (pblocks/diffcount)*((diffcount+pblocks%diffcount)*(locs*BLOCKSIZE + diffavg) + sizelocal) > 0){
                    diffdict = 0;
            }
          }
        }
        }
        
    }else if (permanent_decision == 0) diffdict = 0;
     if (!strcmp(costf,"DF") and globdsize > 0) {diffdict = 1; DF = 1;}

    duration5 += (std::clock() - start) / (double) CLOCKS_PER_SEC;

    if (diffdict == 1) {
        globaldictlen += diff.size();
        //globaldict.insert(globaldict.end(), diff.begin(), diff.end());

        int value = 0;
        int c = 0;
        int glsize = globdsize+diff.size();
        for (int index = globdsize; index < glsize; ++index)
            lookup[diff[c++]] = index;
        std::stringstream buffermm;
        msgpack::pack(buffermm, minmax);
        string stmm = buffermm.str();
        header.minmaxsize = stmm.size();
        
        
        std::stringstream bufferdmm;
        msgpack::pack(bufferdmm, minmaxdiff);
        string stmm1 = bufferdmm.str();
        header.minmaxdiffsize = stmm1.size();

        string comprst;
         if (st == "") {
            std::stringstream buff;
            msgpack::pack(buff, diff);
            st = buff.str();
            global_dict_memory += st.size();
            //stloc = hps::to_string(vec);
        }
        header.dictsize = st.size();
        //global_dict_memory += st.size();
        sizediff.push_back(st.size());
        globdsize += diff.size();
        header.lendiff = diff.size();

        diffvals.push_back(blocknum);
        diffvals.push_back(header.lendiff);
        short *a = &diffvals[0];
        header.previndices = diffvals.size();
        header.diff = 0; // i am in diff dict
        if (globdsize < 256) {
            char offsets[header.numofvals];
            int k = 0;
            for (string& i: vals) {
                offsets[k++] = lookup[i];
            }
            header.bytes = 2;
            header.indicessize = header.numofvals * sizeof(char);
            fwrite(&header, sizeof(header), 1, f1);
            fwrite(a, diffvals.size() * sizeof(short), 1, f1);
            fwrite(&stmm[0], header.minmaxsize, 1, f1);
            fwrite(&stmm1[0], header.minmaxdiffsize, 1, f1);
            fwrite(&st[0], header.dictsize, 1, f1);
            fwrite(&offsets, sizeof(char), header.numofvals, f1);
            blocksize.push_back(st.size()+header.numofvals*1);
        }
        else if (globdsize < 65536) {
            unsigned short offsets[header.numofvals];
            int k = 0;
            for (string& i: vals) {
                offsets[k++] = lookup[i];
            }
            header.bytes = 1;
            header.indicessize = header.numofvals * sizeof(unsigned short);
            fwrite(&header, sizeof(header), 1, f1);
            fwrite(a, diffvals.size() * sizeof(short), 1, f1);
            fwrite(&stmm[0], header.minmaxsize, 1, f1);
            fwrite(&stmm1[0], header.minmaxdiffsize, 1, f1);
            fwrite(&st[0], header.dictsize, 1, f1);
            fwrite(&offsets, sizeof(unsigned short), header.numofvals, f1);
            blocksize.push_back(st.size()+header.numofvals*2);
        }
        if (globdsize > 65536) {
            unsigned int offsets[header.numofvals];
            int k = 0;
            for (string& i: vals) {
                offsets[k++] = lookup[i];
            }
            header.bytes = 0;
            header.indicessize = header.numofvals * sizeof(unsigned int);
            fwrite(&header, sizeof(header), 1, f1);
            fwrite(a, diffvals.size() * sizeof(short), 1, f1);
            fwrite(&stmm[0], header.minmaxsize, 1, f1);
            fwrite(&stmm1[0], header.minmaxdiffsize, 1, f1);
            fwrite(&st[0], header.dictsize, 1, f1);
            fwrite(&offsets, sizeof(unsigned int), header.numofvals, f1);
            blocksize.push_back(st.size()+header.numofvals*4);
        }
    }

    else if (diffdict == 0) {
        globaldictlen = vec.size();
        if (permanent_decision == 1) {
            sizediff.clear();
            blocksize.clear();
            diffvals.clear();
            lookup.clear();
            global_dict_memory = 0;
        }

        if (permanent_decision == 0) {
            bloom_parameters parameters;
            parameters.projected_element_count = 55000;
            parameters.false_positive_probability = 0.1;
            parameters.random_seed = 0xA5A5A5A5;
            parameters.compute_optimal_parameters();
            bloom_filter filternew(parameters);
            int count = 0;
            for (string& val: vec) {
                filternew.insert(val);
                if ((*filter).contains(val))
                    count++;
            }
            if ((count * 1.0) / vec.size() > 0.2)
                permanent_decision = 1;
            *filter = filternew;

        }

        int value = 0;
        for (int index = 0; index < vec.size(); ++index)
            lookup[vec[index]] = index;

        std::stringstream buffermm;
        msgpack::pack(buffermm, minmax);
        string stmm = buffermm.str();
        header.minmaxsize = stmm.size();

        string comprst;
        
        if (stloc == "") {
            std::stringstream buff;
            msgpack::pack(buff, vec);
            stloc = buff.str();
            //stloc = hps::to_string(vec);
        }
        
        header.dictsize = stloc.size();
        global_dict_memory += stloc.size();
        header.lendiff = distinct_count;

        header.diff = 1;

        diffvals.push_back(blocknum);
        diffvals.push_back(header.lendiff);
        header.previndices = diffvals.size();
        short *a = &diffvals[0];


        if (vec.size() < 256) {
            char offsets[header.numofvals];
            int k = 0;
            for (string& i: vals) {
                offsets[k++] = lookup[i];
            }
            header.bytes = 2;
            header.indicessize = header.numofvals * sizeof(char);
            fwrite(&header, sizeof(header), 1, f1);
            fwrite(a, diffvals.size() * sizeof(short), 1, f1);
            fwrite(&stmm[0], header.minmaxsize, 1, f1);
            fwrite(&stloc[0], header.dictsize, 1, f1);
            blocksize.push_back(stloc.size()+header.numofvals*1);
            fwrite(&offsets, sizeof(char), header.numofvals, f1);
        }
        else if (vec.size() < 65536) {
            unsigned short offsets[vals.size()];
            int k = 0;
            for (string& i: vals) {
                offsets[k++] = lookup[i];
            }
            header.bytes = 1;
            header.indicessize = header.numofvals * sizeof(unsigned short);
            fwrite(&header, sizeof(header), 1, f1);
            fwrite(a, diffvals.size() * sizeof(short), 1, f1);
            fwrite(&stmm[0], header.minmaxsize, 1, f1);
            fwrite(&stloc[0], header.dictsize, 1, f1);
            blocksize.push_back(stloc.size()+header.numofvals*2);
            fwrite(&offsets, sizeof(unsigned short), header.numofvals, f1);
        }
        if (vec.size() > 65536) {
            unsigned int offsets[header.numofvals];
            int k = 0;
            for (string& i: vals) {
                offsets[k++] = lookup[i];
            }
            header.bytes = 0;
            header.indicessize = header.numofvals * sizeof(unsigned int);
            fwrite(&header, sizeof(header), 1, f1);
            fwrite(a, diffvals.size() * sizeof(short), 1, f1);
            fwrite(&stmm[0], header.minmaxsize, 1, f1);
            fwrite(&stloc[0], header.dictsize, 1, f1);
            blocksize.push_back(stloc.size()+header.numofvals*4);
            fwrite(&offsets, sizeof(unsigned int), header.numofvals, f1);
        }


    }
    return 1;

}


int ArcadeWriter::compress(char *infile, char *outfile, int startp, int numofvals, int *retcols, int colnum, char* costf, char* dthreshold) {
    vector<int> mcolumns(retcols, retcols + colnum);

    int COLNUM = mcolumns.size();
    vector <int> permanent_decision(COLNUM);
    for (int i=0; i<COLNUM; i++)
        permanent_decision[i] = 1;
    double duration5 = 0.0;
    vector<unsigned long> global_dict_memory(COLNUM);
    vector<vector<int>> sizediff(COLNUM);
    vector<vector<int>> blocksize(COLNUM);
    vector<int> globaldictlen(COLNUM);
    //vector<vector <string>> glob(COLNUM);
    vector<unordered_map<string, int>> lookup(COLNUM);
    vector<vector<short>> diffvals(COLNUM);
    bool isdictionary = true;
    bloom_parameters parameters;
    parameters.projected_element_count = 55000;
    parameters.false_positive_probability = 0.1;
    parameters.random_seed = 0xA5A5A5A5;
    parameters.compute_optimal_parameters();

    //Instantiate Bloom Filter
    bloom_filter filter(parameters);

    int blocknum = 0;
    std::string input;
    input = infile;
    std::clock_t start;
    double duration = 0;
    SNAPPY = 0;

    std::ifstream finput(input.c_str());

    FILE *f1;
    int columnindexes[COLNUM + 1];
    memset(columnindexes, 0, COLNUM + 1 * sizeof(int));
    f1 = fopen(outfile, "wb");

    /*init output file headers*/
    fwrite("DIFF", 4, 1, f1);
    long ft = ftell(f1);
    struct fileH fileheader1;
    fileheader1.numofcols = COLNUM;
    fileheader1.numofvals = numofvals;
    fileheader1.numofblocks = ceil(fileheader1.numofvals * 1.0 / BLOCKSIZE);
    //cout << fileheader1.numofblocks << endl;

    fwrite(&fileheader1, sizeof(fileheader1), 1, f1);
    long blocksizes[fileheader1.numofblocks + 1];

    fwrite(blocksizes, (fileheader1.numofblocks + 1) * sizeof(long), 1, f1);

    bool eof = false;
    string line;
    vector<string> dataset; // buffer that holds a batch of rows in raw text

    int num_of_vals1 = 0;

    for (int i = 0; i < startp; i++) {
        std::getline(finput, line);
    }

    while (!eof) {
        int numValues = 0;      // num of lines read in a batch

        dataset.clear();
        // read a batch of lines from the input file
        start = std::clock();


        for (int i = 0; i < BLOCKSIZE; ++i) {
            if (!std::getline(finput, line) or num_of_vals1 >= fileheader1.numofvals) {
                eof = true;
                break;
            }
            dataset.push_back(line.substr(0, line.size()-1)); //(-1 for yelp)
            ++numValues;
            num_of_vals1++;
        }
        duration += (std::clock() - start) / (double) CLOCKS_PER_SEC;
        /*compress batch*/
        long tell_init = ftell(f1);
        for (int k = 0; k < COLNUM + 1; k++)
            fwrite(&columnindexes[k], sizeof(int), 1, f1);
        long tell = ftell(f1);
        vector<std::string> column(dataset.size());
        for (int j = 0; j < COLNUM; j++) {
            long tell1 = ftell(f1);
            //compress(dataset, f1, sizediff[j], globaldict[j], glob[j], lookup[j], diffvals[j]);
            start = std::clock();
            extractColumn(dataset, mcolumns[j], column);
            duration += (std::clock() - start) / (double) CLOCKS_PER_SEC;
            compress_batch(column,  f1, &filter, isdictionary, blocksize[j], sizediff[j],
                           lookup[j], diffvals[j], blocknum, BLOCKSIZE, SNAPPY, global_dict_memory[j], globaldictlen[j],
                           permanent_decision[j], duration5, costf, dthreshold);
            //compress(slice(dataset,0,dataset.size()-1),f1);
            columnindexes[j] = tell1 - tell;
        }
        long tell_end = ftell(f1);
        columnindexes[COLNUM] = tell_end - tell;
        fseek(f1, tell_init, SEEK_SET);
        for (int k = 0; k < COLNUM + 1; k++)
            fwrite(&columnindexes[k], sizeof(int), 1, f1);


        fseek(f1, tell_end, SEEK_SET);
        blocksizes[blocknum] = tell_init;
        blocknum++;
    }


    std::cout<<"csv import time: "<< duration <<'\n';
    fseek(f1, ft, SEEK_SET);
    fileheader1.numofvals = num_of_vals1;
    fileheader1.numofblocks = ceil(fileheader1.numofvals * 1.0 / BLOCKSIZE);
    fwrite(&fileheader1, sizeof(fileheader1), 1, f1);
    blocksizes[fileheader1.numofblocks] = BLOCKSIZE;
    fwrite(blocksizes, (fileheader1.numofblocks + 1) * sizeof(long), 1, f1);
    fflush(f1);
    return 0;
}



//echo "nameserver 8.8.8.8" | sudo tee /etc/resolv.conf > /dev/null
