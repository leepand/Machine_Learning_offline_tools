#include <fstream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <pthread.h>
#include <sys/time.h>
#include <sstream>
#include <iostream>
#include <map>
#include <vector>
#include<algorithm>//sort

using namespace std;

#define MAX_STRING 100
char in_file[MAX_STRING], out_file[MAX_STRING];
int debug_mode = 2, num_threads = 12;
pthread_mutex_t *pmutex = NULL;

const long PER_PRINT = 100000l;
long per_print = 100000l;


typedef struct _threadParam{
    long index;
    long start_pos;
    long end_pos;
}ThreadParam;
map<string, long> g_mapKeyIDs; //<key, keyid>
static long g_keyID = 0;//the new added key id
vector<string> g_vNewKeys;// new added keys add to the end of the output file;


static void split(std::string& s, std::string& delim,std::vector< std::string >* ret)
{
    size_t last = 0;
    size_t index=s.find_first_of(delim,last);
    while (index!=std::string::npos)
    {
        ret->push_back(s.substr(last,index-last));
        //last=index+1;
        last = index + delim.length();
        index=s.find_first_of(delim,last);
    }
    if (index-last>0)
    {
        ret->push_back(s.substr(last,index-last));
    }
}
static long GetFileLines(char *_file){
    long _cn = 0;
    ifstream ifs(_file);
    if(ifs.is_open()){
        string line;
        while(getline(ifs, line)){
            _cn++;
        }
        ifs.close();
    }
    return _cn;
}
static void GetFilePos(long *_pos, long _num, long _lines, char *_file){
    if(_num == 0) return;
    long factor = _lines/_num;
    while(factor*_num < _lines){
        factor++;
    }
    long i = 0, index = 0;
    ifstream ifs(_file);
    long lastPos = 0;
    if(ifs.is_open()){
        string line;
        while(getline(ifs, line)){
            if(i == _lines) break;

            if(i == index * factor){
                _pos[index++] = lastPos;//get current length of file
            }
            i++;
            lastPos = ifs.tellg();//get current length of file
        }
        _pos[index++] = lastPos;//eof
        for(;index < _num+1; index++){
            _pos[index] = -1;
        }
        ifs.close();
    }
    /*for(int i = 0; i < _num+1; i++){
        cout << "_pos[" << i << "] =" << _pos[i] << endl;
    }*/
}
static long GetPerPrint(long lines){
    per_print = lines / (num_threads * 10);
    if(per_print == 0){
        per_print = 1;
    }
    if(per_print > PER_PRINT)
    {
        per_print = PER_PRINT;
    }
    per_print = 500l;
    cout << "per_print:" << per_print << endl;
    return per_print;
}

void InitKeyID(FILE *fp){
    ifstream ifs(in_file);
    if(!ifs.is_open()){
        cout << "cant open the input file:" << in_file << endl;
        return;
    }
    string line;
    string delim = "\t";
    vector<string> vec;
    g_keyID = 1;
    g_mapKeyIDs.clear();
    g_vNewKeys.clear();
    while(getline(ifs, line)){
        vec.clear();
        split(line, delim, &vec);
        if(2 == vec.size()){
            g_mapKeyIDs[vec[0]] = g_keyID++;
        }else{
            cout << "never happened error:" << line << " split size =" << vec.size() << endl;
        }
    }
    ifs.close();
}
void *VarThread(void *param){
    ThreadParam *thp = (ThreadParam *)param;
    //cout << "thread[" << thp->index << "]:" << thp->start_pos << "->" << thp->end_pos << endl;
    if(thp->start_pos == -1 || thp->end_pos == -1) return NULL;
    ifstream ifs(in_file);
    if(!ifs.is_open()){
        cout << "thread[" << thp->index << "]:input file open failed:" << in_file << endl;
        return NULL;
    }
    stringstream sstr;
    sstr << out_file << "_" << thp->index;
    string out_file;
    sstr >> out_file;
    ofstream ofs(out_file.c_str());
    if(!ofs.is_open()){
        cout << "output file open failed:" << out_file << endl;
        return NULL;
    }
    ifs.seekg(thp->start_pos, ios::beg);
    string line;
    string delim = "\t";
    string sdelim = " ";
    string subdelim = ":";
    vector<string> vec;
    vector<string> svec;
    vector<string> subvec;
    long counter = 0;
    string outstring;
    while(getline(ifs, line)){
        if(ifs.tellg() > thp->end_pos) break;
        vec.clear();
        split(line, delim, &vec);
        if(vec.size() >= 2){
            svec.clear();
            outstring = vec[0] + ":1\t";
            split(vec[1], sdelim, &svec);
            for(int i = 0; i < svec.size(); ++i){
                if(i > 0){
                    outstring += " ";
                }
                subvec.clear();
                split(svec[i], subdelim, &subvec);
                if(2== subvec.size()){
                    stringstream ss;
                    auto it  = g_mapKeyIDs.find(subvec[0]);
                    if(it != g_mapKeyIDs.end()){
                        ss << it->second;
                    }else{
                        pthread_mutex_lock(pmutex);
                        ss << g_keyID;
                        g_mapKeyIDs[subvec[0]] = g_keyID++;
                        g_vNewKeys.push_back(subvec[0]);
                        pthread_mutex_unlock(pmutex);
                    }
                    outstring += ss.str() + ":1:" + subvec[1];
                }/*else{
                    cout << svec[i] << " split failed:" << subvec.size() << endl;
                }*/
            }
        }else{
            cout << line << " split size=" << vec.size() << endl;
        }



        ofs << outstring << endl;
        counter++;
        if(counter % per_print == 0){
            cout << "predict th" << thp->index << ":" << counter << endl;
        }
    }
    ifs.close();
    ofs.close();
}
void VarModel(FILE * fp){
    ofstream ofs(out_file);
    if(!ofs.is_open()){
        fprintf(fp, "open out file[%s] failed!\n",out_file);
        return;
    }
    long lines = GetFileLines(in_file);
    GetPerPrint(lines);
    cout << "lines:" << lines << endl;
    long *seekpos = (long *)calloc(num_threads+1, sizeof(long));//each start pos of the predict_file for thread, the last one is the position of eof
    GetFilePos(seekpos, num_threads,lines, in_file);
    pthread_t *pt = (pthread_t *)malloc(num_threads * sizeof(pthread_t));
    ThreadParam *pa = (ThreadParam *)calloc(num_threads, sizeof(ThreadParam));
    long real_thread_cn = 0;
    for (long i = 0; i < num_threads; i++){
        if(seekpos[i] == -1 || seekpos[i+1] == -1) break;
        pa[i].index = i;
        pa[i].start_pos = seekpos[i];
        pa[i].end_pos = seekpos[i+1];
        pthread_create(&pt[i], NULL, VarThread, (void *)(pa+i));
        real_thread_cn++;
    }
    for (long i = 0; i < real_thread_cn; i++){
        pthread_join(pt[i], NULL);

        stringstream sstr;
        sstr << out_file << "_" << i;
        string istrout;
        sstr >> istrout;
        ifstream iifs(istrout.c_str());
        if(!iifs.is_open()){
            cout << i << " output file open failed:" << istrout << endl;
            continue;
        }else{
            iifs.seekg(0, std::ios::end);
            long len = iifs.tellg();
            iifs.seekg(0, std::ios::beg);
            char * buffer = (char*)calloc(len+1, sizeof(char));
            iifs.read(buffer, len);
            ofs << buffer;
            free(buffer);
            buffer = NULL;
            iifs.close();
        }

    }
    for(auto &it : g_vNewKeys){
        ofs << it << ":1\t"<< endl;
    }
    string rmcmd = "rm " + string(out_file) + "_*";
    system(rmcmd.c_str());


    free(seekpos);
    seekpos = NULL;
    free(pt);
    pt = NULL;
    free(pa);
    pa = NULL;
    ofs.close();
}


int ArgPos(char *str, int argc, char **argv) {
  int a;
  for (a = 1; a < argc; a++) if (!strcmp(str, argv[a])) {
    if (a == argc - 1) {
      printf("Argument missing for %s\n", str);
      exit(1);
    }
    return a;
  }
  return -1;
}
int main(int argc, char **argv) {
    struct timeval t_start;
    gettimeofday(&t_start, NULL);
    printf("t_start.tv_sec:%d\n", t_start.tv_sec);
    printf("t_start.tv_usec:%d\n", t_start.tv_usec);

    int i;
    if (argc == 1) {
        printf("Muti-thread domod variable toolkit v 1.0a\n\n");
        printf("Options:\n");
        printf("Parameters:\n");
        printf("\t-in <file>\n");
        printf("\t\the input data from <file> to read\n");
        printf("\t-out <file>\n");
        printf("\t\toutput the result to <file>\n");
        printf("\t-threads <int>\n");
        printf("\t\tUse <int> threads (default 12) to run the train and predict process\n");
        printf("\t-debug <int>\n");
        printf("\t\tSet the debug mode (default = 2 = more info during training)\n");
        printf("\nExamples:\n");
        printf("./dmvar -in in/weibo_wear_recom.txt -out weibo_wear_recom_new.txt -threads 20 -debug 2\n\n");
        return 0;
    }
    in_file[0] = 0;
    out_file[0]=0;
    if ((i = ArgPos((char *)"-in", argc, argv)) > 0) strcpy(in_file, argv[i + 1]);
    if ((i = ArgPos((char *)"-out", argc, argv)) > 0) strcpy(out_file, argv[i + 1]);
    if ((i = ArgPos((char *)"-threads", argc, argv)) > 0) num_threads = atoi(argv[i + 1]);
    if ((i = ArgPos((char *)"-debug", argc, argv)) > 0) debug_mode = atoi(argv[i + 1]);

    pmutex = new pthread_mutex_t();
    pthread_mutex_init(pmutex, NULL);

	FILE *flog;
    if((flog=fopen("train.log", "w")) == NULL) {
		fprintf(stderr, "open train.log file failed.\n");
		return EXIT_FAILURE;
	}
    cout << "Initialize the key id..." << endl;
    InitKeyID(flog);
    cout << "VARWalk..." << endl;
    VarModel(flog);

    pthread_mutex_destroy(pmutex);
    delete pmutex;
    pmutex = NULL;

    struct timeval t_end;
    gettimeofday(&t_end, NULL);
    printf("t_start.tv_sec:%d\n", t_start.tv_sec);
    printf("t_start.tv_usec:%d\n", t_start.tv_usec);
    printf("t_end.tv_sec:%d\n", t_end.tv_sec);
    printf("t_end.tv_usec:%d\n", t_end.tv_usec);
    cout << "start time :" << t_start.tv_sec << "." << t_start.tv_usec << endl
        << "end time :" << t_end.tv_sec << "." << t_end.tv_usec << endl;
    if((t_end.tv_usec - t_start.tv_usec) > 0){
        cout << "using time : " << t_end.tv_sec - t_start.tv_sec << "."<< t_end.tv_usec - t_start.tv_usec << " s" << endl;
    }else{
        cout << "using time : " << t_end.tv_sec - t_start.tv_sec - 1 << "."<< 1000000 + t_end.tv_usec - t_start.tv_usec << " s" << endl;
    }
    return 0;
}

