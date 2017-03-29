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

char tag_file[MAX_STRING], cluster_file[MAX_STRING], src_file[MAX_STRING], out_file[MAX_STRING];
map<string, map<string, double> > g_tagmap;

typedef struct _threadParam{
    long index;
    long start_pos;
    long end_pos;
}ThreadParam;
typedef struct _InteThreadParam{
    long index;
    long clus_start_pos;
    long clus_end_pos;
    long src_start_pos;
    long src_end_pos;
} InteThreadParam;

const long PER_PRINT = 100000l;
long per_print = 100000l;
const int vocab_hash_size = 30000000;  // Maximum 30 * 0.7 = 21M words in the vocabulary
typedef struct _vocab_word {
  long count;
  char *word;
  double idf;
} vocab_word_t;

unsigned int debug_mode = 1, num_threads = 12;
unsigned int topn = 150;

pthread_mutex_t *g_pmutex = NULL;



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
bool CompareStrDblPair(pair<string, double> a,pair<string, double> b)
{
      return a.second > b.second;   //…˝–Ú≈≈¡–
}
/*void IDFWalk(){
    for(auto &it: gMapKeys){
        double idf = log((double)gDocCount/(1+(double)(it.second)));
        gMapIDF[it.first] = idf;
    }
    gMapKeys.clear();
}*/

void *TagThread(void *param) {
    ThreadParam *thp = (ThreadParam *)param;
    //cout << "thread[" << thp->index << "]:" << thp->start_pos << "->" << thp->end_pos << endl;
    if(thp->start_pos == -1 || thp->end_pos == -1) return NULL;
    ifstream ifs(tag_file);
    if(!ifs.is_open()){
        cout << "tag file open failed :" << tag_file << endl;
        return NULL;
    }
    ifs.seekg(thp->start_pos, ios::beg);
    string line;
    string delim = "\t";
    vector<string> vec;
    //map<string, int> mkeys;
    long counter = 0;
    while(getline(ifs, line)){
        if(ifs.tellg() > thp->end_pos) break;
        vec.clear();
        //mkeys.clear();
        split(line, delim, &vec);
        if(vec.size() == 2){
            auto it = g_tagmap.find(vec[0]);
            if(it == g_tagmap.end()){
                map<string, double> m;
                pthread_mutex_lock(g_pmutex);
                g_tagmap[vec[0]] = m;
                pthread_mutex_unlock(g_pmutex);
            }
        }
        //cout << "thread [" << thp->index << "] stop, " << mkeys.size() << " keys added!" << endl;
        counter++;
        if(counter % per_print == 0){
            cout << "train th" << thp->index << ":" << counter << endl;
        }

    }
    ifs.close();
}

void *IntegrateThread(void *param) {
    InteThreadParam *thp = (InteThreadParam *)param;
    //cout << "thread[" << thp->index << "]:" << thp->start_pos << "->" << thp->end_pos << endl;
    if(thp->clus_start_pos == -1 ||
       thp->clus_end_pos == -1 ||
       thp->src_start_pos == -1 ||
       thp->src_end_pos == -1) return NULL;
    ifstream clus_ifs(cluster_file);
    if(!clus_ifs.is_open()){
        cout << "cluster_file file open failed:" << cluster_file << endl;
        return NULL;
    }
    ifstream src_ifs(src_file);
    if(!src_ifs.is_open()){
        cout << "src_file file open failed:" << src_file << endl;
        return NULL;
    }
    clus_ifs.seekg(thp->clus_start_pos, ios::beg);
    src_ifs.seekg(thp->src_start_pos, ios::beg);
    string clus_line;
    string src_line;
    string delim = " ";
    vector<string> clus_vec;
    vector<string> src_vec;
    long counter = 0;
    //vector<pair<string, double> > vkv;
    while(getline(clus_ifs, clus_line) && getline(src_ifs, src_line)){
        if(clus_ifs.tellg() > thp->clus_end_pos || src_ifs.tellg() > thp->src_end_pos) break;
        clus_vec.clear();
        src_vec.clear();
        split(clus_line, delim, &clus_vec);
        split(src_line, delim, &src_vec);
        if(clus_vec.size() != 2){
            cout << "[error]clus_vec.size():" << clus_vec.size() << endl
                 << "clus_line:" << clus_line << endl
                 << "src_line:" << src_line << endl;
            continue;
        }
        //count the repeat words
        for(unsigned int i = 0; i < src_vec.size(); i++){
            if(debug_mode >= 2){
                cout << "src_vec[" << i << "]=" << src_vec[i]<< endl;
            }
            auto it = g_tagmap.find(src_vec[i]);
            pthread_mutex_lock(g_pmutex);
            if(it != g_tagmap.end()){
                if(debug_mode >= 2){
                    cout << "find " << src_vec[i] << endl;
                }
                auto sit = it->second.find(clus_vec[1]);
                if(sit != it->second.end()){
                    if(debug_mode >= 2){
                        cout << "find: " << sit->second << ":" << sit->second << endl;
                    }
                    sit->second += 1;
                }else{
                    it->second[clus_vec[1]] = 1;
                    if(debug_mode >= 2){
                        cout << "add: " << it->second[clus_vec[1]] << endl;
                    }
                }
            }
            pthread_mutex_unlock(g_pmutex);
        }
        counter++;
        if(counter % per_print == 0){
            cout << "predict th" << thp->index << ":" << counter << endl;
        }
    }
    clus_ifs.close();
    src_ifs.close();
}

/*void *SaveTrainModelThread(void *param){
    vector<pair<string, double> > vidf;
    for(auto &it:gMapIDF){
        vidf.push_back(pair<string, double>(it.first, it.second));
    }
    sort(vidf.begin(), vidf.end(), CompareIDFPair);
    ofstream ofs(model_file);
    if(!ofs.is_open()){
        cout << "cant open the output model file:" << model_file << endl;
        return NULL;
    }
    for(long i = 0; i < vidf.size(); i++){
        ofs << vidf[i].first << " " << vidf[i].second << endl;
    }
    ofs.close();
}*/


void TagModel(FILE *fp){
    long lines = GetFileLines(tag_file);
    GetPerPrint(lines);
    cout << "lines:" << lines << endl;
    long *seekpos = (long *)calloc(num_threads+1, sizeof(long));//each start pos of the predict_file for thread, the last one is the position of eof
    GetFilePos(seekpos, num_threads,lines, tag_file);
    pthread_t *pt = (pthread_t *)malloc(num_threads * sizeof(pthread_t));
    ThreadParam *pa = (ThreadParam *)calloc(num_threads, sizeof(ThreadParam));
    long real_thread_cn = 0;
    for (long i = 0; i < num_threads; i++){
        if(seekpos[i] == -1 || seekpos[i+1] == -1) break;
        pa[i].index = i;
        pa[i].start_pos = seekpos[i];
        pa[i].end_pos = seekpos[i+1];
        pthread_create(&pt[i], NULL, TagThread, (void *)(pa+i));
        real_thread_cn++;
    }
    for (long i = 0; i < real_thread_cn; i++){
        pthread_join(pt[i], NULL);
    }

    //string rmcmd = "rm " + string(output_file) + "_*";
    //system(rmcmd.c_str());


    free(seekpos);
    seekpos = NULL;
    free(pt);
    pt = NULL;
    free(seekpos);
    seekpos = NULL;
    free(pa);
    pa = NULL;
}
void IntegrateModel(FILE *fp){
    ofstream ofs(out_file);
    if(!ofs.is_open()){
        fprintf(fp, "open out file[%s] failed!\n",out_file);
        return;
    }
    long lines = GetFileLines(cluster_file);
    long src_lines = GetFileLines(src_file);
    if(lines != src_lines){
        cout << "[error]clus_lines:" << lines << "!= src_lines:" << src_lines << endl;
        return;
    }

    GetPerPrint(lines);
    cout << "lines:" << lines << endl;
    long *clus_seekpos = (long *)calloc(num_threads+1, sizeof(long));//each start pos of the predict_file for thread, the last one is the position of eof
    GetFilePos(clus_seekpos, num_threads,lines, cluster_file);
    long *src_seekpos = (long *)calloc(num_threads+1, sizeof(long));//each start pos of the predict_file for thread, the last one is the position of eof
    GetFilePos(src_seekpos, num_threads,lines, src_file);

    pthread_t *pt = (pthread_t *)malloc(num_threads * sizeof(pthread_t));
    InteThreadParam *pa = (InteThreadParam *)calloc(num_threads, sizeof(InteThreadParam));
    long real_thread_cn = 0;
    for (long i = 0; i < num_threads; i++){
        if(clus_seekpos[i] == -1 ||
           clus_seekpos[i+1] == -1 ||
           src_seekpos[i] == -1 ||
           src_seekpos[i+1] == -1) break;
        pa[i].index = i;
        pa[i].clus_start_pos = clus_seekpos[i];
        pa[i].clus_end_pos = clus_seekpos[i+1];
        pa[i].src_start_pos = src_seekpos[i];
        pa[i].src_end_pos = src_seekpos[i+1];
        pthread_create(&pt[i], NULL, IntegrateThread, (void *)(pa+i));
        real_thread_cn++;
    }

    for (long i = 0; i < real_thread_cn; i++){
        pthread_join(pt[i], NULL);
    }
    vector<pair<string, double> > vsort;
    for(auto &it: g_tagmap){
        long total_cnt = 0L;
        long max = 0L;
        long min = 999999999L;
        if(it.second.size() == 0) continue;
        ofs << it.first;
        vsort.clear();
        for(auto &sit : it.second){
            total_cnt += sit.second;
            if(sit.second > max) max = sit.second;
            if(sit.second < min) min = sit.second;
            vsort.push_back(pair<string, double>(sit.first, sit.second));
        }
        if(min != max){
            long dis = max - min;
            for(auto &sit: vsort){
                sit.second = (sit.second - min)/dis;
                //sit.second /= total_cnt;
            }
        }
        sort(vsort.begin(), vsort.end(), CompareStrDblPair);
        unsigned int top = topn;
        if(topn == 0 || topn > vsort.size()){
            top = vsort.size();
        }
        unsigned int itop = 0;
        for(auto &sit:vsort){
            itop++;
            ofs << " " << sit.first << ":" << sit.second;
            if(itop == top) break;
        }
        ofs << endl;
    }

    free(src_seekpos);
    src_seekpos = NULL;
    free(clus_seekpos);
    clus_seekpos = NULL;
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
        printf("Muti-thread tag cluster frequency toolkit v 1.0a\n\n");
        printf("Options:\n");
        printf("Parameters for training:\n");
        printf("\t-tag <file>\n");
        printf("\t\t<tag,cluster> data from <file> to read\n");
        printf("\t-cluster <file>\n");
        printf("\t\t<lineno.,cluster> data from <file> to read\n");
        printf("\t-out <file>\n");
        printf("\t\toutput the result to <file>\n");
        printf("\t-src <file>\n");
        printf("\t\tsource <file> to match the cluster <file>\n");
        printf("\t-topn <int>\n");
        printf("\t\tonly keep the top <int> keywords,default 150\n");
        printf("\t-threads <int>\n");
        printf("\t\tUse <int> threads (default 12) to run the train and predict process\n");
        printf("\t-debug <int>\n");
        printf("\t\tSet the debug mode (default = 2 = more info during training)\n");
        printf("\nExamples:\n");
        printf("./clustag -topn 0 -tag in/nine_class_label.txt -cluster in/cluster.train -src out/wiki_cn._nospace -out out/result.out -threads 20 -debug 2\n\n");
        return 0;
    }
    tag_file[0] = 0;
    cluster_file[0] = 0;
    src_file[0] = 0;
    out_file[0]=0;
    if ((i = ArgPos((char *)"-tag", argc, argv)) > 0) strcpy(tag_file, argv[i + 1]);
    if ((i = ArgPos((char *)"-cluster", argc, argv)) > 0) strcpy(cluster_file, argv[i + 1]);
    if ((i = ArgPos((char *)"-src", argc, argv)) > 0) strcpy(src_file, argv[i + 1]);
    if ((i = ArgPos((char *)"-out", argc, argv)) > 0) strcpy(out_file, argv[i + 1]);
    if ((i = ArgPos((char *)"-threads", argc, argv)) > 0) num_threads = atoi(argv[i + 1]);
    if ((i = ArgPos((char *)"-debug", argc, argv)) > 0) debug_mode = atoi(argv[i + 1]);
    if ((i = ArgPos((char *)"-topn", argc, argv)) > 0) topn = atoi(argv[i + 1]);

    g_pmutex = new pthread_mutex_t();

    pthread_mutex_init(g_pmutex, NULL);


	FILE *flog;
    if((flog=fopen("train.log", "w")) == NULL) {
		fprintf(stderr, "open train.log file failed.\n");
		return EXIT_FAILURE;
	}
    cout << "start initializing tags..." << endl;
    TagModel(flog);

    if(debug_mode >= 2){
        for(auto & it : g_tagmap){
            cout << "g_tagmap[" << it.first << "]:";
            for(auto &sit:it.second){
                cout << sit.first << ":" << sit.second;
            }
            cout << endl;
        }
    }
    cout << "Integrating..." << endl;
    IntegrateModel(flog);

    if(debug_mode >= 2){
        for(auto & it : g_tagmap){
            cout << "g_tagmap[" << it.first << "]:";
            for(auto &sit:it.second){
                cout << sit.first << ":" << sit.second;
            }
            cout << endl;
        }
    }
    //IDFWalk();

    //cout << "start predicting and save idf model..." << endl;
    //pthread_t *pt = (pthread_t *)malloc(sizeof(pthread_t));
    //pthread_create(pt, NULL, SaveTrainModelThread, NULL);
    //pthread_join(*pt, NULL);

    pthread_mutex_destroy(g_pmutex);
    delete g_pmutex;
    g_pmutex = NULL;

    struct timeval t_end;
    gettimeofday(&t_end, NULL);
    printf("t_start.tv_sec:%d\n", t_start.tv_sec);
    printf("t_start.tv_usec:%d\n", t_start.tv_usec);
    printf("t_end.tv_sec:%d\n", t_end.tv_sec);
    printf("t_end.tv_usec:%d\n", t_end.tv_usec);
    time_t t = (t_end.tv_sec - t_start.tv_sec) * 1000000 + (t_end.tv_usec - t_start.tv_usec);
    cout << "start time :" << t_start.tv_sec << "." << t_start.tv_usec << endl
        << "end time :" << t_end.tv_sec << "." << t_end.tv_usec << endl
        << "using time : " << t_end.tv_sec - t_start.tv_sec << "."<< t_end.tv_usec - t_start.tv_usec << " s" << endl;
    return 0;
}

