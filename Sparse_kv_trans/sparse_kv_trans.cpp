#include <fstream>
#include <iostream>
#include <vector>
#include <string>
#include <sys/time.h>
#include <cstring>
#include <stdlib.h>

#define MAX_STRING 100

char in_file[MAX_STRING], out_file[MAX_STRING];
unsigned int debug_mode = 1;

using namespace std;

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
void Matrix(){
    ifstream ifs(in_file);
    if(!ifs.is_open()){
        cout << "input file open failed:" << in_file << endl;
        return;
    }
    ofstream ofs(out_file);
    if(!ofs.is_open()){
        cout << "output file open failed:" << out_file << endl;
        return;
    }
    string line;
    string delim = " ";
    vector<string> vec;
    long index = 0l;
    while(getline(ifs, line)){
        vec.clear();
        ofs << index++ ;
        split(line, delim, &vec);
        for(int i = 0; i < vec.size(); ++i){
            if(vec[i].length() && vec[i] != "0"){
                ofs << " " <<  i + 1 << ":" << vec[i];
            }
        }
        ofs << endl;
    }
    ifs.close();
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
        printf("sparse matrix transform toolkit v 1.0a\n\n");
        printf("Options:\n\tconvert the sparse matrix into the form of :[lineno. colum_index:value colum_index:value...]\n");
        printf("Parameters:\n");
        printf("\t-in <file>\n");
        printf("\t\t<tag,cluster> data from <file> to read\n");
        printf("\t-out <file>\n");
        printf("\t\toutput the result to <file>\n");
        printf("\t-debug <int>\n");
        printf("\t\tSet the debug mode (default = 2 = more info during training)\n");
        printf("\nExamples:\n");
        printf("./sparse_kv_trans -in in/matrix.txt -out out/matrix_out.txt -debug 2\n\n");
        return 0;
    }
    in_file[0] = 0;
    out_file[0]=0;
    if ((i = ArgPos((char *)"-in", argc, argv)) > 0) strcpy(in_file, argv[i + 1]);
    if ((i = ArgPos((char *)"-out", argc, argv)) > 0) strcpy(out_file, argv[i + 1]);
    if ((i = ArgPos((char *)"-debug", argc, argv)) > 0) debug_mode = atoi(argv[i + 1]);

    //g_pmutex = new pthread_mutex_t();
    //pthread_mutex_init(g_pmutex, NULL);

    Matrix();

    //pthread_mutex_destroy(g_pmutex);
    //delete g_pmutex;
    //g_pmutex = NULL;

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

