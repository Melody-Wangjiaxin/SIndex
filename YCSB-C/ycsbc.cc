//
//  ycsbc.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/19/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include <cstring>
#include <string>
#include <iostream>
#include <vector>
#include <future>
#include "core/utils.h"
#include "core/timer.h"
#include "core/client.h"
#include "core/core_workload.h"
#include "db/db_factory.h"
#include <unistd.h>
#include <atomic>

using namespace std;

void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
string ParseCommandLine(int argc, const char *argv[], utils::Properties &props);

int cnt = 0;
int thread_num = 0;
extern void rcu_init(size_t start_num, size_t worker_num);
extern void run_benchmark(size_t sec, ycsbc::CoreWorkload* workload_, ycsbc::DB* db_);
extern void run_benchmark2(ycsbc::CoreWorkload* workload_, ycsbc::DB* db_, int op_num);
extern void *run_fg(void *arguments);
extern size_t fg_n;
extern std::atomic<size_t> ready_threads;
extern std::atomic<size_t> ready_bgs;
std::string dbname;
std::string test_type;
 

int DelegateClient(ycsbc::DB *db, ycsbc::CoreWorkload *wl, const int num_ops,
    bool is_loading) {
  db->Init();
  ycsbc::Client client(*db, *wl);
  int oks = 0;
  for (int i = 0; i < num_ops; ++i) {
    if (is_loading) {
      if(dbname == "sindex"){
        oks += client.DoInsert2();
      }else{
        oks += client.DoInsert();
      }
    } else {
      oks += client.DoTransaction();
    }
  }
  db->Close();
  return oks;
}

int main(const int argc, const char *argv[]) {
  
  utils::Properties props;
  string file_name = ParseCommandLine(argc, argv, props);

  ycsbc::DB *db = ycsbc::DBFactory::CreateDB(props);
  if (!db) {
    cout << "Unknown database name " << props["dbname"] << endl;
    exit(0);
  }
  dbname = props["dbname"];

  ycsbc::CoreWorkload wl;
  wl.Init(props);
  const int num_threads = stoi(props.GetProperty("threadcount", "1"));

  if(test_type == "fix_op"){
    // // Loads data
    // vector<future<int>> actual_ops;
    // int total_ops = stoi(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
    
    // for (int i = 0; i < num_threads; ++i) {
    //   actual_ops.emplace_back(async(launch::async,
    //       DelegateClient, db, &wl, total_ops / num_threads, true));
    // }
    // assert((int)actual_ops.size() == num_threads);

    // int sum = 0;
    // for (auto &n : actual_ops) {
    //   assert(n.valid());
    //   sum += n.get();
    // }
    // cerr << "# Loading records:\t" << sum << endl;

    // db = ycsbc::DBFactory::CreateDB(props);
    // if(dbname == "sindex"){
    //   sem_init(&sem, 0, 1);
    //   cnt = num_threads;
    //   minimum_thread_id = 0x7fffffff;
    // }

    // // Peforms transactions
    // actual_ops.clear();
    // total_ops = stoi(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);
    
    // for (int i = 0; i < num_threads; ++i) {
    //   actual_ops.emplace_back(async(launch::async,
    //       DelegateClient, db, &wl, total_ops / num_threads, false));
    // }
    // assert((int)actual_ops.size() == num_threads);

    // sum = 0;
    // for (auto &n : actual_ops) {
    //   assert(n.valid());
    //   sum += n.get();
    // }
    // double duration = timer.End();
    // cerr << "duration: " << '\t' << duration << endl;
    // cerr << "# Transaction throughput (TPS)" << endl;
    // cerr << props["dbname"] << '\t' << file_name << '\t' << num_threads << '\t';
    // cerr << total_ops / duration << endl;
    // Loads data
    vector<future<int>> actual_ops;
    int total_ops = stoi(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
    
    for (int i = 0; i < num_threads; ++i) {
      actual_ops.emplace_back(async(launch::async,
          DelegateClient, db, &wl, total_ops / num_threads, true));
    }
    assert((int)actual_ops.size() == num_threads);

    int sum = 0;
    for (auto &n : actual_ops) {
      assert(n.valid());
      sum += n.get();
    }
    cout << "# Loading records:\t" << sum << endl;

    actual_ops.clear();
    total_ops = stoi(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);
    db = ycsbc::DBFactory::CreateDB(props);
    run_benchmark2(&wl, db, total_ops);

  } else if(test_type == "fix_time"){
    // Loads data
    vector<future<int>> actual_ops;
    int total_ops = stoi(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
    
    for (int i = 0; i < num_threads; ++i) {
      actual_ops.emplace_back(async(launch::async,
          DelegateClient, db, &wl, total_ops / num_threads, true));
    }
    assert((int)actual_ops.size() == num_threads);

    int sum = 0;
    for (auto &n : actual_ops) {
      assert(n.valid());
      sum += n.get();
    }
    cerr << "# Loading records:\t" << sum << endl;

    db = ycsbc::DBFactory::CreateDB(props);
    run_benchmark(10, &wl, db);
  } else{
    cout << "Unknown test type" << endl;
  }
  while(ready_bgs != 0) continue;
  if(db != nullptr) delete db; 
}

string ParseCommandLine(int argc, const char *argv[], utils::Properties &props) {
  int argindex = 1;
  string filename;
  while (argindex < argc && StrStartWith(argv[argindex], "-")) {
    if (strcmp(argv[argindex], "-threads") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("threadcount", argv[argindex]);
      fg_n = stoi(argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-db") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("dbname", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-host") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("host", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-port") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("port", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-slaves") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("slaves", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-P") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      filename.assign(argv[argindex]);
      ifstream input(argv[argindex]);
      try {
        props.Load(input);
      } catch (const string &message) {
        cout << message << endl;
        exit(0);
      }
      input.close();
      argindex++;
    } else if (strcmp(argv[argindex], "-test-type") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      test_type = argv[argindex];
      argindex++;
    } else {
      cout << "Unknown option '" << argv[argindex] << "'" << endl;
      exit(0);
    }
  }

  if (argindex == 1 || argindex != argc) {
    UsageMessage(argv[0]);
    exit(0);
  }

  return filename;
}

void UsageMessage(const char *command) {
  cout << "Usage: " << command << " [options]" << endl;
  cout << "Options:" << endl;
  cout << "  -threads n: execute using n threads (default: 1)" << endl;
  cout << "  -db dbname: specify the name of the DB to use (default: basic)" << endl;
  cout << "  -P propertyfile: load properties from the given file. Multiple files can" << endl;
  cout << "                   be specified, and will be processed in the order specified" << endl;
  cout << "  -test-type type of test: set the standard of test, fix time and count operations, or fix operation count and calculate time" << endl;
}

inline bool StrStartWith(const char *str, const char *pre) {
  return strncmp(str, pre, strlen(pre)) == 0;
}

