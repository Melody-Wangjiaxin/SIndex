#ifndef YCSB_C_SINDEX_DB_H_
#define YCSB_C_SINDEX_DB_H_

#include "core/db.h"

#include <getopt.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "lib/sindex.h"
#include "lib/helper.h"
#include "lib/sindex_impl.h"
#include "core/core_workload.h"
#include "core/timer.h"

using std::string;
using namespace ycsbc;

template <size_t len>
class StrKey {
	typedef std::array<double, len> model_key_t;

public:
	static constexpr size_t model_key_size() { return len; }

	static StrKey max() {
		static StrKey max_key;
		memset(&max_key.buf, 255, len);
		return max_key;
	}
	static StrKey min() {
		static StrKey min_key;
		memset(&min_key.buf, 0, len);
		return min_key;
	}

	StrKey() { memset(&buf, 0, len); }
	StrKey(const std::vector<uint64_t> key) { 
    memset(&buf, 0, len);
    memcpy(&buf, key.data(), key.size() * 8);
    // COUT_N_EXIT("str key no uint64"); 
  }
	StrKey(const std::string &s) {
		memset(&buf, 0, len);
		memcpy(&buf, s.data(), s.size());
	}
	StrKey(const StrKey &other) { memcpy(&buf, &other.buf, len); }
	StrKey &operator=(const StrKey &other) {
		memcpy(&buf, &other.buf, len);
		return *this;
	}

	model_key_t to_model_key() const {
		model_key_t model_key;
		for (size_t i = 0; i < len; i++) {
			model_key[i] = buf[i];
		}
		return model_key;
	}

	void get_model_key(size_t begin_f, size_t l, double *target) const {
		for (size_t i = 0; i < l; i++) {
			target[i] = buf[i + begin_f];
		}
	}

	bool less_than(const StrKey &other, size_t begin_i, size_t l) const {
		return memcmp(buf + begin_i, other.buf + begin_i, l) < 0;
	}

	friend bool operator<(const StrKey &l, const StrKey &r) {
		return memcmp(&l.buf, &r.buf, len) < 0;
	}
	friend bool operator>(const StrKey &l, const StrKey &r) {
		return memcmp(&l.buf, &r.buf, len) > 0;
	}
	friend bool operator>=(const StrKey &l, const StrKey &r) {
		return memcmp(&l.buf, &r.buf, len) >= 0;
	}
	friend bool operator<=(const StrKey &l, const StrKey &r) {
		return memcmp(&l.buf, &r.buf, len) <= 0;
	}
	friend bool operator==(const StrKey &l, const StrKey &r) {
		return memcmp(&l.buf, &r.buf, len) == 0;
	}
	friend bool operator!=(const StrKey &l, const StrKey &r) {
		return memcmp(&l.buf, &r.buf, len) != 0;
	}

	friend std::ostream &operator<<(std::ostream &os, const StrKey &key) {
		os << "key [" << std::hex;
		for (size_t i = 0; i < sizeof(StrKey); i++) {
			os << "0x" << key.buf[i] << " ";
		}
		os << "] (as byte)" << std::dec;
		return os;
	}

	uint8_t buf[len];
} PACKED;

typedef StrKey<64> index_key_t;
typedef sindex::SIndex<index_key_t, uint64_t> sindex_t;

struct alignas(CACHELINE_SIZE) FGParam;

typedef FGParam fg_param_t;

struct alignas(CACHELINE_SIZE) FGParam {
  sindex_t *table;
  uint64_t throughput;
  uint32_t thread_id;
};
// parameters
double read_ratio = 1;
double insert_ratio = 0;
double update_ratio = 0;
double delete_ratio = 0;
double scan_ratio = 0;
size_t table_size = 1000000;
size_t runtime = 10;
size_t fg_n = 1;
size_t bg_n = 1;
uint64_t dummy_value = 1234;
int op_per_thread = 0;

volatile bool running = false;
std::atomic<size_t> ready_threads(0);
std::vector<index_key_t> exist_keys;
std::vector<index_key_t> non_exist_keys;
utils::Timer<double> timer;

extern std::string test_type;;
void *run_fg(void *arguments);
void run_benchmark(size_t sec, ycsbc::CoreWorkload* workload_, ycsbc::DB* db_);

class args {
	public:
		void *param;
		ycsbc::CoreWorkload* workload_;
		ycsbc::DB* db_;
		int op_num;
		
		args(void *param, ycsbc::CoreWorkload* workload_, ycsbc::DB* db_, int op_num)
		{
			this->param = param;
			this->workload_ = workload_;
			this->db_ = db_;
			this->op_num = op_num;
		}
};

namespace ycsbc {

class SIndexDB : public DB {
 public:
  
	SIndexDB(){}

	SIndexDB(std::vector<std::vector<uint64_t>> keys)
	{
		if(keys.size() != 0) prepare_sindex(tab_xi, keys);
	}

	~SIndexDB(){
		// COUT_THIS("~SIndexDB()");
		if(tab_xi != nullptr) delete tab_xi;
	}

  int Read(const std::string &table, const std::string &key,
           const std::vector<std::string> *fields,
           std::vector<KVPair> &result);

  int Scan(const std::string &table, const std::string &key,
           int len, const std::vector<std::string> *fields,
           std::vector<std::vector<KVPair>> &result);

  int Update(const std::string &table, const std::string &key,
             std::vector<KVPair> &values);

  int Insert(const std::string &table, const std::string &key,
             std::vector<KVPair> &values);

  int Delete(const std::string &table, const std::string &key);

	sindex_t *get_tabxi() { return tab_xi; }
	
 private:
  sindex_t *tab_xi;
  uint32_t thread_id;
  inline void prepare_sindex(sindex_t *&table, std::vector<std::vector<uint64_t>> keys)
	{
		// prepare data
		exist_keys.reserve(keys.size());
		for (size_t i = 0; i < keys.size(); ++i) {
			// exist_keys.push_back(*reinterpret_cast<index_key_t*>(keys[i].data()));
			exist_keys.push_back(index_key_t(keys[i]));
		}
		COUT_VAR(exist_keys.size());
		COUT_VAR(non_exist_keys.size());

		// initilize SIndex (sort keys first)
		std::sort(exist_keys.begin(), exist_keys.end());
		std::vector<uint64_t> vals(exist_keys.size(), 1);
		table = new sindex_t(exist_keys, vals, fg_n, bg_n);
	}

};

} // ycsbc


int SIndexDB::Read(const std::string &table, const std::string &key,
           const std::vector<std::string> *fields,
           std::vector<KVPair> &result)
{
	uint64_t val;
	// uint64_t tmp_key = stoull(key.substr(4));
	// uint64_t tmp_key[1] = {stoull(key.substr(4))};
	tab_xi->get(*reinterpret_cast<const index_key_t*>(&key), dummy_value, (uint32_t)gettid());
	// for(size_t i = 0; i < 10; i++){
	// 	std::string field = "field" + i;
	// 	StrKey<64>* strKey = new StrKey<64>(key + field);
	// 	tab_xi->get(*strKey, val, thread_id);
	// 	result.push_back(make_pair(key, std::to_string(val)));
	// }
	// result.push_back(make_pair(key, std::to_string(val)));
	return DB::kOK;
}

int SIndexDB::Scan(const std::string &table, const std::string &key,
           int len, const std::vector<std::string> *fields,
           std::vector<std::vector<KVPair>> &result)
{
	std::vector<std::pair<index_key_t, uint64_t>> res;
	// for(size_t i = 0; i < 10; i++){
	// 	res.clear();
	// 	std::string field = "field" + i;
	// 	StrKey<64>* strKey = new StrKey<64>(key + field);
	// 	tab_xi->scan(*strKey, len, res, thread_id);
	// }
	// uint64_t tmp_key = stoull(key.substr(4));
	// uint64_t tmp_key[1] = {stoull(key.substr(4))};
	tab_xi->scan(*reinterpret_cast<const index_key_t*>(&key), len, res, (uint32_t)gettid());
	// StrKey<64>* strKey = new StrKey<64>(key);
	// tab_xi->scan(*strKey, len, res, thread_id);

	return DB::kOK;
}

int SIndexDB::Update(const std::string &table, const std::string &key,
             std::vector<KVPair> &values)
{
	// uint64_t tmp_key = stoull(key.substr(4));
	// uint64_t tmp_key[1] = {stoull(key.substr(4))};
	uint64_t val = 1234;
	tab_xi->put(*reinterpret_cast<const index_key_t*>(&key), val, (uint32_t)gettid());
	return DB::kOK;
}

int SIndexDB::Insert(const std::string &table, const std::string &key,
             std::vector<KVPair> &values)
{
	// for(KVPair kv: values){
	// 	StrKey<64>* strKey = new StrKey<64>(key + kv.first);
	// 	std::string value = kv.second;
	// 	// COUT_THIS("key = " << kv.first << " value = " << kv.second);
	// 	uint64_t val = 1234;
	// 	tab_xi->put(*strKey, val, thread_id);
	// }
	// StrKey<64>* strKey = new StrKey<64>(key)
	// uint64_t tmp_key = stoull(key.substr(4));
	// uint64_t tmp_key[1] = {stoull(key.substr(4))};
	uint64_t val = 1234;
	tab_xi->put(*reinterpret_cast<const index_key_t*>(&key), val, (uint32_t)gettid());

	return DB::kOK;
}

int SIndexDB::Delete(const std::string &table, const std::string &key)
{
	// for(size_t i = 0; i < 10; i++){
	// 	std::string field = "field" + i;
	// 	StrKey<64>* strKey = new StrKey<64>(key + field);
	// 	tab_xi->remove(*strKey, thread_id);
	// }
	// StrKey<64>* strKey = new StrKey<64>(key);
	// uint64_t tmp_key = stoull(key.substr(4));
	// uint64_t tmp_key[1] = {stoull(key.substr(4))};
	tab_xi->remove(*reinterpret_cast<const index_key_t*>(&key), (uint32_t)gettid());
	return DB::kOK;
}

void *run_fg(void *arguments) {
	// args* args = arguments;
	void *param = ((args*)arguments)->param;
	ycsbc::CoreWorkload &workload_ = *(((args*)arguments)->workload_);
	ycsbc::DB &db_ = *(((args*)arguments)->db_);

	fg_param_t &thread_param = *(fg_param_t *)param;
	uint32_t thread_id = thread_param.thread_id;
	sindex_t *table = thread_param.table;

	COUT_THIS("[micro] Worker" << thread_id << " Ready.");
	ready_threads++;
	volatile bool res = false;
	uint64_t dummy_value = 1234;
	// sindex_t *tab = ((ycsbc::SIndexDB*)&db_)->get_tabxi();
	UNUSED(res);


	while (!running)
		;

	while (running) {
		switch (workload_.NextOperation()) {
			case READ:
			{
				// const std::string &key = workload_.NextTransactionKey();
				std::vector<uint64_t> key = workload_.NextTransactionKeyUint();
				// table->get(*reinterpret_cast<index_key_t*>(key.data()), dummy_value, thread_id);
				table->get(index_key_t(key), dummy_value, thread_id);
				break;
			}
			case UPDATE:
			{
				// const std::string &key = workload_.NextTransactionKey();
				std::vector<uint64_t> key = workload_.NextTransactionKeyUint();
				std::vector<DB::KVPair> values;
				// table->put(*reinterpret_cast<index_key_t*>(key.data()), dummy_value, thread_id);
				table->put(index_key_t(key), dummy_value, thread_id);
				break;
			}
			case INSERT:
			{
				// const std::string &key = workload_.NextSequenceKey();
				std::vector<uint64_t> key = workload_.NextTransactionKeyUint();
				// table->put(*reinterpret_cast<index_key_t*>(key.data()), dummy_value, thread_id);
				table->put(index_key_t(key), dummy_value, thread_id);
				break;
			}
			case SCAN:
			{
				// const std::string &key = workload_.NextTransactionKey();
				std::vector<uint64_t> key = workload_.NextTransactionKeyUint();
				int len = workload_.NextScanLength();
				std::vector<std::pair<index_key_t, uint64_t>> results;
				// table->scan(*reinterpret_cast<index_key_t*>(key.data()), len, results, thread_id);
				table->scan(index_key_t(key), len, results, thread_id);
				break;
			}
			case READMODIFYWRITE:
			{
				// const std::string &key = workload_.NextTransactionKey();
				std::vector<uint64_t> key = workload_.NextTransactionKeyUint();
				std::vector<DB::KVPair> result;
				// table->get(*reinterpret_cast<index_key_t*>(key.data()), dummy_value, thread_id);
				// table->put(*reinterpret_cast<index_key_t*>(key.data()), dummy_value, thread_id);
				table->get(index_key_t(key), dummy_value, thread_id);
				table->put(index_key_t(key), dummy_value, thread_id);
				break;
			}
			
			default:
				throw utils::Exception("Operation request is not recognized!");
		}
		thread_param.throughput++;
	}

	pthread_exit(nullptr);
}

void *run_fg2(void *arguments) {
	void *param = ((args*)arguments)->param;
	ycsbc::CoreWorkload &workload_ = *(((args*)arguments)->workload_);
	ycsbc::DB &db_ = *(((args*)arguments)->db_);
	int op_num = ((args*)arguments)->op_num;

	fg_param_t &thread_param = *(fg_param_t *)param;
	uint32_t thread_id = thread_param.thread_id;
	sindex_t *table = thread_param.table;

	COUT_THIS("[micro] Worker" << thread_id << " Ready.");
	ready_threads++;
	volatile bool res = false;
	uint64_t dummy_value = 1234;
	UNUSED(res);
	
	
	for(size_t i = 0; i < op_num; i++){
		switch (workload_.NextOperation()) {
			case READ:
			{
				std::vector<uint64_t> key = workload_.NextTransactionKeyUint();
				table->get(index_key_t(key), dummy_value, thread_id);
				break;
			}
			case UPDATE:
			{
				std::vector<uint64_t> key = workload_.NextTransactionKeyUint();
				std::vector<DB::KVPair> values;
				table->put(index_key_t(key), dummy_value, thread_id);
				break;
			}
			case INSERT:
			{
				std::vector<uint64_t> key = workload_.NextTransactionKeyUint();
				table->put(index_key_t(key), dummy_value, thread_id);
				break;
			}
			case SCAN:
			{
				std::vector<uint64_t> key = workload_.NextTransactionKeyUint();
				int len = workload_.NextScanLength();
				std::vector<std::pair<index_key_t, uint64_t>> results;
				table->scan(index_key_t(key), len, results, thread_id);
				break;
			}
			case READMODIFYWRITE:
			{
				std::vector<uint64_t> key = workload_.NextTransactionKeyUint();
				std::vector<DB::KVPair> result;
				table->get(index_key_t(key), dummy_value, thread_id);
				table->put(index_key_t(key), dummy_value, thread_id);
				break;
			}
			
			default:
				throw utils::Exception("Operation request is not recognized!");
		}
		thread_param.throughput++;
	}
	pthread_exit(nullptr);
}

void run_benchmark(size_t sec, ycsbc::CoreWorkload* workload_, ycsbc::DB* db_) {
	pthread_t threads[fg_n];
	fg_param_t fg_params[fg_n];
	// check if parameters are cacheline aligned
	for (size_t i = 0; i < fg_n; i++) {
		if ((uint64_t)(&(fg_params[i])) % CACHELINE_SIZE != 0) {
			COUT_N_EXIT("wrong parameter address: " << &(fg_params[i]));
		}
	}

	running = false;
	for (size_t worker_i = 0; worker_i < fg_n; worker_i++) {
		fg_params[worker_i].table = ((ycsbc::SIndexDB*)db_)->get_tabxi();
		fg_params[worker_i].thread_id = worker_i;
		fg_params[worker_i].throughput = 0;

		args* a = new args((void *)&fg_params[worker_i], workload_, db_, 0); // 操作数没有用

		int ret = pthread_create(&threads[worker_i], nullptr, run_fg, (void*)a);
		if (ret) {
			COUT_N_EXIT("Error:" << ret);
		}
	}

	COUT_THIS("[micro] prepare data ...");
	while (ready_threads < fg_n) sleep(1);

	running = true;
	std::vector<size_t> tput_history(fg_n, 0);
	size_t current_sec = 0;
	while (current_sec < sec) {
		sleep(1);
		uint64_t tput = 0;
		for (size_t i = 0; i < fg_n; i++) {
			tput += fg_params[i].throughput - tput_history[i];
			tput_history[i] = fg_params[i].throughput;
		}
		COUT_THIS("[micro] >>> sec " << current_sec << " throughput: " << tput);
		++current_sec;
	}

	running = false;
	void *status;
	for (size_t i = 0; i < fg_n; i++) {
		int rc = pthread_join(threads[i], &status);
		if (rc) {
			COUT_N_EXIT("Error:unable to join," << rc);
		}
	}

	size_t throughput = 0;
	for (auto &p : fg_params) {
		throughput += p.throughput;
	}
	COUT_THIS("[micro] Throughput(op/s): " << throughput / sec);
}

void run_benchmark2(ycsbc::CoreWorkload* workload_, ycsbc::DB* db_, int op_num) {
	pthread_t threads[fg_n];
	fg_param_t fg_params[fg_n];
	// check if parameters are cacheline aligned
	for (size_t i = 0; i < fg_n; i++) {
		if ((uint64_t)(&(fg_params[i])) % CACHELINE_SIZE != 0) {
			COUT_N_EXIT("wrong parameter address: " << &(fg_params[i]));
		}
	}

	running = false;
	op_per_thread = op_num / fg_n;
	for (size_t worker_i = 0; worker_i < fg_n; worker_i++) {
		fg_params[worker_i].table = ((ycsbc::SIndexDB*)db_)->get_tabxi();
		fg_params[worker_i].thread_id = worker_i;
		fg_params[worker_i].throughput = 0;

		args* a = new args((void *)&fg_params[worker_i], workload_, db_, op_per_thread);
		// COUT_THIS("create run_fg2");
		int ret = pthread_create(&threads[worker_i], nullptr, run_fg2, (void*)a);
		if (ret) {
			COUT_N_EXIT("Error:" << ret);
		}
	}

	COUT_THIS("[micro] prepare data ...");
	while (ready_threads < fg_n) sleep(1);

	timer.Start();
	void *status;
	for (size_t i = 0; i < fg_n; i++) {
		int rc = pthread_join(threads[i], &status);
		if (rc) {
			COUT_N_EXIT("Error:unable to join," << rc);
		}
		ready_threads--;
	}
	double duration = timer.End();

	size_t throughput = 0;
	for (auto &p : fg_params) {
		throughput += p.throughput;
	}

	COUT_VAR(op_num);
	COUT_VAR(duration);
	COUT_VAR(throughput);

	COUT_THIS("[micro] Throughput(op/s): " << 1.0 * op_num / duration);
	while(ready_threads != 0) continue;
  // while(ready_bgs != 0) continue;
	((ycsbc::SIndexDB*)db_)->get_tabxi()->terminate_bg();
}

#endif // YCSB_C_SINDEX_DB_H_