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

using std::string;

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
	StrKey(uint64_t key) { COUT_N_EXIT("str key no uint64"); }
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

volatile bool running = false;
std::atomic<size_t> ready_threads(0);
std::vector<index_key_t> exist_keys;
std::vector<index_key_t> non_exist_keys;

void *run_fg(void *arguments);


// class args {
// 	public:
// 		void *param;
// 		ycsbc::CoreWorkload* workload_;
// 		ycsbc::DB* db_;
		
// 		args(void *param, ycsbc::CoreWorkload* workload_, ycsbc::DB* db_)
// 		{
// 			this->param = param;
// 			this->workload_ = workload_;
// 			this->db_ = db_;
// 		}

// };

namespace ycsbc {

class SIndexDB : public DB {
 public:
  
	SIndexDB()
	{
		// prepare_sindex(tab_xi);
	}

	SIndexDB(std::vector<std::string> keys)
	{
		if(keys.size() != 0) prepare_sindex(tab_xi, keys);
	}

  int Read(const std::string &table, const std::string &key,
           const std::vector<std::string> *fields,
           std::vector<KVPair> &result)
	{
		pid_t tid = gettid();
		thread_id = (uint32_t)tid;
		// COUT_THIS("thread_id = " << thread_id);
		uint64_t val;
		// for(size_t i = 0; i < 10; i++){
		// 	std::string field = "field" + i;
		// 	StrKey<64>* strKey = new StrKey<64>(key + field);
		// 	tab_xi->get(*strKey, val, thread_id);
		// 	result.push_back(make_pair(key, std::to_string(val)));
		// }

		StrKey<64>* strKey = new StrKey<64>(key);
		tab_xi->get(*strKey, val, thread_id);
		result.push_back(make_pair(key, std::to_string(val)));

		return DB::kOK;
	}

  int Scan(const std::string &table, const std::string &key,
           int len, const std::vector<std::string> *fields,
           std::vector<std::vector<KVPair>> &result)
	{
		pid_t tid = gettid();
		thread_id = (uint32_t)tid;
		// COUT_THIS("thread_id = " << thread_id);
		std::vector<std::pair<index_key_t, uint64_t>> res;

		// for(size_t i = 0; i < 10; i++){
		// 	res.clear();
		// 	std::string field = "field" + i;
		// 	StrKey<64>* strKey = new StrKey<64>(key + field);
		// 	tab_xi->scan(*strKey, len, res, thread_id);
		// }

		StrKey<64>* strKey = new StrKey<64>(key);
		tab_xi->scan(*strKey, len, res, thread_id);

		return DB::kOK;
	}

  int Update(const std::string &table, const std::string &key,
             std::vector<KVPair> &values)
	{
		// COUT_THIS("Update tab_key = " << key << "key = " << values[0].first << " values " << values[0].second);
		pid_t tid = gettid();
		thread_id = (uint32_t)tid;
		// COUT_THIS("thread_id = " << thread_id);
		// StrKey<64>* strKey = new StrKey<64>(key + values[0].first);
		StrKey<64>* strKey = new StrKey<64>(key);
		uint64_t val = 1234;
		tab_xi->put(*strKey, val, thread_id);
		return DB::kOK;
	}

  int Insert(const std::string &table, const std::string &key,
             std::vector<KVPair> &values)
	{
		// COUT_THIS("Insert tab_key = " << key);
		pid_t tid = gettid();
		thread_id = (uint32_t)tid;
		// COUT_THIS("thread_id = " << thread_id);
		// for(KVPair kv: values){
		// 	StrKey<64>* strKey = new StrKey<64>(key + kv.first);
		// 	std::string value = kv.second;
		// 	// COUT_THIS("key = " << kv.first << " value = " << kv.second);
		// 	uint64_t val = 1234;
		// 	tab_xi->put(*strKey, val, thread_id);
		// }

		StrKey<64>* strKey = new StrKey<64>(key);
		uint64_t val = 1234;
		tab_xi->put(*strKey, val, thread_id);

		return DB::kOK;
	}

  int Delete(const std::string &table, const std::string &key)
	{
		pid_t tid = gettid();
		thread_id = (uint32_t)tid;
		// COUT_THIS("thread_id = " << thread_id);
		// for(size_t i = 0; i < 10; i++){
		// 	std::string field = "field" + i;
		// 	StrKey<64>* strKey = new StrKey<64>(key + field);
		// 	tab_xi->remove(*strKey, thread_id);
		// }

		StrKey<64>* strKey = new StrKey<64>(key);

		tab_xi->remove(*strKey, thread_id);
		return DB::kOK;
	}

	// void run_benchmark(size_t sec, CoreWorkload* workload_) {
	// 	pthread_t threads[fg_n];
	// 	fg_param_t fg_params[fg_n];
	// 	// check if parameters are cacheline aligned
	// 	for (size_t i = 0; i < fg_n; i++) {
	// 		if ((uint64_t)(&(fg_params[i])) % CACHELINE_SIZE != 0) {
	// 			COUT_N_EXIT("wrong parameter address: " << &(fg_params[i]));
	// 		}
	// 	}

	// 	running = false;
	// 	for (size_t worker_i = 0; worker_i < fg_n; worker_i++) {
	// 		fg_params[worker_i].table = tab_xi;
	// 		fg_params[worker_i].thread_id = worker_i;
	// 		fg_params[worker_i].throughput = 0;

	// 		args* a = new args((void *)&fg_params[worker_i], workload_, this);
	// 		// a->param = ;
	// 		// a->workload_ = ;
	// 		// a->db_ = this;

	// 		int ret = pthread_create(&threads[worker_i], nullptr, run_fg,
	// 														(void*)a);
	// 		if (ret) {
	// 			COUT_N_EXIT("Error:" << ret);
	// 		}
	// 	}

	// 	COUT_THIS("[micro] prepare data ...");
	// 	while (ready_threads < fg_n) sleep(1);

	// 	running = true;
	// 	std::vector<size_t> tput_history(fg_n, 0);
	// 	size_t current_sec = 0;
	// 	while (current_sec < sec) {
	// 		sleep(1);
	// 		uint64_t tput = 0;
	// 		for (size_t i = 0; i < fg_n; i++) {
	// 			tput += fg_params[i].throughput - tput_history[i];
	// 			tput_history[i] = fg_params[i].throughput;
	// 		}
	// 		COUT_THIS("[micro] >>> sec " << current_sec << " throughput: " << tput);
	// 		++current_sec;
	// 	}

	// 	running = false;
	// 	void *status;
	// 	for (size_t i = 0; i < fg_n; i++) {
	// 		int rc = pthread_join(threads[i], &status);
	// 		if (rc) {
	// 			COUT_N_EXIT("Error:unable to join," << rc);
	// 		}
	// 	}

	// 	size_t throughput = 0;
	// 	for (auto &p : fg_params) {
	// 		throughput += p.throughput;
	// 	}
	// 	COUT_THIS("[micro] Throughput(op/s): " << throughput / sec);
	// }

 private:
  sindex_t *tab_xi;
  uint32_t thread_id;
  inline void parse_args(int argc, char **argv)
	{
		struct option long_options[] = {
				{"read", required_argument, 0, 'a'},
				{"insert", required_argument, 0, 'b'},
				{"remove", required_argument, 0, 'c'},
				{"update", required_argument, 0, 'd'},
				{"scan", required_argument, 0, 'e'},
				{"table-size", required_argument, 0, 'f'},
				{"runtime", required_argument, 0, 'g'},
				{"fg", required_argument, 0, 'h'},
				{"bg", required_argument, 0, 'i'},
				{"sindex-root-err-bound", required_argument, 0, 'j'},
				{"sindex-root-memory", required_argument, 0, 'k'},
				{"sindex-group-err-bound", required_argument, 0, 'l'},
				{"sindex-group-err-tolerance", required_argument, 0, 'm'},
				{"sindex-buf-size-bound", required_argument, 0, 'n'},
				{"sindex-buf-compact-threshold", required_argument, 0, 'o'},
				{"sindex-partial-len", required_argument, 0, 'p'},
				{"sindex-forward-step", required_argument, 0, 'q'},
				{"sindex-backward-step", required_argument, 0, 'r'},
				{0, 0, 0, 0}};
		std::string ops = "a:b:c:d:e:f:g:h:i:j:k:l:m:n:o:p:q:r:";
		int option_index = 0;

		while (1) {
			int c = getopt_long(argc, argv, ops.c_str(), long_options, &option_index);
			if (c == -1) break;

			switch (c) {
				case 0:
					if (long_options[option_index].flag != 0) break;
					abort();
					break;
				case 'a':
					read_ratio = strtod(optarg, NULL);
					INVARIANT(read_ratio >= 0 && read_ratio <= 1);
					break;
				case 'b':
					insert_ratio = strtod(optarg, NULL);
					INVARIANT(insert_ratio >= 0 && insert_ratio <= 1);
					break;
				case 'c':
					delete_ratio = strtod(optarg, NULL);
					INVARIANT(delete_ratio >= 0 && delete_ratio <= 1);
					break;
				case 'd':
					update_ratio = strtod(optarg, NULL);
					INVARIANT(update_ratio >= 0 && update_ratio <= 1);
					break;
				case 'e':
					scan_ratio = strtod(optarg, NULL);
					INVARIANT(scan_ratio >= 0 && scan_ratio <= 1);
					break;
				case 'f':
					table_size = strtoul(optarg, NULL, 10);
					INVARIANT(table_size > 0);
					break;
				case 'g':
					runtime = strtoul(optarg, NULL, 10);
					INVARIANT(runtime > 0);
					break;
				case 'h':
					fg_n = strtoul(optarg, NULL, 10);
					INVARIANT(fg_n > 0);
					break;
				case 'i':
					bg_n = strtoul(optarg, NULL, 10);
					break;
				case 'j':
					sindex::config.root_error_bound = strtol(optarg, NULL, 10);
					INVARIANT(sindex::config.root_error_bound > 0);
					break;
				case 'k':
					sindex::config.root_memory_constraint =
							strtol(optarg, NULL, 10) * 1024 * 1024;
					INVARIANT(sindex::config.root_memory_constraint > 0);
					break;
				case 'l':
					sindex::config.group_error_bound = strtol(optarg, NULL, 10);
					INVARIANT(sindex::config.group_error_bound > 0);
					break;
				case 'm':
					sindex::config.group_error_tolerance = strtol(optarg, NULL, 10);
					INVARIANT(sindex::config.group_error_tolerance > 0);
					break;
				case 'n':
					sindex::config.buffer_size_bound = strtol(optarg, NULL, 10);
					INVARIANT(sindex::config.buffer_size_bound > 0);
					break;
				case 'o':
					sindex::config.buffer_compact_threshold = strtol(optarg, NULL, 10);
					INVARIANT(sindex::config.buffer_compact_threshold > 0);
					break;
				case 'p':
					sindex::config.partial_len_bound = strtol(optarg, NULL, 10);
					INVARIANT(sindex::config.partial_len_bound > 0);
					break;
				case 'q':
					sindex::config.forward_step = strtol(optarg, NULL, 10);
					INVARIANT(sindex::config.forward_step > 0);
					break;
				case 'r':
					sindex::config.backward_step = strtol(optarg, NULL, 10);
					INVARIANT(sindex::config.backward_step > 0);
					break;

				default:
					abort();
			}
		}

		COUT_THIS("[micro] Read:Insert:Update:Delete:Scan = "
							<< read_ratio << ":" << insert_ratio << ":" << update_ratio << ":"
							<< delete_ratio << ":" << scan_ratio)
		double ratio_sum =
				read_ratio + insert_ratio + delete_ratio + scan_ratio + update_ratio;
		INVARIANT(ratio_sum > 0.9999 && ratio_sum < 1.0001);  // avoid precision lost
		COUT_VAR(runtime);
		COUT_VAR(fg_n);
		COUT_VAR(bg_n);
		COUT_VAR(sindex::config.root_error_bound);
		COUT_VAR(sindex::config.root_memory_constraint);
		COUT_VAR(sindex::config.group_error_bound);
		COUT_VAR(sindex::config.group_error_tolerance);
		COUT_VAR(sindex::config.buffer_size_bound);
		COUT_VAR(sindex::config.buffer_size_tolerance);
		COUT_VAR(sindex::config.buffer_compact_threshold);
		COUT_VAR(sindex::config.partial_len_bound);
		COUT_VAR(sindex::config.forward_step);
		COUT_VAR(sindex::config.backward_step);
	}

  inline void prepare_sindex(sindex_t *&table, std::vector<std::string> keys)
	{
		// prepare data
		std::random_device rd;
		std::mt19937 gen(rd());
		std::uniform_int_distribution<int64_t> rand_int8(
				0, std::numeric_limits<uint8_t>::max());
		exist_keys.reserve(keys.size());
		for (size_t i = 0; i < keys.size(); ++i) {
			index_key_t* k = new index_key_t(keys[i]);
			exist_keys.push_back(*k);
		}
		COUT_VAR(exist_keys.size());
		COUT_VAR(non_exist_keys.size());

		// initilize SIndex (sort keys first)
		std::sort(exist_keys.begin(), exist_keys.end());
		std::vector<uint64_t> vals(exist_keys.size(), 1);
		table = new sindex_t(exist_keys, vals, fg_n, bg_n);
	}

	
	

};

// void *run_fg(void *arguments) {
// 	// args* args = arguments;
// 	void *param = ((args*)arguments)->param;
// 	ycsbc::CoreWorkload &workload_ = *(((args*)arguments)->workload_);
// 	ycsbc::DB &db_ = *(((args*)arguments)->db_);

// 	fg_param_t &thread_param = *(fg_param_t *)param;
// 	uint32_t thread_id = thread_param.thread_id;
// 	sindex_t *table = thread_param.table;

// 	std::random_device rd;
// 	std::mt19937 gen(rd());
// 	std::uniform_real_distribution<> ratio_dis(0, 1);

// 	size_t exist_key_n_per_thread = exist_keys.size() / fg_n;
// 	size_t exist_key_start = thread_id * exist_key_n_per_thread;
// 	size_t exist_key_end = (thread_id + 1) * exist_key_n_per_thread;
// 	std::vector<index_key_t> op_keys(exist_keys.begin() + exist_key_start,
// 																	exist_keys.begin() + exist_key_end);

// 	if (non_exist_keys.size() > 0) {
// 		size_t non_exist_key_n_per_thread = non_exist_keys.size() / fg_n;
// 		size_t non_exist_key_start = thread_id * non_exist_key_n_per_thread,
// 					non_exist_key_end = (thread_id + 1) * non_exist_key_n_per_thread;
// 		op_keys.insert(op_keys.end(), non_exist_keys.begin() + non_exist_key_start,
// 									non_exist_keys.begin() + non_exist_key_end);
// 	}

// 	COUT_THIS("[micro] Worker" << thread_id << " Ready.");
// 	size_t query_i = 0, insert_i = op_keys.size() / 2, delete_i = 0, update_i = 0;
// 	// exsiting keys fall within range [delete_i, insert_i)
// 	ready_threads++;
// 	volatile bool res = false;
// 	uint64_t dummy_value = 1234;
// 	UNUSED(res);

// 	while (!running)
// 		;

// 	while (running) {
// 		switch (workload_.NextOperation()) {
// 			case READ:
// 			{
// 				const std::string &table = workload_.NextTable();
// 				const std::string &key = workload_.NextTransactionKey();
// 				std::vector<DB::KVPair> result;
// 				if (!workload_.read_all_fields()) {
// 					std::vector<std::string> fields;
// 					fields.push_back("field" + workload_.NextFieldName());
// 					db_.Read(table, key, &fields, result);
// 				} else {
// 					db_.Read(table, key, NULL, result);
// 				}
// 				break;
// 			}
// 			case UPDATE:
// 			{
// 				const std::string &table = workload_.NextTable();
// 				const std::string &key = workload_.NextTransactionKey();
// 				std::vector<DB::KVPair> values;
// 				if (workload_.write_all_fields()) {
// 					workload_.BuildValues(values);
// 				} else {
// 					workload_.BuildUpdate(values);
// 				}
// 				db_.Update(table, key, values);
// 				break;
// 			}
				
// 			case INSERT:
// 			{
// 				const std::string &table = workload_.NextTable();
// 				const std::string &key = workload_.NextSequenceKey();
// 				std::vector<DB::KVPair> values;
// 				workload_.BuildValues(values);
// 				db_.Insert(table, key, values);
// 				break;
// 			}
				
// 			case SCAN:
// 			{
// 				const std::string &table = workload_.NextTable();
// 				const std::string &key = workload_.NextTransactionKey();
// 				int len = workload_.NextScanLength();
// 				std::vector<std::vector<DB::KVPair>> result;
// 				if (!workload_.read_all_fields()) {
// 					std::vector<std::string> fields;
// 					fields.push_back("field" + workload_.NextFieldName());
// 					db_.Scan(table, key, len, &fields, result);
// 				} else {
// 					db_.Scan(table, key, len, NULL, result);
// 				}
// 				break;
// 			}
				
// 			case READMODIFYWRITE:
// 			{
// 				const std::string &table = workload_.NextTable();
// 				const std::string &key = workload_.NextTransactionKey();
// 				std::vector<DB::KVPair> result;

// 				if (!workload_.read_all_fields()) {
// 					std::vector<std::string> fields;
// 					fields.push_back("field" + workload_.NextFieldName());
// 					db_.Read(table, key, &fields, result);
// 				} else {
// 					db_.Read(table, key, NULL, result);
// 				}

// 				std::vector<DB::KVPair> values;
// 				if (workload_.write_all_fields()) {
// 					workload_.BuildValues(values);
// 				} else {
// 					workload_.BuildUpdate(values);
// 				}
// 				db_.Update(table, key, values);
// 				break;
// 			}
				
// 			default:
// 				throw utils::Exception("Operation request is not recognized!");
// 		}
// 		thread_param.throughput++;
// 	}

// 	pthread_exit(nullptr);
// }

} // ycsbc

#endif // YCSB_C_SINDEX_DB_H_