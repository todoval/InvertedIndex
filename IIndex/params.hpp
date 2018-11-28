
#include <cstdint>
#include <cstddef>

struct generator_params {
	static size_t n_feats() {
		return 10;
	}
	static uint64_t max_incr() {
		return 150;
	}
	static uint64_t incr_div() {
		return 100;
	}
	static size_t max_objs() {
		return 100;
	}
	static uint64_t seed() {
		return 123;
	}
	static size_t query_size() {
		return 5;
	}
	static const char* result_ident() {
		return "results start here";
	}
};
