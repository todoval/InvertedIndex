
#include <vector>
#include <list>
#include <set>
#include <random>
#include <string>
#include <stdexcept>
#include <cstdint>
#include <cstddef>

/*
 * Toy implementation of storage, use for testing on small data on Windows.
 * The memory-mapped storage is compatible with this.
 */
class storage
{
	std::vector<uint64_t> d;
public:
	uint64_t* data() {
		return d.data();
	}

	size_t size() const {
		return d.size();
	}

	void drop() {
		d.clear();
	}

	uint64_t* operator() (size_t s) {
		d.resize (s);
		return data();
	}
};

#if defined (__unix__) || (defined (__APPLE__) && defined (__MACH__))
/*
 * Real implementation of memory-mapped storage used in ReCodex, usuable on
 * Unixes and probably on MinGW/CygWin.
 */

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>

class disk_storage
{
	int fd;
	uint64_t* d;
	size_t s;

	void resize (size_t size) {
		ftruncate (fd, size * sizeof (uint64_t));
		s = size;
	}

	void map() {
		if (!s) {
			d = 0;
			return;
		}
		d = (uint64_t*) mmap (nullptr,
		                      s * sizeof (uint64_t),
		                      PROT_READ | PROT_WRITE,
		                      MAP_SHARED,
		                      fd,
		                      0);
		if (d == (void*) -1) {
			perror ("mmap");
			throw std::runtime_error ("mmap failed");
		}

	}

	void unmap() {
		if (s && munmap (d, s * sizeof (uint64_t))) {
			perror ("munmap");
			throw std::runtime_error ("munmap failed");
		}
	}
public:
	uint64_t* data() {
		return d;
	}

	size_t size() const {
		return s;
	}

	void drop() {
		unmap();
		resize (0);
		map();
	}

	uint64_t* operator() (size_t s) {
		unmap();
		resize (s);
		map();
		return d;
	}

	disk_storage (const std::string&fn) {
		fd = open (fn.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
		if (fd < 0) {
			perror ("open");
			throw std::runtime_error ("could not open index file");
		}
		struct stat st;
		if (fstat (fd, &st)) {
			perror ("stat");
			throw std::runtime_error ("could not stat() the index file");
		}
		s = st.st_size / sizeof (uint64_t);
		map();
	}

	~disk_storage() {
		unmap();
		close (fd);
	}

	disk_storage (const disk_storage&) = delete;
	disk_storage& operator= (const disk_storage&) = delete;
	disk_storage (disk_storage&&) = delete;
	disk_storage& operator= (disk_storage&&) = delete;
};
#else
#define primitive_storage
#endif

/*
 * data generator
 */
template<class Params>
class feature_objects_generator
{
	class iterator
	{
		bool end;
		uint64_t val;
		std::mt19937 mt;
		std::uniform_int_distribution<uint64_t> uid;
		uint64_t increment() {
			return uid (mt) / Params::incr_div();
		}
	public:
		uint64_t operator*() const {
			return val;
		}

		iterator& operator++() {
			val += 1 + increment();
			if (val >= Params::max_objs()) end = true;
			return *this;
		}

		bool operator== (const iterator & a) const {
			return end == a.end;
		}

		bool operator!= (const iterator & a) const {
			return end != a.end;
		}

		iterator (bool end, size_t n)
			: end (end),
			  mt (Params::seed() + n),
			  uid (0, Params::max_incr()) {
			val = increment();
		}

	};

	class iterator_proxy
	{
		uint64_t feat;
	public:
		iterator_proxy (uint64_t feat) : feat (feat) {}

		iterator begin() const {
			return iterator (false, feat);
		}

		iterator end() const {
			return iterator (true, feat);
		}
	};

public:
	iterator_proxy operator[] (uint64_t feat) const {
		return iterator_proxy (feat);
	}

	uint64_t size() const {
		return Params::n_feats();
	}
};

#include "inverted_index.hpp"
#include "params.hpp"


int main()
{
//#define primitive_generator
#ifdef primitive_generator
	std::vector<std::list<uint64_t>> fs;
	fs.resize (5);
	fs[0].push_back (5);
	fs[1].push_back (5);
	fs[2].push_back (5);
	fs[3].push_back (5);
	fs[4].push_back (4);
	fs[4].push_back (5);
	fs[4].push_back (6);
#else
	feature_objects_generator<generator_params> fs;
#endif

#ifdef primitive_storage
	storage s;
	{
		storage s2;
		ii::create (s2, fs);
		//this simulates the storage effect (breaks the pointers)
		s = s2;
	}
#else
	{
		disk_storage s ("test.dat");
		ii::create (s, fs);
	}
	//reopen the storage (also mostly for breaking the pointers)
	disk_storage s ("test.dat");
#endif

	std::mt19937 mt (generator_params::seed()
	                 + generator_params::max_objs() + 1);
	
	std::uniform_int_distribution<uint64_t>
	uid (0, generator_params::n_feats() - 1);

	std::set<uint64_t> query;
	for (size_t i = 0; i < generator_params::query_size(); ++i)
		query.insert (uid (mt));
	//query.insert(0);

	std::cout << generator_params::result_ident() << std::endl;

	ii::search (s.data(), s.size(), query,
	[] (uint64_t f) {
		std::cout << f << std::endl;
	});

	return 0;
}
