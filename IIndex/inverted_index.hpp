
#ifndef _ii_hpp
#define _ii_hpp
#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include <vector>
#include <queue>
#include <condition_variable>
#include <future>
#include <stdexcept>
#include <iostream>

namespace ii_impl
{
	class ThreadPool { //the threadPool class used for parallel run
	public:
		ThreadPool(size_t);
		template<class F, class... Args>
		auto enqueue(F&& f, Args&&... args)->std::future<typename std::result_of<F(Args...)>::type>;
		~ThreadPool();
	private:
		std::vector< std::thread > workers; //all the workers for join later
		std::queue< std::function<void()> > tasks; //the task queue
		std::atomic_flag lock = ATOMIC_FLAG_INIT;
		bool stop;
	};
	inline ThreadPool::ThreadPool(size_t threads) //constructor launches 'threads' number of workers
		: stop(false)
	{
		for (size_t i = 0; i<threads; ++i)
			workers.emplace_back(
				[this]
		{
			for (;;)
			{
				std::function<void()> task;
				while (lock.test_and_set() && !stop) {};
				if (this->stop && this->tasks.empty()) return;
				if (!tasks.empty())
				{
					task = std::move(this->tasks.front());
					this->tasks.pop();
				}
				lock.clear();
				if (task) task();
			}
		}
		);
	}
	template<class F, class... Args>
	auto ThreadPool::enqueue(F&& f, Args&&... args) // add new work item to the pool
		-> std::future<typename std::result_of<F(Args...)>::type>
	{
		using return_type = typename std::result_of<F(Args...)>::type;
		auto task = std::make_shared< std::packaged_task<return_type()> >(
			std::bind(std::forward<F>(f), std::forward<Args>(args)...)
			);

		std::future<return_type> res = task->get_future(); // don't allow enqueueing after stopping the pool
		{
			if (stop) throw std::runtime_error("enqueue on stopped ThreadPool");
			tasks.emplace([task]() { (*task)(); });
		}
		return res;
	}
	inline ThreadPool::~ThreadPool() //destructor, joins all threads
	{
		while (lock.test_and_set()) {};
		stop = true;
		lock.clear(); //clear lock
		for (std::thread &worker : workers) //join all threads upon destruction
			worker.join();
	}
}


#endif
namespace ii
{

	struct postingList //struct that saves offsets for the first iteration
	{
	public:
		uint64_t offsetBegin;
		uint64_t offsetEnd;
		postingList(uint64_t begin, uint64_t end)
		{
			offsetBegin = begin;
			offsetEnd = end;
		}
	};

	template<typename Truncate, typename FeatureObjectLists>
	void create(Truncate&& truncate, FeatureObjectLists&& features)
	{
		uint64_t *pointerToData = truncate(sizeof(uint64_t) / 8 * (1 + features.size())); //reserve for offsets and feature size
		*pointerToData = features.size(); //add size of feature to the beginning
		uint64_t count = 0; //how much data will be reserved (count of all the documents that are in the file)
		for (uint64_t i = 0; i < features.size(); i++) //adding offsets
		{
			*(pointerToData + i + 1) = count + 1 + features.size(); //save offset to the start of the feature objects from the beginning
			for (auto it = features[i].begin(); it != features[i].end(); ++it) //iteration over the objects of the feature
			{
				pointerToData = truncate(sizeof(uint64_t) / 8 * (1 + features.size() + count + 1)); //reserve new memory for the new feature object
				*(pointerToData + features.size() + 1 + count) = *it; //add the new object to the end of the file
				count++; //increment count
			}
		}
	}

	template<class Fs, class OutFn>
	void search(const uint64_t*segment, size_t size, Fs&& fs, OutFn&& callback)
	{
		ii_impl::ThreadPool pool(std::thread::hardware_concurrency()); //declaration of threadpool
		std::queue<std::vector<uint64_t>> buffer; //buffer of intersections
		std::queue<postingList> firstIteration; //queue of queries that are saved only with the offsets - first iteration
		for (auto &&f : fs) //first iteration, where it will be iterated only with the help of offsets
		{
			uint64_t begin = (uint64_t) *(segment + f + 1); //the starting offset
			uint64_t end; //the ending offset
			if (f == (uint64_t)*segment - 1) //wether the query is not at the end of the list
				end = size;
			else end = (uint64_t) *(segment + f + 2);
			firstIteration.emplace(begin, end); //add the query to the queue
		}
		std::vector<std::future<std::vector<uint64_t>>> results; //newly created intersection lists
		while (firstIteration.size() > 1) //iterating through the first vectors and finding intersections of their pairs
		{
			auto firstElem = firstIteration.front(); //get the first element
			firstIteration.pop(); //delete the first element from the queue
			auto secondElem = firstIteration.front(); //get the second element
			firstIteration.pop(); //delete the second element from the queue
			results.emplace_back(pool.enqueue([&pool, firstElem, secondElem, segment]() //lambda function to find the value max in feature f
			{
				std::vector<uint64_t> toRet; //vector of the items that are in the intersection
				uint64_t i = 0;
				uint64_t j = 0;
				uint64_t sizeFirst = firstElem.offsetEnd - firstElem.offsetBegin;
				uint64_t sizeSecond = secondElem.offsetEnd - secondElem.offsetBegin;
				while (i < sizeFirst && j < sizeSecond) //itersection algorithm
				{
					if (*(segment + i + firstElem.offsetBegin) < *(segment + j + secondElem.offsetBegin)) i++;
					else if (*(segment + i + firstElem.offsetBegin) > *(segment + j + secondElem.offsetBegin)) j++;
					else if (*(segment + i + firstElem.offsetBegin) == *(segment + j + secondElem.offsetBegin)) //intersection value has been found
					{
						toRet.push_back(*(segment + i + firstElem.offsetBegin)); //add the intersection element
						i++;
						j++;
					}
				}
				return toRet;
			}));
		}
		for (auto && result : results) //calling threads for every pair of posting lists in firstIteration
		{
			auto tempRes = result.get(); //actual call of the threads
			buffer.emplace(tempRes); //emplacing into buffer for further intersections
		}
		results.clear(); //clear the results so we can use it later
		if (firstIteration.size() == 1) //no elements to intersect with, push back the whole list
		{
			auto lastElement = firstIteration.front();
			firstIteration.pop();
			std::vector<uint64_t> toRet; //vector that will be emplaced in the buffer
			for (uint64_t i = lastElement.offsetBegin; i < lastElement.offsetEnd; i++)
			{
				toRet.push_back((uint64_t) * (segment + i));
			}
			buffer.emplace(toRet); //create the element
		}
		while (buffer.size() > 1)
		{
			while (buffer.size() > 1) //do tasks while buffer doesn't have only the last intersection
			{
				auto listOne = buffer.front(); //first intersection vector
				buffer.pop();
				auto listTwo = buffer.front(); //second intersection vector
				buffer.pop();
				results.emplace_back(pool.enqueue([listOne, listTwo]() //lambda function to find the value max in feature f
				{
					std::vector<uint64_t> toRet; //return vector of intersection
					uint64_t i = 0; //iterator over listOne
					uint64_t j = 0; //iterator over listTwo
					while (i < listOne.size() && j < listTwo.size()) //itersection algorithm
					{
						if (listOne[i] < listTwo[j]) i++;
						else if (listOne[i] > listTwo[j]) j++;
						else if (listOne[i] == listTwo[j]) //intersection value has been found
						{
							toRet.push_back(listOne[i]); //add the intersection element
							i++;
							j++;
						}
					}
					return toRet; //return the intersection
				}));
			}

			for (auto && result : results)
			{
				auto tempRes = result.get();
				buffer.emplace(tempRes);
			}
			results.clear();
		}
		auto allResults = buffer.front();
		buffer.pop();
		for (int i = 0; i < allResults.size(); i++)
		{
			callback(allResults[i]);
		}
	}
}; //namespace ii

#endif
