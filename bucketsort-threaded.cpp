#include <iostream>
#include <thread>
#include <future>
#include <random>
//#include "pdqsort.h"

static const int arrSize = 250000000;
static const int numBuckets = 0.02 * arrSize; // 2% has tended to be a sweet spot for my input arrays; this isn't always going to be true though
static const int elementsPerBucket = arrSize / numBuckets;
static const int insertionSortSize = 16; // Use insertion sort for arrays smaller than this size
static const int maxDepth = 55; //Max depth allowed for quicksort; 2 * log_2(arrSize)

// End is an inclusive index here
void insertionSort(int* arr, int start, int end)
{
    for (int i = start + 1; i <= end; ++i)
    {
        for (int j = i; j > 0 && arr[j - 1] > arr[j]; --j)
        {
            std::swap(arr[j], arr[j - 1]);
        }
    }
}

// Possible optimization: median-of-three pivoting
int partition(int* arr, int start, int end)
{
    int pivot = arr[end];
    int lowSideEnd = start - 1; // Points to last item in the low side

    for (int i = start; i < end; ++i)
    {
        // Move items less than pivot to the "low" side
        if (arr[i] <= pivot)
        {
            ++lowSideEnd;
            std::swap(arr[i], arr[lowSideEnd]);
        }
    }

    std::swap(arr[lowSideEnd + 1], arr[end]); // Pivot belongs to right of low side
    return lowSideEnd + 1;
}

void introSort(int* arr, int start, int end, int depth)
{
    if (end - start < insertionSortSize)
    {
        insertionSort(arr, start, end);
    }
    else if (depth == maxDepth)
    {
        std::make_heap(&arr[start], &arr[end + 1]);
        std::sort_heap(&arr[start], &arr[end + 1]);
    }
    else
    {
        int pivotIdx = partition(arr, start, end);
       
        // Recursively sort left and right side of pivot
        // and sort side with less elements first
        if (pivotIdx - 1 - start < end - pivotIdx + 1)
        {
            introSort(arr, start, pivotIdx - 1, depth + 1);
            introSort(arr, pivotIdx + 1, end, depth + 1); // Tail optimization
        }
        else
        {
            introSort(arr, pivotIdx + 1, end, depth + 1);
            introSort(arr, start, pivotIdx - 1, depth + 1); // Tail optimization
        }
    }
}

class bucketADT
{
private:
    std::vector<int> extension = std::vector<int>();
    int internalSize = 0;

public:
    int arr[elementsPerBucket] = { 0 };
    
    int getInternalSize()
    {
        return internalSize;
    }
    
    void push_back(int val)
    {
        if (internalSize == elementsPerBucket)
        {
            extension.push_back(val);
        }
        else
        {
            arr[internalSize] = val;
            ++internalSize;
        }
    }
    
    std::vector<int> convert()
    {
        std::vector<int> ret;
        
        for (int i = 0; i < internalSize; ++i)
        {
            ret.push_back(arr[i]);
        }
        
        if (internalSize == elementsPerBucket)
        {
            ret.insert(ret.end(), extension.begin(), extension.end());
        }
        
        return ret;
    }
};

/*
 * Sorts an individual bucket using introSort
 *
 * pdqSort can be used for improved performance
 */
std::vector<int> individualBucketSort(std::shared_ptr<bucketADT[]> buckets, int start, int end)
{
    std::vector<int> result;
    
    // Sort each bucket and transfer sorted bucket into final result
    for (int i = start; i < end; ++i)
    {
        if (buckets[i].getInternalSize() < elementsPerBucket)
        {
            //pdqsort(&buckets[i].arr[0], &buckets[i].arr[buckets[i].currPtr]);
            introSort(buckets[i].arr, 0, buckets[i].getInternalSize() - 1, 0);
            
            result.insert(result.end(), &buckets[i].arr[0], &buckets[i].arr[buckets[i].getInternalSize()]);
        }
        else
        {
            std::vector<int> curr = buckets[i].convert();
            
            //pdqsort(curr.begin(), curr.end());
            introSort(&curr[0], 0, curr.size() - 1, 0);
            
            result.insert(result.end(), curr.begin(), curr.end());
        }
    }
    
    return result;
}

/*
 * Generic bucket sort implementation
 *
 * Sorts an array that can have values between INT_MIN and INT_MAX
 */
std::vector<int> bucketSort(const std::vector<int>& arr, int numBuckets)
{
    std::shared_ptr<bucketADT[]> buckets(new bucketADT[numBuckets]);
    
    long max = INT_MIN;
    long min = INT_MAX;
    
    for (int itr : arr)
    {
        max = itr > max ? itr : max;
        min = itr < min ? itr : min;
    }
    
    // Want max to be 1 above so proportions are always less than 1
    max += 1;
    
    for (int curr : arr)
    {
        // Get a proportion to the biggest number in the array
        // Then use that multiplied by amount of buckets to
        // determine bucket location
        size_t idx = (size_t)numBuckets * ((double)abs(curr - min) / (double)abs(max - min));
        buckets[idx].push_back(curr);
    }
    
    const auto processorCount = std::thread::hardware_concurrency();
    
    // If can't determine processor count, fallback to single thread
    if (processorCount == 0)
    {
        return individualBucketSort(buckets, 0, numBuckets);
    }
     
    // Multithreaded sorting of each bucket
    std::vector<int> result;
    result.reserve(arr.size());
    
    // We want to prevent thrashing
    int bucketsPerThread = numBuckets / processorCount;
    std::vector<std::future<std::vector<int>>> sortedBuckets;

    // Create the threads and begin sorting each group of buckets
    for (int start = 0; start < numBuckets;)
    {
        int end = start + bucketsPerThread > numBuckets ? numBuckets : start + bucketsPerThread;
        sortedBuckets.push_back(std::async(std::launch::async, &individualBucketSort, buckets, start, end));
        start += bucketsPerThread;
    }
    
    // Gather results and combine them
    for (int i = 0; i < sortedBuckets.size(); ++i)
    {
        const std::vector<int>& sortedBucket = sortedBuckets[i].get();
        result.insert(result.end(), sortedBucket.begin(), sortedBucket.end());
    }

    return result;
}

template<typename T>
T random(T range_from, T range_to) 
{
    static std::random_device rand_dev;
    static std::mt19937 generator(rand_dev());
    std::uniform_int_distribution<T> distr(range_from, range_to);
    return distr(generator);
}

int main()
{
    using namespace std;

    vector<int> test(arrSize);

    for (int i = 0; i < test.size(); ++i)
    {
        test[i] = random(INT_MIN, INT_MAX);
    }
    
    std::cout << "Starting sort" << std::endl;
    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    
    test = bucketSort(test, numBuckets);
    
    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    std::cout << "Finished sorting" << std::endl;
    
    std::cout << "Time elapsed = " << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() << " ms" << std::endl;

    int prev = INT_MIN;
    
    for (int itr : test)
    {
        if (itr < prev)
        {
            std::cout << "Not sorted" << std::endl;
            return 1;
        }
    
        prev = itr;
    }
    
    return 0;
}
