#pragma once
#include <string>
#include <atomic>
#include <vector>


namespace peloton{
namespace ltm{

class MemoryTracker
{
public:
        MemoryTracker(){};
        virtual ~MemoryTracker(){};

        static MemoryTracker& GetInstance(){
                static MemoryTracker retrieved_data;
                return retrieved_data;
        }

        void AddBytes (size_t bytes){
            retrieved_bytes += bytes;
                v.push_back(retrieved_bytes);
        }

    void PrintBytes(){
        for (size_t i = 0; i < v.size(); i++)
                std::cout << v[i] <<", " << std::endl;
    }
private:
        size_t retrieved_bytes = 0;
        std::vector<size_t> v{};

};


}
}
