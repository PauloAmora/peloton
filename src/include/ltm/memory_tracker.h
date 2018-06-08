#pragma once
#include <string>
#include <atomic>
#include <vector>
#include "util/file_util.h"
#include <iostream>
#include <fstream>


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
                std::cout << v[i] << std::endl;
    }
    void FlushBytes(){
        std::ofstream myfile ("/home/paulo/example.txt");
          if (myfile.is_open())
          {
            myfile << "This is a line.\n";
            myfile << "This is another line.\n";
            for(uint count = 0; count < v.size(); count ++){
                myfile << v[count] << std::endl;
            }
            myfile.close();
          }
    }

private:
        size_t retrieved_bytes = 0;
        std::vector<size_t> v{};

};


}
}
