#include <iostream>
#include <chrono>
#include <fstream>
#include <unordered_map>
#include <vector>
#include <math.h>
#include <charconv>
#include <limits>
#include <sstream>
#include <mutex>
#include <thread>
#include <algorithm>
#include <cstring>
#include <functional>
// for mmap:
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <memory>
/* Your mission, should you choose to accept it, is to write a program that retrieves temperature measurement 
values from a text file and calculates the MIN, MEAN, and MAX temperature per weather station.
 There's just one caveat: the file has 1,000,000,000 rows! That's more than 10 GB of data! 😱

The text file has a simple structure with one measurement value per row:
Hamburg;12.0
Bulawayo;8.9
Palembang;38.8
Hamburg;34.2
St. John's;15.2
Cracow;12.6
... etc. ...

The program should print out the min, mean, and max values per station, alphabetically ordered. 
The format that is expected varies slightly from language to language, but the following example shows the expected output for the first three stations:


Hamburg;12.0;23.1;34.2
Bulawayo;8.9;22.1;35.2
Palembang;38.8;39.9;41.0
Oh, and this input.txt is different for each submission since it's generated on-demand. So no hard-coding the results! 😉

Choose a language from the cards at the top of this page to get started! 🚀 

static inline void read_file(const std::string& file_path)
{   
    std::ifstream in_file(file_path, std::ios::binary);
                                    // min, max, sum
    std::unordered_map<std::string, std::tuple<float, float, float, int>> station_map;
    std::string line;
    float d_min = 500.0, d_max = -500.0;
    int start_count = 1;
    int line_count = 0;
    auto start_time = NowMilli();
    while (std::getline(in_file, line))
    {
        auto split_ind = line.find_first_of(';');
        auto station = line.substr(0, split_ind);
        auto float_str = line.substr(split_ind + 1, line.length() -1);
        float measurement = std::stof(float_str);
        //std::cout << "station: " << station << " float_str: " << float_str << std::endl; 
        auto itr = station_map.find(station);
     
        if (itr != station_map.end())
        {
            std::tuple<float, float, float, int> entry = itr->second;
            float min = std::min(std::get<0>(entry), measurement);
            float max = std::max(std::get<1>(entry), measurement);
            float sum = measurement + std::get<2>(entry);
            int count = std::get<3>(entry) + 1;
            itr->second = std::tie(min, max, sum, count);
        }
        else
        {
            station_map.insert(std::make_pair(station, std::tie(d_min, d_max, measurement, start_count)));
        }
        
        if (line_count % 10000000 == 0)
        {
            std::cout << line_count << " passed: "<< NowMilli() - start_time << std::endl;
            start_time = NowMilli();
        }
        
        line_count++;
    }
        for (auto& item : station_map)
        {
            std::stringstream ss;
            ss << item.first << ";";
            auto entry = item.second;
            float min = std::get<0>(entry);
            float max = std::get<1>(entry);
            float sum = std::get<2>(entry);
            int count = std::get<3>(entry);
            float avg = sum/count;
            ss << min << ";" << max << ";" << avg;
            std::cout << ss.str() << std::endl;

        }
}
*/

// Unordered_map took speed from 10m/10s to 10m/5s
//std::unordered_map<std::string, std::tuple<float, float, float, int>> station_map;
typedef std::unordered_map<std::string_view, float> s_map;
typedef std::shared_ptr<s_map> s_map_ptr;
std::mutex io_lock;
const char* map_file(const char* fname, size_t& length);
static inline uint64_t NowMilli()
{
    using namespace std::chrono;
    return duration_cast<milliseconds>(high_resolution_clock::now().time_since_epoch()).count();
}

void handle_error(const char* msg) {
    perror(msg);
    exit(255);
}

inline const char* map_file(const char* fname, size_t& length)
{
    int fd = open(fname, O_RDONLY);
    if (fd == -1)
        handle_error("open");

    // obtain file size
    struct stat sb;
    if (fstat(fd, &sb) == -1)
        handle_error("fstat");

    length = sb.st_size;

    const char* addr = static_cast<const char*>(mmap(NULL, length, PROT_READ, MAP_PRIVATE, fd, 0u));
    if (addr == MAP_FAILED)
        handle_error("mmap");

    // TODO close fd at some point in time, call munmap(...)
    return addr;
}

inline void process_stations(const std::string_view& the_str, const size_t size, s_map_ptr station_map)
{
//   std::string str(buffer);
//   float d_min = 500.0, d_max = -500.0;
 //  int start_count = 1;
   //auto thread_id = std::this_thread::get_id();

   for (int i = 0; i < size;)
   {

        auto entry_ind = the_str.find_first_of('\n', i);
        auto entry_size = entry_ind - i;
        std::string_view full_entry = the_str.substr(i, entry_size);
        auto split_ind = full_entry.find_first_of(';');
        std::string_view station = full_entry.substr(0, split_ind);
        std::string_view float_str = full_entry.substr(split_ind + 1);
  
        float measurement;
        try {
          //  char* end = (char*) float_str.data() + entry_size;
          //  measurement = std::strtod(float_str.data(), &end);
        //     {
         //    std::lock_guard<std::mutex> guard(io_lock);   
         //    std::cout << "NUM " << num << std::endl;
         //    }
             
        } 
        catch (std::exception& e)
        {
            std::cout << "COULD NOT PROCESS: " << float_str << std::endl;
        }
        /*
        auto itr = station_map->find(station);
        if (itr != station_map->end())
        {
         //   std::tuple<float, float, float, int> entry = itr->second;
         //   float min = std::min(std::get<0>(entry), measurement);
         //   float max = std::max(std::get<1>(entry), measurement);
         //   float sum = measurement + std::get<2>(entry);
          //  int count = std::get<3>(entry) + 1;
         //   itr->second = std::tie(min, max, sum, count);
         itr->second = measurement;
        }
        else
        {
            station_map->insert(std::make_pair(station, measurement));
        }
        */
        i+= entry_size + 1;
 //   if (i % 100000000 == 0)
  //  {
    //    std::cout << "thread: " << thread_id << " at " << i << std::endl;
  //  }
   }
}

int main(int argc, char** argv)
{
    auto start_time = NowMilli();
    auto tick_time = start_time;
    int num_threads = 50;
    std::vector<s_map_ptr> station_maps;
    std::vector<std::thread> worker_threads;

    for (int i = 0; i < num_threads; i++)
        station_maps.push_back(s_map_ptr(new s_map()));

    std::string file = "/Users/josephmcilvaine/1brc/measurements.txt";

   size_t file_len;
   std::cout << "starting read" << std::endl;
   const char* file_head = map_file(file.c_str(), file_len);
   std::cout << "done read" << std::endl;

//   size_t bytes_processed = 0;

   size_t buffer_size = file_len/num_threads;
   int map_ind = 0;
   std::cout << "starting loop " << file_len << std::endl;
   const char* curr_head = 0;
   for (size_t bytes_processed = 0; bytes_processed < file_len;)
   {
        
    //    char c = file_head[];
        curr_head = file_head + bytes_processed;
        if (bytes_processed + buffer_size > file_len)
        {
            std::cout << "going into here" << std::endl;
            std::string_view part = std::string_view{curr_head, file_len - bytes_processed};
           // std::string str(copy_buffer);
            auto last_byte = part.find_last_of('\n');
            bytes_processed = file_len;
            worker_threads.push_back(std::thread(std::bind(process_stations, part, file_len - bytes_processed, station_maps[map_ind++])));
      //   std::cout << "final last_byte: " << last_byte << " final bytes_processed: " << bytes_processed << std::endl;
        //    continue;
        }
        else
        {
            std::string_view part = std::string_view{curr_head, buffer_size};
            auto last_byte = part.find_last_of('\n') + 1;
            part = std::string_view{curr_head, last_byte};
            bytes_processed += buffer_size - (buffer_size - last_byte);
            std::cout << "PART LEN: " << part.length() << std::endl;
            worker_threads.push_back(std::thread(std::bind(process_stations, part, last_byte, station_maps[map_ind++])));
            std::cout << "last_byte: " << last_byte << " bytes_processed: " << bytes_processed << std::endl; 
        }

        
   }
   std::cout << "ending loop" << std::endl;
   auto total_time_ms = NowMilli() - start_time;
   std::cout << "Time To Run: " << total_time_ms << std::endl;
  // process_stations(copy_buffer, indy, mp);
   std::cout << "STARTING SLEEP FOR 10s" << std::endl;
//   std::this_thread::sleep_for(std::chrono::milliseconds(1000));
   for (auto& t : worker_threads)
   {

    if (t.joinable())
    {
        std::cout << "joinin thread...." << std::endl;
        t.join();
    }
   }
   total_time_ms = NowMilli() - start_time;
   std::cout << "Time To Run 2: " << total_time_ms << std::endl;
   return 0;
}
