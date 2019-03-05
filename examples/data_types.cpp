/**
* Test special data type templates for multi-element replies using Redox.
*/

#include <iostream>
#include <set>
#include <unordered_set>
#include <vector>
#include "redox.hpp"

using namespace std;
using redox::Redox;
using redox::Command;

int main(int argc, char* argv[]) {

  redox::Redox rdx; // Initialize Redox (default host/port)
  if(!rdx.connect()) return 1; // Start the event loop

  rdx.del("mylist");

  rdx.commandSync(redox::util::strToVec("LPUSH mylist 1 2 3 4 5 6 7 8 9 10"));

  rdx.command<vector<string>>({"LRANGE", "mylist", "0", "4"},
    [](Command<vector<string>>& c){
      if(!c.ok()) return;
      cout << "Last 5 elements as a vector: ";
      for (const string& s : c.reply()) cout << s << " ";
      cout << endl;
    }
  );

  rdx.command<unordered_set<string>>(redox::util::strToVec("LRANGE mylist 0 4"),
    [](Command<unordered_set<string>>& c){
      if(!c.ok()) return;
      cout << "Last 5 elements as a hash: ";
      for (const string& s : c.reply()) cout << s << " ";
      cout << endl;
    }
  );

  rdx.command<set<string>>(redox::util::strToVec("LRANGE mylist 0 4"),
    [&rdx](Command<set<string>>& c) {
      if(c.ok()) {
        cout << "Last 5 elements as a set: ";
        for (const string& s : c.reply()) cout << s << " ";
        cout << endl;
      }
      rdx.stop();
    }
  );

  rdx.wait();
  return 0;
}
