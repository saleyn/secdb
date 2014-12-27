#include "secdb_api.hpp"

using namespace std;
using namespace secdb;

void usage(const char* program) {
  cerr << "Dump content of secdb database file\n\n"
       << "Usage: " << program << " -f Filename [-v [Level]] [-b Size]\n\n"
       << "  -f Filename  - read the Filename\n"
       << "  -v [Level]   - giving -v -vv -vvv or Level gives verbose output\n"
       << endl;
  exit(1);
}

void unhandled_exception() {
  auto p = std::current_exception();
  try    { std::rethrow_exception(p); }
  catch  ( std::exception& e ) { cerr << e.what() << endl; }
  catch  ( ... )               { cerr << "Unknown exception" << endl; }

  exit(1);
}

int main(int argc, char* argv[]) {
  int         verbose;
  std::string filename;


  for (int i=1; i < argc; ++i) {
    if (!strcmp(argv[i], "-f") && i < argc-1)
      filename = argv[++i];
    else if (!strncmp(argv[i], "-v", 2)) {
      if (i < argc-1 && argv[i+1][0] != '-')
        verbose += atoi(argv[++i]);
      else
        verbose += strlen(argv[i])-1;
    } else {
      cerr << "Invalid option: " << argv[i] << endl << endl;
      usage(argv[0]);
    }
  }

  std::set_terminate(&unhandled_exception);

  // Following an empty line is a chunkmap:
  DBState state(filename, verbose);

  //std::vector<unsigned char> data(buf_size);
  //secdb::BitReader reader(depth);

  return 0;
}