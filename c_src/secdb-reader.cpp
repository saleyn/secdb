#include "secdb_api.hpp"

using namespace std;
using namespace secdb;

void usage(const char* program) {
  cerr <<
    "Dump content of secdb database file\n\n"
    "Usage: " << program << " -f Filename [-v [Level]] [-b Size]\n\n"
    "  -f Filename      - read the Filename\n"
    "  -v [Level]       - giving -v -vv -vvv or Level gives verbose output\n"
    "  -d Depth         - limit output to Depth levels\n"
    "  -c | --candle    - print candles\n"
    "  -i               - print file information only (no market data)\n"
    "  --no-symbol      - don't output symbol name\n"
    "  --epoch-time     - print epoch-time rather than YYYY-MM-DD HH:MM:SS\n"
    "  --unique         - only print MD records if there's a change\n"
    "                     in price&quantity\n"
    "  --unique-price   - only print MD records if there's a change\n"
    "                     in price only compared to prior L1 change\n"
    "                     (suppresses printing when there's a quantity\n"
    "                      change at some level but no price changes\n"
    "                      on the levels of interest limited by -d option)\n"
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
  int                 verbose;
  std::string         filename;
  bool                no_symbol    = false;
  bool                epoch_time   = false;
  DBState::PrintFlags unique       = DBState::PrintFlags::None;
  bool                show_candles = false;
  int                 depth        = INT_MAX;
  bool                file_info    = false;


  for (int i=1; i < argc; ++i) {
    if (!strcmp(argv[i], "-f") && i < argc-1)
      filename = argv[++i];
    else if (!strcmp(argv[i], "-d") && i < argc-1)
      depth = atoi(argv[++i]);
    else if (!strcmp(argv[i], "--no-symbol"))
      no_symbol  = true;
    else if (!strcmp(argv[i], "--epoch-time"))
      epoch_time = true;
    else if (!strcmp(argv[i], "--unique"))
      unique = DBState::PrintFlags::UniquePriceAndQty;
    else if (!strcmp(argv[i], "--unique-price"))
      unique = DBState::PrintFlags::UniquePrice;
    else if (!strcmp(argv[i], "-c") || !strcmp(argv[i], "--candle"))
      show_candles = true;
    else if (!strcmp(argv[i], "-i"))
      file_info = true;
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

  int               flags  = int(unique);
  if (!no_symbol)   flags |= int(DBState::PrintFlags::OutputSymbolName);
  if (epoch_time)   flags |= int(DBState::PrintFlags::EpochTimeFormat);
  if (show_candles) flags |= int(DBState::PrintFlags::DisplayCandles);
  if (file_info)    flags |= int(DBState::PrintFlags::DisplayFileInfo);

  state.print(std::cout, depth, flags);

  return 0;
}