#include "secdb_cache.h"
#include <utxx/error.hpp>

namespace secdb {

SecdbCache::SecdbCache(const std::string& a_root)
  : m_root(a_root)
{
  try   { boost::filesystem::create_directories(a_root); }
  catch ( std::exception& e )
        { throw utxx::runtime_error
          ("Cannot create directory ", a_root, ": ", e.what(); }
}

template <class T>
void SecdbCache::write(const Symbol& a_sym, const T& a_event)
{
  auto key = YMSymbol(a_event.timestamp(), a_sym);
  auto dbi = m_databases.find(key);

  if (dbi == m_databases.end())
    dbi = m_databases.emplace
      (std::piecewise_construct,
       std::forward_as_tuple(std::move(key)),
       std::forward_as_tuple(a_sym, m_root / key.basename())).first;

  dbi->Write(a_event);
}

} // namespace secdb