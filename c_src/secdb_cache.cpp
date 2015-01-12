#include "secdb_cache.h"
#include "secdb_types.h"

namespace secdb {

SecdbCache::SecdbCache(const std::string& a_root, mode_t a_dir_mode)
  : m_root(a_root)
{
  size_t  m = std::string::npos, n = 0;
  while ((m = a_root.find(path_sep(), n)) != std::string::npos) {
    auto dir = a_root.substr(n, m-n);
    if (mkdir(dir.c_str(), a_dir_mode) < 0)
      throw runtime_error("Cannot create directory ", dir, ": ", strerror(errno));
    n = m;
  }
}

template <class T>
void SecdbCache::write(const Symbol& a_sym, const T& a_event)
{
  auto key = YMSymbol(a_event.timestamp(), a_sym, m_root);
  auto dbi = m_databases.find(key);

  if (dbi == m_databases.end()) {
    dbi = m_databases.emplace
      (std::piecewise_construct,
       std::forward_as_tuple(std::move(key)),
       std::forward_as_tuple(a_sym, key.filename())).first;

  // TODO: Implement dbi->Write()
  // dbi->Write(a_event);
  }
}

} // namespace secdb