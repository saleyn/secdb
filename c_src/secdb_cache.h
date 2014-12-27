//-----------------------------------------------------------------------------
/// \file  secdb-writer.h
//-----------------------------------------------------------------------------
/// \brief Multi-file asynchronous logger
///
/// The logger logs data to multiple streams asynchronously
/// It is optimized for performance of the producer to ensure minimal latency.
/// The producer of log messages never blocks during submission of a message.
//-----------------------------------------------------------------------------
// Copyright (c) 2014 Omnibius, LLC
// Author:  Serge Aleynikov <saleyn@gmail.com>
// Created: 2014-12-20
//-----------------------------------------------------------------------------
#pragma once

#include <unordered_map>
#include <boost/filesystem.hpp>
#include "secdb_api.h"
#include "secdb_types.h"

namespace secdb {

  template <class T>
  inline void hash_combine(std::size_t& seed, const T& v) {
      seed ^= std::hash<T>()(v) + 0x9e3779b9 + (seed<<6) + (seed>>2);
  }

  struct SecdbCache {
    SecdbCache(const std::string& a_root);

    /// Write a MDSnapshot or Trade record
    template <class T>
    void write(const Symbol& a_sym, const T& a_data);

    void print(const YMSymbol& a_sym) const;

  private:
    using DatabasesMap = std::unordered_map<YMSymbol, DBState, YMSymHash>;

    boost::filesystem::path m_root;
    DatabasesMap            m_databases;
  };

} // namespace secdb