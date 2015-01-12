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
#include "secdb_api.h"
#include "secdb_types.h"

namespace secdb {

  struct SecdbCache {
    SecdbCache(const std::string& a_root, mode_t a_dir_mode = 0775);

    /// Write a MDSnapshot or Trade record
    void write(const Symbol& a_sym, const MDSnapshot& a_data);
    void write(const Symbol& a_sym, const Trade&      a_data);

    void print(const YMSymbol& a_sym) const;

  private:
    using DatabasesMap = std::unordered_map<YMSymbol, DBState, YMSymHash>;

    std::string  m_root;
    DatabasesMap m_databases;
  };

} // namespace secdb