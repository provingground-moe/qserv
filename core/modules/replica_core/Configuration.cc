/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
 *
 * This product includes software developed by the
 * LSST Project (http://www.lsst.org/).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the LSST License Statement and
 * the GNU General Public License along with this program.  If not,
 * see <http://www.lsstcorp.org/LegalNotices/>.
 */

// Class header
#include "replica_core/Configuration.h"

// System headers

#include <boost/lexical_cast.hpp>
#include <iterator>
#include <sstream>
#include <stdexcept>

// Qserv headers

#include "util/ConfigStore.h"

namespace {

    /**
     * Fetch and parse a value of the specified key into.
     *
     * The function may throw the following exceptions:
     *
     *   std::range_error
     *   std::bad_lexical_cast
     */
    template <typename T>
    void
    parseKeyVal (lsst::qserv::util::ConfigStore &configStore,
                 const std::string &key,
                 T &result) {
        
        const std::string val = configStore.get(key);
        if (val.empty())
            throw std::range_error("key '"+key+"' has no value");

        result = boost::lexical_cast<T>(val);        
    }
}
namespace lsst {
namespace qserv {
namespace replica_core {

Configuration::Configuration (const std::string &configFile)
    :   _configFile                 (configFile),
        _workers                    {},
        _requestBufferSizeBytes     (1024),
        _defaultRetryTimeoutSec     (2),
        _controllerHttpPort         (80),
        _controllerHttpThreads      (1),
        _controllerRequestTimeoutSec(0),
        _workerSvcPort              (50000),
        _workerXrootdPort           (1094),
        _workerNumConnectionsLimit  (16),
        _workerNumProcessingThreads (1) {

    loadConfiguration();
}

Configuration::~Configuration () {
}

void
Configuration::loadConfiguration () {

    lsst::qserv::util::ConfigStore configStore(_configFile);

    // Parse the list of worker names

    std::istringstream ss(configStore.getRequired("common.workers"));
    std::istream_iterator<std::string> begin(ss), end;
    _workers = std::vector<std::string>(begin, end);

    ::parseKeyVal(configStore, "common.request_buf_size_bytes",     _requestBufferSizeBytes);
    ::parseKeyVal(configStore, "common.request_retry_interval_sec", _defaultRetryTimeoutSec);

    ::parseKeyVal(configStore, "controller.http_server_port",       _controllerHttpPort);
    ::parseKeyVal(configStore, "controller.http_server_threads",    _controllerHttpThreads);
    ::parseKeyVal(configStore, "controller.request_timeout_sec",    _controllerRequestTimeoutSec);

    ::parseKeyVal(configStore, "worker.svc_port",                   _workerSvcPort);
    ::parseKeyVal(configStore, "worker.xrootd_port",                _workerXrootdPort);
    ::parseKeyVal(configStore, "worker.max_connections",            _workerNumConnectionsLimit);
    ::parseKeyVal(configStore, "worker.num_processing_threads",     _workerNumProcessingThreads);
}
    
}}} // namespace lsst::qserv::replica_core
