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
#include "replica_core/ConfigurationFile.h"

// System headers

#include <boost/lexical_cast.hpp>
#include <iterator>
#include <sstream>
#include <stdexcept>

// Qserv headers

#include "util/ConfigStore.h"

namespace {

/**
 * Fetch and parse a value of the specified key into. Return the specified
 * default value if the parameter was not found.
 *
 * The function may throw the following exceptions:
 *
 *   std::bad_lexical_cast
 */
template <typename T, typename D>
void parseKeyVal (lsst::qserv::util::ConfigStore &configStore,
                  std::string const& key,
                  T& val,
                  D& defaultVal) {

    std::string const str = configStore.get(key);
    val = str.empty() ? defaultVal : boost::lexical_cast<T>(str);        
}

}  // namespace

namespace lsst {
namespace qserv {
namespace replica_core {

ConfigurationFile::ConfigurationFile (std::string const& configFile)
    :   Configuration (),
        _configFile (configFile) {

    loadConfiguration();
}

ConfigurationFile::~ConfigurationFile () {
}

void
ConfigurationFile::loadConfiguration () {

    lsst::qserv::util::ConfigStore configStore(_configFile);

    // Parse the list of worker names

    {
        std::istringstream ss(configStore.getRequired("common.workers"));
        std::istream_iterator<std::string> begin(ss), end;
        _workers = std::vector<std::string>(begin, end);
    }
    {
        std::istringstream ss(configStore.getRequired("common.databases"));
        std::istream_iterator<std::string> begin(ss), end;
        _databases = std::vector<std::string>(begin, end);
    }

    ::parseKeyVal(configStore, "common.request_buf_size_bytes",     _requestBufferSizeBytes,       defaultRequestBufferSizeBytes);
    ::parseKeyVal(configStore, "common.request_retry_interval_sec", _retryTimeoutSec,              defaultRetryTimeoutSec);

    ::parseKeyVal(configStore, "common.database_technology", _databaseTechnology, defaultDatabaseTechnology);
    ::parseKeyVal(configStore, "common.database_host",       _databaseHost,       defaultDatabaseHost);
    ::parseKeyVal(configStore, "common.database_port",       _databasePort,       defaultDatabasePort);
    ::parseKeyVal(configStore, "common.database_user",       _databaseUser,       defaultDatabaseUser);
    ::parseKeyVal(configStore, "common.database_password",   _databasePassword,   defaultDatabasePassword);
    ::parseKeyVal(configStore, "common.database_name",       _databaseName,       defaultDatabaseName);

    ::parseKeyVal(configStore, "controller.http_server_port",       _controllerHttpPort,           defaultControllerHttpPort);
    ::parseKeyVal(configStore, "controller.http_server_threads",    _controllerHttpThreads,        defaultControllerHttpThreads);
    ::parseKeyVal(configStore, "controller.request_timeout_sec",    _controllerRequestTimeoutSec,  defaultControllerRequestTimeoutSec);

    ::parseKeyVal(configStore, "worker.technology",                 _workerTechnology,             defaultWorkerTechnology);
    ::parseKeyVal(configStore, "worker.num_svc_processing_threads", _workerNumProcessingThreads,   defaultWorkerNumProcessingThreads);
    ::parseKeyVal(configStore, "worker.num_fs_processing_threads",  _workerNumFsProcessingThreads, defaultWorkerNumFsProcessingThreads);
    ::parseKeyVal(configStore, "worker.fs_buf_size_bytes",          _workerFsBufferSizeBytes,      defaultWorkerFsBufferSizeBytes);


    // Optional common parameters for workers

    uint16_t commonWorkerSvcPort;
    uint16_t commonWorkerFsPort;
    uint16_t commonWorkerXrootdPort;

    ::parseKeyVal(configStore, "worker.svc_port",    commonWorkerSvcPort,    defaultWorkerSvcPort);
    ::parseKeyVal(configStore, "worker.fs_port",     commonWorkerFsPort,     defaultWorkerFsPort);
    ::parseKeyVal(configStore, "worker.xrootd_port", commonWorkerXrootdPort, defaultWorkerXrootdPort);

    std::string commonDataDir;
    
    ::parseKeyVal(configStore, "worker.data_dir",    commonDataDir,    defaultDataDir);

    // Parse optional worker-specific configuraton sections. Assume default
    // or (previously parsed) common values if a whole secton or individual
    // parameters are missing.

    for (std::string const& name: _workers) {

        std::string const section = "worker:" + name;
        if (_workerInfo.count(name))
            throw std::range_error (
                    "ConfigurationFile::loadConfiguration() duplicate worker entry: '" +
                    name + "' in: [common] or ["+section+"], configuration file: " + _configFile);

        _workerInfo[name].name = name;

        ::parseKeyVal(configStore, section+".svc_host",    _workerInfo[name].svcHost,    defaultWorkerSvcHost);
        ::parseKeyVal(configStore, section+".svc_port",    _workerInfo[name].svcPort,    commonWorkerSvcPort);
        ::parseKeyVal(configStore, section+".fs_port",     _workerInfo[name].fsPort,     commonWorkerFsPort);
        ::parseKeyVal(configStore, section+".xrootd_host", _workerInfo[name].xrootdHost, defaultWorkerXrootdHost);
        ::parseKeyVal(configStore, section+".xrootd_port", _workerInfo[name].xrootdPort, commonWorkerXrootdPort);

        ::parseKeyVal(configStore, section+".data_dir",    _workerInfo[name].dataDir,    commonDataDir);
        Configuration::translateDataDir(_workerInfo[name].dataDir, name);
    }
    
    // Parse mandatory database-specific configuraton sections

    for (std::string const& name: _databases) {

        std::string const section = "database:" + name;
        if (_databaseInfo.count(name))
            throw std::range_error (
                    "ConfigurationFile::loadConfiguration() duplicate database entry: '" +
                    name + "' in: [common] or ["+section+"], configuration file: " + _configFile);

        _databaseInfo[name].name = name;
        {
            std::istringstream ss(configStore.getRequired(section+".partitioned_tables"));
            std::istream_iterator<std::string> begin(ss), end;
            _databaseInfo[name].partitionedTables = std::vector<std::string>(begin, end);
        }
        {
            std::istringstream ss(configStore.getRequired(section+".regular_tables"));
            std::istream_iterator<std::string> begin(ss), end;
            _databaseInfo[name].regularTables = std::vector<std::string>(begin, end);
        }
    }
}
    
}}} // namespace lsst::qserv::replica_core