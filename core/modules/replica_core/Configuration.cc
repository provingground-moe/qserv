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

#include <stdexcept>

// Qserv headers

#include "replica_core/ConfigurationFile.h"
#include "replica_core/ConfigurationMySQL.h"
#include "replica_core/FileUtils.h"

namespace lsst {
namespace qserv {
namespace replica_core {


Configuration::pointer
Configuration::load (std::string const& configUrl) {
 
    for (auto const& proposedPrefix: std::vector<std::string>{"file:","mysql:"}) {
 
        std::string::size_type const prefixSize = proposedPrefix.size();
        std::string const            prefix = configUrl.substr (0, prefixSize);
        std::string const            suffix = configUrl.substr (prefixSize);
 
        if ("file:"  == prefix)
            return Configuration::pointer (
                new ConfigurationFile  (suffix));

        if ("mysql:" == prefix)
            return Configuration::pointer (
                new ConfigurationMySQL (
                    database::mysql::ConnectionParams::parse (
                        suffix,
                        Configuration::defaultDatabaseHost,
                        Configuration::defaultDatabasePort,
                        Configuration::defaultDatabaseUser,
                        Configuration::defaultDatabasePassword)));
    }
    throw std::invalid_argument (
            "Configuration::load:  unsupported configUrl: " + configUrl);
}


// Set some reasonable defaults

size_t       const Configuration::defaultRequestBufferSizeBytes      {1024};
unsigned int const Configuration::defaultRetryTimeoutSec             {1};
uint16_t     const Configuration::defaultControllerHttpPort          {80};
size_t       const Configuration::defaultControllerHttpThreads       {1};
unsigned int const Configuration::defaultControllerRequestTimeoutSec {3600};
std::string  const Configuration::defaultWorkerTechnology            {"TEST"};
size_t       const Configuration::defaultWorkerNumProcessingThreads  {1};
size_t       const Configuration::defaultWorkerNumFsProcessingThreads{1};
size_t       const Configuration::defaultWorkerFsBufferSizeBytes     {1048576};
std::string  const Configuration::defaultWorkerSvcHost               {"localhost"};
uint16_t     const Configuration::defaultWorkerSvcPort               {50000};
std::string  const Configuration::defaultWorkerFsHost                {"localhost"};
uint16_t     const Configuration::defaultWorkerFsPort                {50001};
std::string  const Configuration::defaultDataDir                     {"{worker}"};
std::string  const Configuration::defaultDatabaseTechnology          {"mysql"};
std::string  const Configuration::defaultDatabaseHost                {"localhost"};
uint16_t     const Configuration::defaultDatabasePort                {3306};
std::string  const Configuration::defaultDatabaseUser                {FileUtils::getEffectiveUser()};
std::string  const Configuration::defaultDatabasePassword            {""};
std::string  const Configuration::defaultDatabaseName                {"replica"};

void
Configuration::translateDataDir (std::string&       dataDir,
                                 std::string const& workerName) {

    std::string::size_type const leftPos = dataDir.find('{');
    if (leftPos == std::string::npos) return;

    std::string::size_type const  rightPos = dataDir.find('}');
    if (rightPos == std::string::npos) return;

    if (dataDir.substr (leftPos, rightPos - leftPos + 1) == "{worker}")
        dataDir.replace(leftPos, rightPos - leftPos + 1, workerName);
}

Configuration::Configuration ()
    :   _workers                      (),
        _requestBufferSizeBytes       (defaultRequestBufferSizeBytes),
        _retryTimeoutSec              (defaultRetryTimeoutSec),
        _controllerHttpPort           (defaultControllerHttpPort),
        _controllerHttpThreads        (defaultControllerHttpThreads),
        _controllerRequestTimeoutSec  (defaultControllerRequestTimeoutSec),
        _workerTechnology             (defaultWorkerTechnology),
        _workerNumProcessingThreads   (defaultWorkerNumProcessingThreads),
        _workerNumFsProcessingThreads (defaultWorkerNumFsProcessingThreads),
        _workerFsBufferSizeBytes      (defaultWorkerFsBufferSizeBytes),
        _databaseTechnology           (defaultDatabaseTechnology),
        _databaseHost                 (defaultDatabaseHost),
        _databasePort                 (defaultDatabasePort),
        _databaseUser                 (defaultDatabaseUser),
        _databasePassword             (defaultDatabasePassword),
        _databaseName                 (defaultDatabaseName) {
}

Configuration::~Configuration () {
}

bool
Configuration::isKnownWorker (std::string const& name) const {
    return _workerInfo.count(name) > 0;
}

WorkerInfo const&
Configuration::workerInfo (std::string const& name) const {
    if (!isKnownWorker(name)) 
        throw std::out_of_range (
                "Configuration::workerInfo() uknown worker name '" + name + "'");
    return _workerInfo.at(name);
}

bool
Configuration::isKnownDatabase (std::string const& name) const {
    return _databaseInfo.count(name) > 0;
}

DatabaseInfo const&
Configuration::databaseInfo (std::string const& name) const {
    if (!isKnownDatabase(name)) 
        throw std::out_of_range (
                "Configuration::databaseInfo() uknown database name '" + name + "'");
    return _databaseInfo.at(name);
}
    
}}} // namespace lsst::qserv::replica_core
