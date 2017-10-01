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

namespace lsst {
namespace qserv {
namespace replica_core {

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
uint16_t     const Configuration::defaultWorkerFsPort                {50001};
std::string  const Configuration::defaultWorkerXrootdHost            {"localhost"};
uint16_t     const Configuration::defaultWorkerXrootdPort            {1094};
std::string  const Configuration::defaultDataDir                     {"{worker}"};


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
        _workerFsBufferSizeBytes      (defaultWorkerFsBufferSizeBytes) {
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
