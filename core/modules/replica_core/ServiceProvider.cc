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

#include "replica_core/ServiceProvider.h"

// System headers

#include "replica_core/Configuration.h"
#include "replica_core/WorkerInfo.h"

// Qserv headers

namespace lsst {
namespace qserv {
namespace replica_core {


ServiceProvider::ServiceProvider (Configuration &configuration)
    :   _configuration(configuration) {
}

std::vector<std::string>
ServiceProvider::workers () const {
    return _configuration.workers();
}

std::shared_ptr<WorkerInfo>
ServiceProvider::workerInfo (const std::string& workerName) const {

    const std::string &workerHost = workerName;

    std::shared_ptr<WorkerInfo> info(new WorkerInfo (
        workerName,
        workerHost,
        std::to_string(_configuration.workerSvcPort()),
        workerHost,
        std::to_string(_configuration.workerSvcPort()))
    );
    return info;
}

}}} // namespace lsst::qserv::replica_core
