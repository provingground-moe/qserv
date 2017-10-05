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
#include "replica_core/ConfigurationMySQL.h"

// System headers

// Qserv headers


namespace lsst {
namespace qserv {
namespace replica_core {

ConfigurationMySQL::ConfigurationMySQL (database::mysql::ConnectionParams const& connectionParams)
    :   Configuration (),
        _connectionParams (connectionParams) {

    loadConfiguration();
}

ConfigurationMySQL::~ConfigurationMySQL () {
}

std::string
ConfigurationMySQL::configUrl () const {
    return  "mysql:host=" + _connectionParams.host +
            ",port="      + std::to_string(_connectionParams.port) +
            ",database="  + _connectionParams.database +
            ",user="      + _connectionParams.user +
            ",password=*****";
}

void
ConfigurationMySQL::loadConfiguration () {
}

}}} // namespace lsst::qserv::replica_core