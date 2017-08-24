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

#include "replica_core/FileUtils.h"

// System headers

// Qserv headers
#include "replica_core/Configuration.h"


namespace {
    const std::vector<std::string> extensions{".frm", ".MYD", ".MYI"};
} // namespace

namespace lsst {
namespace qserv {
namespace replica_core {

std::vector<std::string>
FileUtils::partitionedFiles (const DatabaseInfo &databaseInfo,
                             unsigned int        chunk) {

    std::vector<std::string> result;

    const std::string chunkSuffix = "_" + std::to_string(chunk);

    for (const auto &table: databaseInfo.partitionedTables) {
        
        const std::string file = databaseInfo.name + "/" + table + chunkSuffix;
        for (const auto &ext: ::extensions)
            result.push_back(file + ext);

        const std::string fileOverlap = databaseInfo.name + "/" + table + "FullOverlap" + chunkSuffix;
        for (const auto &ext: ::extensions)
            result.push_back(fileOverlap + ext);
    }
    return result;
}

std::vector<std::string>
FileUtils::regularFiles (const DatabaseInfo &databaseInfo) {

    std::vector<std::string> result;

    for (const auto &table : databaseInfo.regularTables) {
        const std::string filename = databaseInfo.name + "/" + table;
        for (const auto &ext : ::extensions)
            result.push_back(filename + ext);
    }
    return result;
}

}}} // namespace lsst::qserv::replica_core
