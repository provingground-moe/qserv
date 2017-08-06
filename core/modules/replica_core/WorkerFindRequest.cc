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

#include "replica_core/WorkerFindRequest.h"

// System headers

// Qserv headers

#include "lsst/log/Log.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica_core.WorkerFindRequest");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica_core {

////////////////////////////////////////////////////////////
///////////////////// WorkerFindRequest ////////////////////
////////////////////////////////////////////////////////////

WorkerFindRequest::pointer
WorkerFindRequest::create (int                priority,
                           const std::string &id,
                           const std::string &database,
                           unsigned int       chunk) {

    return WorkerFindRequest::pointer (
        new WorkerFindRequest (priority,
                               id,
                               database,
                               chunk));
}

WorkerFindRequest::WorkerFindRequest (int                priority,
                                      const std::string &id,
                                      const std::string &database,
                                      unsigned int       chunk)
    :   WorkerRequest ("FIND",
                       priority,
                       id),

        _database (database),
        _chunk    (chunk) {
}

WorkerFindRequest::~WorkerFindRequest () {
}

/////////////////////////////////////////////////////////////
///////////////////// WorkerFindRequestX ////////////////////
/////////////////////////////////////////////////////////////

WorkerFindRequestX::pointer
WorkerFindRequestX::create (int                priority,
                            const std::string &id,
                            const std::string &database,
                            unsigned int       chunk) {

    return WorkerFindRequestX::pointer (
        new WorkerFindRequestX (priority,
                                id,
                                database,
                                chunk));
}

WorkerFindRequestX::WorkerFindRequestX (int                priority,
                                        const std::string &id,
                                        const std::string &database,
                                        unsigned int       chunk)
    :   WorkerFindRequest (priority,
                           id,
                           database,
                           chunk) {
}

WorkerFindRequestX::~WorkerFindRequestX () {
}


bool
WorkerFindRequestX::execute (bool incremental) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "execute"
         << "  db: "    << database()
         << "  chunk: " << chunk());

    // TODO: provide the actual implementation instead of the dummy one.

    return WorkerRequest::execute(incremental);
}


}}} // namespace lsst::qserv::replica_core

