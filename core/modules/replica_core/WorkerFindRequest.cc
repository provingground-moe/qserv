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

#include <boost/filesystem.hpp>

// Qserv headers

#include "lsst/log/Log.h"
#include "replica_core/Configuration.h"
#include "replica_core/FileUtils.h"
#include "replica_core/ServiceProvider.h"

// This macro to appear witin each block which requires thread safety

#define LOCK_DATA_FOLDER \
std::lock_guard<std::mutex> lock(_mtxDataFolderOperations)

namespace fs = boost::filesystem;

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
WorkerFindRequest::create (
        ServiceProvider   &serviceProvider,
        const std::string &worker,
        const std::string &id,
        int                priority,
        const std::string &database,
        unsigned int       chunk) {

    return WorkerFindRequest::pointer (
        new WorkerFindRequest (
                serviceProvider,
                worker,
                id,
                priority,
                database,
                chunk));
}

WorkerFindRequest::WorkerFindRequest (
        ServiceProvider   &serviceProvider,
        const std::string &worker,
        const std::string &id,
        int                priority,
        const std::string &database,
        unsigned int       chunk)

    :   WorkerRequest (
            serviceProvider,
            worker,
            "FIND",
            id,
            priority),

        _database    (database),
        _chunk       (chunk),
        _replicaInfo () {

    serviceProvider.assertDatabaseIsValid (database);
}

WorkerFindRequest::~WorkerFindRequest () {
}

const ReplicaInfo&
WorkerFindRequest::replicaInfo () const {
    return _replicaInfo;
}

bool
WorkerFindRequest::execute (bool incremental) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "execute"
         << "  worker: " << worker()
         << "  database: " << database()
         << "  chunk: " << chunk());

    // Set up the result if the operation is over

    bool completed = WorkerRequest::execute(incremental);
    if (completed) _replicaInfo =
        ReplicaInfo (ReplicaInfo::COMPLETE,
                     worker(),
                     database(),
                     chunk());
    return completed;
}


/////////////////////////////////////////////////////////////////
///////////////////// WorkerFindRequestPOSIX ////////////////////
/////////////////////////////////////////////////////////////////

WorkerFindRequestPOSIX::pointer
WorkerFindRequestPOSIX::create (
        ServiceProvider   &serviceProvider,
        const std::string &worker,
        const std::string &id,
        int                priority,
        const std::string &database,
        unsigned int       chunk) {

    return WorkerFindRequestPOSIX::pointer (
        new WorkerFindRequestPOSIX (
                serviceProvider,
                worker,
                id,
                priority,
                database,
                chunk));
}

WorkerFindRequestPOSIX::WorkerFindRequestPOSIX (
        ServiceProvider   &serviceProvider,
        const std::string &worker,
        const std::string &id,
        int                priority,
        const std::string &database,
        unsigned int       chunk)

    :   WorkerFindRequest (
            serviceProvider,
            worker,
            id,
            priority,
            database,
            chunk) {
}

WorkerFindRequestPOSIX::~WorkerFindRequestPOSIX () {
}

bool
WorkerFindRequestPOSIX::execute (bool incremental) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "execute"
         << "  worker: "   << worker()
         << "  database: " << database()
         << "  chunk: "    << chunk());

    LOCK_DATA_FOLDER;

    const WorkerInfo   &workerInfo   = _serviceProvider.config().workerInfo  (worker  ());
    const DatabaseInfo &databaseInfo = _serviceProvider.config().databaseInfo(database());

    // Check if the data directory exists and it can be read
    
    boost::system::error_code ec;

    const fs::path dataDirPath   = fs::path(workerInfo.dataDir) / database();
    const bool     dataDirExists = fs::exists(dataDirPath, ec);
    if (ec) {
        LOGS(_log, LOG_LVL_ERROR, context() << "execute"
            << "  failed to check the status of data directory: " << dataDirPath
            << "  worker: "   << worker()
            << "  database: " << database()
            << "  chunk: "    << chunk());
        setStatus(STATUS_FAILED);
        return true;
    }
    if (!dataDirExists) {
        LOGS(_log, LOG_LVL_ERROR, context() << "execute"
            << "  data directory doesn't exist: " << dataDirPath
            << "  worker: "   << worker()
            << "  database: " << database()
            << "  chunk: "    << chunk());
        setStatus(STATUS_FAILED);
        return true;
    }

    // For each file associated with the chunk check if the file is present in
    // the data directory.
    //
    // - assume the request failure for any file system operation failure
    // - assume the successfull completion otherwise and adjust the replica
    //   information record accordingly, depending on the findings.

    const std::vector<std::string> files =
        FileUtils::partitionedFiles (databaseInfo, chunk());

    size_t numFilesFound = 0;
    for (const auto &file: files) {
        const fs::path        filePath = dataDirPath / file;
        const fs::file_status fileStat = fs::status(filePath, ec);
        if (fileStat.type() == fs::status_error) {
            LOGS(_log, LOG_LVL_ERROR, context() << "execute"
                << "  failed to check the status of file: " << filePath
                << "  worker: "   << worker()
                << "  database: " << database()
                << "  chunk: "    << chunk());
            setStatus(STATUS_FAILED);
            return true;
        }
        if (fs::exists(fileStat)) ++numFilesFound;
    }

    ReplicaInfo::Status status = ReplicaInfo::Status::NOT_FOUND;
    if (numFilesFound)
        status = files.size() == numFilesFound ?
            ReplicaInfo::Status::COMPLETE :
            ReplicaInfo::Status::INCOMPLETE;
  
    // Fill in the info on the chunk before finishing the operation    
    _replicaInfo = ReplicaInfo (status, worker(), database(), chunk());

    setStatus(STATUS_SUCCEEDED);

    return true;
}


/////////////////////////////////////////////////////////////
///////////////////// WorkerFindRequestX ////////////////////
/////////////////////////////////////////////////////////////

WorkerFindRequestX::pointer
WorkerFindRequestX::create (
        ServiceProvider   &serviceProvider,
        const std::string &worker,
        const std::string &id,
        int                priority,
        const std::string &database,
        unsigned int       chunk) {

    return WorkerFindRequestX::pointer (
        new WorkerFindRequestX (
                serviceProvider,
                worker,
                id,
                priority,
                database,
                chunk));
}

WorkerFindRequestX::WorkerFindRequestX (
        ServiceProvider   &serviceProvider,
        const std::string &worker,
        const std::string &id,
        int                priority,
        const std::string &database,
        unsigned int       chunk)

    :   WorkerFindRequest (
            serviceProvider,
            worker,
            id,
            priority,
            database,
            chunk) {
}

WorkerFindRequestX::~WorkerFindRequestX () {
}


bool
WorkerFindRequestX::execute (bool incremental) {

    // TODO: provide the actual implementation instead of the dummy one.

    return WorkerFindRequest::execute(incremental);
}


}}} // namespace lsst::qserv::replica_core

