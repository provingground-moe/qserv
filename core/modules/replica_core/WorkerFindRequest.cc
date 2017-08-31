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
        unsigned int       chunk,
        bool               computeCheckSum) {

    return WorkerFindRequest::pointer (
        new WorkerFindRequest (
                serviceProvider,
                worker,
                id,
                priority,
                database,
                chunk,
                computeCheckSum));
}

WorkerFindRequest::WorkerFindRequest (
        ServiceProvider   &serviceProvider,
        const std::string &worker,
        const std::string &id,
        int                priority,
        const std::string &database,
        unsigned int       chunk,
        bool               computeCheckSum)

    :   WorkerRequest (
            serviceProvider,
            worker,
            "FIND",
            id,
            priority),

        _database        (database),
        _chunk           (chunk),
        _computeCheckSum (computeCheckSum),
        _replicaInfo     () {

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
         << "  database: " << database()
         << "  chunk: "    << chunk());

    // Set up the result if the operation is over

    bool completed = WorkerRequest::execute(incremental);
    if (completed) _replicaInfo =
        ReplicaInfo (ReplicaInfo::COMPLETE,
                     worker(),
                     database(),
                     chunk(),
                     ReplicaInfo::FileInfoCollection());
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
        unsigned int       chunk,
        bool               computeCheckSum) {

    return WorkerFindRequestPOSIX::pointer (
        new WorkerFindRequestPOSIX (
                serviceProvider,
                worker,
                id,
                priority,
                database,
                chunk,
                computeCheckSum));
}

WorkerFindRequestPOSIX::WorkerFindRequestPOSIX (
        ServiceProvider   &serviceProvider,
        const std::string &worker,
        const std::string &id,
        int                priority,
        const std::string &database,
        unsigned int       chunk,
        bool               computeCheckSum)

    :   WorkerFindRequest (
            serviceProvider,
            worker,
            id,
            priority,
            database,
            chunk,
            computeCheckSum) {
}

WorkerFindRequestPOSIX::~WorkerFindRequestPOSIX () {
}

bool
WorkerFindRequestPOSIX::execute (bool incremental) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "execute"
         << "  database: " << database()
         << "  chunk: "    << chunk());

    const WorkerInfo   &workerInfo   = _serviceProvider.config().workerInfo  (worker  ());
    const DatabaseInfo &databaseInfo = _serviceProvider.config().databaseInfo(database());

    // Check if the data directory exists and it can be read
    
    WorkerRequest::ErrorContext errorContext;
    boost::system::error_code   ec;

    LOCK_DATA_FOLDER;

    const fs::path        dataDir = fs::path(workerInfo.dataDir) / database();
    const fs::file_status stat    = fs::status(dataDir, ec);

    errorContext = errorContext
        || reportErrorIf (
                stat.type() == fs::status_error,
                EXT_STATUS_FOLDER_STAT,
                "failed to check the status of directory: " + dataDir.string())
        || reportErrorIf (
                !fs::exists(stat),
                EXT_STATUS_NO_FOLDER,
                "the directory does not exists: " + dataDir.string());

    if (errorContext.failed) {
        setStatus(STATUS_FAILED, errorContext.extendedStatus);
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

    ReplicaInfo::FileInfoCollection fileInfoCollection;
    for (const auto &file: files) {

        const fs::path        path = dataDir / file;
        const fs::file_status stat = fs::status(path, ec);

        errorContext = errorContext
            || reportErrorIf (
                    stat.type() == fs::status_error,
                    EXT_STATUS_FILE_STAT,
                    "failed to check the status of file: " + path.string());

        // Pull extra info on the file
        if (fs::exists(stat)) {

            std::string cs = "";
            if (_computeCheckSum) {
                try {
                    cs = std::to_string(FileUtils::compute_cs (path.string()));
                } catch (std::exception &ex) {
                    errorContext = errorContext
                        || reportErrorIf (true,
                                          EXT_STATUS_FILE_READ,
                                          ex.what());
                }
            }
            const uint64_t size = fs::file_size(path, ec);
            errorContext = errorContext
                || reportErrorIf (
                        ec,
                        EXT_STATUS_FILE_SIZE,
                        "failed to read file size: " + path.string());
                
            fileInfoCollection.emplace_back (
                ReplicaInfo::FileInfo({
                    file, size, cs
                })
            );
        }
    }
    if (errorContext.failed) {
        setStatus(STATUS_FAILED, errorContext.extendedStatus);
        return true;
    }

    ReplicaInfo::Status status = ReplicaInfo::Status::NOT_FOUND;
    if (fileInfoCollection.size())
        status = files.size() == fileInfoCollection.size() ?
            ReplicaInfo::Status::COMPLETE :
            ReplicaInfo::Status::INCOMPLETE;
  
    // Fill in the info on the chunk before finishing the operation    

    _replicaInfo = ReplicaInfo (
            status, worker(), database(), chunk(), fileInfoCollection);

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
        unsigned int       chunk,
        bool               computeCheckSum) {

    return WorkerFindRequestX::pointer (
        new WorkerFindRequestX (
                serviceProvider,
                worker,
                id,
                priority,
                database,
                chunk,
                computeCheckSum));
}

WorkerFindRequestX::WorkerFindRequestX (
        ServiceProvider   &serviceProvider,
        const std::string &worker,
        const std::string &id,
        int                priority,
        const std::string &database,
        unsigned int       chunk,
        bool               computeCheckSum)

    :   WorkerFindRequest (
            serviceProvider,
            worker,
            id,
            priority,
            database,
            chunk,
            computeCheckSum) {
}

WorkerFindRequestX::~WorkerFindRequestX () {
}


bool
WorkerFindRequestX::execute (bool incremental) {

    // TODO: provide the actual implementation instead of the dummy one.

    return WorkerFindRequest::execute(incremental);
}


}}} // namespace lsst::qserv::replica_core

