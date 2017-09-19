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

#include "replica_core/WorkerFindAllRequest.h"

// System headers

#include <boost/filesystem.hpp>
#include <map>

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

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica_core.WorkerFindAllRequest");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica_core {


///////////////////////////////////////////////////////////////
///////////////////// WorkerFindAllRequest ////////////////////
///////////////////////////////////////////////////////////////

WorkerFindAllRequest::pointer
WorkerFindAllRequest::create (
        ServiceProvider   &serviceProvider,
        const std::string &worker,
        const std::string &id,
        int                priority,
        const std::string &database,
        bool               computeCheckSum) {

    return WorkerFindAllRequest::pointer (
        new WorkerFindAllRequest (
                serviceProvider,
                worker,
                id,
                priority,
                database,
                computeCheckSum));
}

WorkerFindAllRequest::WorkerFindAllRequest (
        ServiceProvider   &serviceProvider,
        const std::string &worker,
        const std::string &id,
        int                priority,
        const std::string &database,
        bool               computeCheckSum)

    :   WorkerRequest (
            serviceProvider,
            worker,
            "FIND-ALL",
            id,
            priority),

        _database              (database),
        _computeCheckSum       (computeCheckSum),
        _replicaInfoCollection () {
}


WorkerFindAllRequest::~WorkerFindAllRequest () {
}


const ReplicaInfoCollection&
WorkerFindAllRequest::replicaInfoCollection () const {
    return _replicaInfoCollection;
}


bool
WorkerFindAllRequest::execute () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "execute"
         << "  database: " << database());

    // Set up the result if the operation is over

    bool completed = WorkerRequest::execute();
    if (completed)
        for (unsigned int chunk=0; chunk<8; ++chunk)
            _replicaInfoCollection.emplace_back (
                ReplicaInfo::COMPLETE,
                _worker,
                database(),
                chunk,
                ReplicaInfo::FileInfoCollection());

    return completed;
}


////////////////////////////////////////////////////////////////////
///////////////////// WorkerFindAllRequestPOSIX ////////////////////
////////////////////////////////////////////////////////////////////

WorkerFindAllRequestPOSIX::pointer
WorkerFindAllRequestPOSIX::create (
        ServiceProvider   &serviceProvider,
        const std::string &worker,
        const std::string &id,
        int                priority,
        const std::string &database,
        bool               computeCheckSum) {

    return WorkerFindAllRequestPOSIX::pointer (
        new WorkerFindAllRequestPOSIX (
                serviceProvider,
                worker,
                id,
                priority,
                database,
                computeCheckSum));
}

WorkerFindAllRequestPOSIX::WorkerFindAllRequestPOSIX (
        ServiceProvider   &serviceProvider,
        const std::string &worker,
        const std::string &id,
        int                priority,
        const std::string &database,
        bool               computeCheckSum)

    :   WorkerFindAllRequest (
            serviceProvider,
            worker,
            id,
            priority,
            database,
            computeCheckSum) {
}

WorkerFindAllRequestPOSIX::~WorkerFindAllRequestPOSIX () {
}

bool
WorkerFindAllRequestPOSIX::execute () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "execute"
        << "  database: " << database());

    const WorkerInfo   &workerInfo    = _serviceProvider.config().workerInfo  (worker());
    const DatabaseInfo &databaseInfo  = _serviceProvider.config().databaseInfo(database());

    // Scan the data directory to find all files which match the expected pattern(s)
    // and group them by their chunk number

    WorkerRequest::ErrorContext errorContext;
    boost::system::error_code   ec;

    std::map<unsigned int, ReplicaInfo::FileInfoCollection> chunk2fileInfoCollection;
    {
        LOCK_DATA_FOLDER;
        
        const fs::path dataDir = fs::path(workerInfo.dataDir) / database();
        const fs::file_status stat = fs::status(dataDir, ec);
        errorContext = errorContext
            || reportErrorIf (
                    stat.type() == fs::status_error,
                    ExtendedCompletionStatus::EXT_STATUS_FOLDER_STAT,
                    "failed to check the status of directory: " + dataDir.string())
            || reportErrorIf (
                    !fs::exists(stat),
                    ExtendedCompletionStatus::EXT_STATUS_NO_FOLDER,
                    "the directory does not exists: " + dataDir.string());
        try {
            for (fs::directory_entry &entry: fs::directory_iterator(dataDir)) {
                std::tuple<std::string, unsigned int, std::string> parsed;
                if (FileUtils::parsePartitionedFile (
                        parsed,
                        entry.path().filename().string(),
                        databaseInfo)) {

                    LOGS(_log, LOG_LVL_DEBUG, context() << "execute"
                        << "  database: " << database()
                        << "  file: "     << entry.path().filename()
                        << "  table: "    << std::get<0>(parsed)
                        << "  chunk: "    << std::get<1>(parsed)
                        << "  ext: "      << std::get<2>(parsed));

                    std::string cs = "";
                    if (_computeCheckSum) {
                        try {
                            cs = std::to_string(FileUtils::compute_cs (entry.path().string()));
                        } catch (std::exception &ex) {
                            errorContext = errorContext
                                || reportErrorIf (true,
                                                  ExtendedCompletionStatus::EXT_STATUS_FILE_READ,
                                                  ex.what());
                        }
                    }
                    const uint64_t size = fs::file_size(entry.path(), ec);
                    errorContext = errorContext
                        || reportErrorIf (
                                ec,
                                ExtendedCompletionStatus::EXT_STATUS_FILE_SIZE,
                                "failed to read file size: " + entry.path().string());
                        
                    const unsigned chunk = std::get<1>(parsed);

                    chunk2fileInfoCollection[chunk].emplace_back (
                        ReplicaInfo::FileInfo({
                            entry.path().filename().string(),
                            size,
                            cs
                        })
                    );
                }
            }
        } catch (const fs::filesystem_error &ex) {
            errorContext = errorContext
                || reportErrorIf (
                        true,
                        ExtendedCompletionStatus::EXT_STATUS_FOLDER_READ,
                        "failed to read the directory: " + dataDir.string() + ", error: " + std::string(ex.what()));
        }
    }
    if (errorContext.failed) {
        setStatus(STATUS_FAILED, errorContext.extendedStatus);
        return true;
    }

    // Analyze results to see which chunks are complete using chunk 0 as an example
    // of the total number of files which are normally associated with each chunk.

    const size_t numFilesPerChunkRequired =
        FileUtils::partitionedFiles (databaseInfo, 0).size();

    for (auto &entry: chunk2fileInfoCollection) {
        const unsigned int chunk    = entry.first;
        const size_t       numFiles = entry.second.size();
        _replicaInfoCollection.emplace_back (
                numFiles < numFilesPerChunkRequired ? ReplicaInfo::INCOMPLETE : ReplicaInfo::COMPLETE,
                worker(),
                database(),
                chunk,
                chunk2fileInfoCollection[chunk]);
    }
    setStatus(STATUS_SUCCEEDED);
    return true;
}


////////////////////////////////////////////////////////////////
///////////////////// WorkerFindAllRequestX ////////////////////
////////////////////////////////////////////////////////////////

WorkerFindAllRequestX::pointer
WorkerFindAllRequestX::create (
        ServiceProvider   &serviceProvider,
        const std::string &worker,
        const std::string &id,
        int                priority,
        const std::string &database,
        bool               computeCheckSum) {

    return WorkerFindAllRequestX::pointer (
        new WorkerFindAllRequestX (
                serviceProvider,
                worker,
                id,
                priority,
                database,
                computeCheckSum));
}

WorkerFindAllRequestX::WorkerFindAllRequestX (
        ServiceProvider   &serviceProvider,
        const std::string &worker,
        const std::string &id,
        int                priority,
        const std::string &database,
        bool               computeCheckSum)

    :   WorkerFindAllRequest (
            serviceProvider,
            worker,
            id,
            priority,
            database,
            computeCheckSum) {
}

WorkerFindAllRequestX::~WorkerFindAllRequestX () {
}

bool
WorkerFindAllRequestX::execute () {

    // TODO: provide the actual implementation instead of the dummy one.

    return WorkerRequest::execute();
}


}}} // namespace lsst::qserv::replica_core

