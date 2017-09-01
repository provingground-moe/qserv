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

#include "replica_core/WorkerReplicationRequest.h"

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

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica_core.WorkerReplicationRequest");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica_core {


///////////////////////////////////////////////////////////////////
///////////////////// WorkerReplicationRequest ////////////////////
///////////////////////////////////////////////////////////////////

WorkerReplicationRequest::pointer
WorkerReplicationRequest::create (
        ServiceProvider   &serviceProvider,
        const std::string &worker,
        const std::string &id,
        int                priority,
        const std::string &database,
        unsigned int       chunk,
        const std::string &sourceWorker) {

    return WorkerReplicationRequest::pointer (
        new WorkerReplicationRequest (
                serviceProvider,
                worker,
                id,
                priority,
                database,
                chunk,
                sourceWorker));
}

WorkerReplicationRequest::WorkerReplicationRequest (
        ServiceProvider   &serviceProvider,
        const std::string &worker,
        const std::string &id,
        int                priority,
        const std::string &database,
        unsigned int       chunk,
        const std::string &sourceWorker)

    :   WorkerRequest (
            serviceProvider,
            worker,
            "REPLICATE",
            id,
            priority),

        _database        (database),
        _chunk           (chunk),
        _sourceWorker    (sourceWorker),
        _replicationInfo () {

    _serviceProvider.assertWorkerIsValid       (sourceWorker);
    _serviceProvider.assertWorkersAreDifferent (worker, sourceWorker);
}


WorkerReplicationRequest::~WorkerReplicationRequest () {
}


bool
WorkerReplicationRequest::execute (bool incremental) {

   LOGS(_log, LOG_LVL_DEBUG, context() << "execute"
         << "  sourceWorker: " << sourceWorker()
         << "  db: "           << database()
         << "  chunk: "        << chunk());

    // TODO: provide the actual implementation instead of the dummy one.

    const bool complete = WorkerRequest::execute(incremental);
    if (complete) {
        _replicationInfo = ReplicaCreateInfo(100.);     // simulate 100% completed
    }
    return complete;
}


////////////////////////////////////////////////////////////////////////
///////////////////// WorkerReplicationRequestPOSIX ////////////////////
////////////////////////////////////////////////////////////////////////

WorkerReplicationRequestPOSIX::pointer
WorkerReplicationRequestPOSIX::create (
        ServiceProvider   &serviceProvider,
        const std::string &worker,
        const std::string &id,
        int                priority,
        const std::string &database,
        unsigned int       chunk,
        const std::string &sourceWorker) {

    return WorkerReplicationRequestPOSIX::pointer (
        new WorkerReplicationRequestPOSIX (
                serviceProvider,
                worker,
                id,
                priority,
                database,
                chunk,
                sourceWorker));
}

WorkerReplicationRequestPOSIX::WorkerReplicationRequestPOSIX (
        ServiceProvider   &serviceProvider,
        const std::string &worker,
        const std::string &id,
        int                priority,
        const std::string &database,
        unsigned int       chunk,
        const std::string &sourceWorker)

    :   WorkerReplicationRequest (
                serviceProvider,
                worker,
                id,
                priority,
                database,
                chunk,
                sourceWorker) {
}

WorkerReplicationRequestPOSIX::~WorkerReplicationRequestPOSIX () {
}

bool
WorkerReplicationRequestPOSIX::execute (bool incremental) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "execute"
         << "  sourceWorker: " << sourceWorker()
         << "  database: "     << database()
         << "  chunk: "        << chunk());

    // Obtain the list of files to be migrated
    //
    // IMPLEMENTATION NOTES:
    //
    // - Note using the overloaded operator '/' which is used to form
    //   folders and files path names below. The operator will concatename
    //   names and also insert a file separator for an operationg system
    //   on which this code will get compiled.
    //
    // - Temporary file names at a destination folders are prepended with
    //   prefix '_' to prevent colliding with the canonical names. They will
    //   be renamed in the last step.
    //
    // - All operations with the file system namespace (creating new non-temporary
    //   files, checking for folders and files, renaming files, creating folders, etc.)
    //   are guarded by acquering LOCK_DATA_FOLDER where it's needed.

    const WorkerInfo   &inWorkerInfo  = _serviceProvider.config().workerInfo  (sourceWorker());
    const WorkerInfo   &outWorkerInfo = _serviceProvider.config().workerInfo  (worker());
    const DatabaseInfo &databaseInfo  = _serviceProvider.config().databaseInfo(database());

    const fs::path inDir  = fs::path(inWorkerInfo.dataDir)  / database();
    const fs::path outDir = fs::path(outWorkerInfo.dataDir) / database();

    const std::vector<std::string> files =
        FileUtils::partitionedFiles (databaseInfo, chunk());

    std::vector<fs::path> inFiles;
    std::vector<fs::path> tmpFiles;
    std::vector<fs::path> outFiles;

    std::map<std::string,fs::path> file2inFile;
    std::map<std::string,fs::path> file2tmpFile;
    std::map<std::string,fs::path> file2outFile;

    for (const auto &file: files) {

        const fs::path inFile = inDir / file;
        inFiles.push_back(inFile);
        file2inFile[file] = inFile;

        const fs::path tmpFile = outDir / ("_" + file);
        tmpFiles.push_back(tmpFile);
        file2tmpFile[file] = tmpFile;

        const fs::path outFile = outDir / file;
        outFiles.push_back(outFile);
        file2outFile[file] = outFile;
    }

    // Check input files, check and sanitize the destination folder

    uintmax_t totalBytes = 0;   // the total number of bytes in all input files to be moved

    WorkerRequest::ErrorContext errorContext;
    boost::system::error_code   ec;
    {
        LOCK_DATA_FOLDER;

        // Check for a presence of input files and calculate space requirement

        for (const auto &file: inFiles) {
            const fs::file_status stat = fs::status(file, ec);
            errorContext = errorContext
                || reportErrorIf (
                        stat.type() == fs::status_error,
                        ExtendedCompletionStatus::EXT_STATUS_FILE_STAT,
                        "failed to check the status of input file: " + file.string())
                || reportErrorIf (
                        !fs::exists(stat),
                        ExtendedCompletionStatus::EXT_STATUS_NO_FILE,
                        "the input file does not exist: " + file.string());

            totalBytes += fs::file_size(file, ec);
            errorContext = errorContext
                || reportErrorIf (
                        ec,
                        ExtendedCompletionStatus::EXT_STATUS_FILE_SIZE,
                        "failed to get the size of input file: " + file.string());
        }

        const bool outDirExists = fs::exists(outDir, ec);
        errorContext = errorContext
            || reportErrorIf (
                    ec,
                    ExtendedCompletionStatus::EXT_STATUS_FOLDER_STAT,
                    "failed to check the status of output directory: " + outDir.string())
            || reportErrorIf (
                    !outDirExists,
                    ExtendedCompletionStatus::EXT_STATUS_NO_FOLDER,
                    "the output directory doesn't exist: " + outDir.string());

        // The files with canonical(!) names should NOT exist at the destination
        // folder.

        for (const auto &file: outFiles) {
            const fs::file_status stat = fs::status(file, ec);
            errorContext = errorContext
                || reportErrorIf (
                        stat.type() == fs::status_error,
                        ExtendedCompletionStatus::EXT_STATUS_FILE_STAT,
                        "failed to check the status of output file: " + file.string())
                || reportErrorIf (
                        fs::exists(stat),
                        ExtendedCompletionStatus::EXT_STATUS_FILE_EXISTS,
                        "the output file already exists: " + file.string());
        }

        // Check if there are any files with the temporary names at the destination
        // folder and if so then get rid of them.

        for (const auto &file: tmpFiles) {
            const fs::file_status stat = fs::status(file, ec);
            errorContext = errorContext
                || reportErrorIf (
                        stat.type() == fs::status_error,
                        ExtendedCompletionStatus::EXT_STATUS_FILE_STAT,
                        "failed to check the status of temporary file: " + file.string());

            if (fs::exists(stat)) {
                fs::remove(file, ec);
                errorContext = errorContext
                    || reportErrorIf (
                            ec,
                            ExtendedCompletionStatus::EXT_STATUS_FILE_DELETE,
                            "failed to remove temporary file: " + file.string());
            }
        }

        // Make sure a file system at the destination has enough space
        // to accomodate new files
        //
        // NOTE: this operation runs after cleaning up temporary files

        const fs::space_info space = fs::space(outDir, ec);
        errorContext = errorContext
            || reportErrorIf (
                    ec,
                    ExtendedCompletionStatus::EXT_STATUS_SPACE_REQ,
                    "failed to obtaine space information at output folder: " + outDir.string())
            || reportErrorIf (
                    space.available < totalBytes,
                    ExtendedCompletionStatus::EXT_STATUS_NO_SPACE,
                    "not enough free space availble at output folder: " + outDir.string());
    }
    if (errorContext.failed) {
        setStatus(STATUS_FAILED, errorContext.extendedStatus);
        return true;
    }

    // Begin copying files into the destination folder under their
    // temporary names w/o acquring the directory lock.

    for (const auto &file: files) {

        const fs::path inFile  = file2inFile [file];
        const fs::path tmpFile = file2tmpFile[file];

        fs::copy_file(inFile, tmpFile, ec);
        errorContext = errorContext
            || reportErrorIf (
                    ec,
                    ExtendedCompletionStatus::EXT_STATUS_FILE_COPY,
                    "failed to copy file: " + inFile.string() + " into: " + tmpFile.string());
    }
    if (errorContext.failed) {
        setStatus(STATUS_FAILED, errorContext.extendedStatus);
        return true;
    }

    // Rename temporary files into the canonical ones
    // Note that this operation changes the directory namespace in a way
    // which may affect other users (like replica lookup operations, etc.). Hence we're
    // acquering the directory lock to guarantee a consistent view onto the folder.

    {
        LOCK_DATA_FOLDER;

        // ATTENTION: as per ISO/IEC 9945 thie file rename operation will
        //            remove empty files. Not sure if this should be treated
        //            in a special way?

        for (const auto &file: files) {

            const fs::path tmpFile = file2tmpFile[file];
            const fs::path outFile = file2outFile[file];

            fs::rename(tmpFile, outFile, ec);
            errorContext = errorContext
                || reportErrorIf (
                        ec,
                        ExtendedCompletionStatus::EXT_STATUS_FILE_RENAME,
                        "failed to rename file: " + tmpFile.string());
        }
    }
    if (errorContext.failed) {
        setStatus(STATUS_FAILED, errorContext.extendedStatus);
        return true;
    }

    // For now (before finalizing the progress reporting protocol) just return
    // the perentage of the total amount of data moved
 
    _replicationInfo = ReplicaCreateInfo(100.);

    setStatus(STATUS_SUCCEEDED);
    return true;
}


////////////////////////////////////////////////////////////////////
///////////////////// WorkerReplicationRequestX ////////////////////
////////////////////////////////////////////////////////////////////

WorkerReplicationRequestX::pointer
WorkerReplicationRequestX::create (
        ServiceProvider   &serviceProvider,
        const std::string &worker,
        const std::string &id,
        int                priority,
        const std::string &database,
        unsigned int       chunk,
        const std::string &sourceWorker) {

    return WorkerReplicationRequestX::pointer (
        new WorkerReplicationRequestX (
                serviceProvider,
                worker,
                id,
                priority,
                database,
                chunk,
                sourceWorker));
}

WorkerReplicationRequestX::WorkerReplicationRequestX (
        ServiceProvider   &serviceProvider,
        const std::string &worker,
        const std::string &id,
        int                priority,
        const std::string &database,
        unsigned int       chunk,
        const std::string &sourceWorker)

    :   WorkerReplicationRequest (
                serviceProvider,
                worker,
                id,
                priority,
                database,
                chunk,
                sourceWorker) {
}

WorkerReplicationRequestX::~WorkerReplicationRequestX () {
}

bool
WorkerReplicationRequestX::execute (bool incremental) {

    // TODO: provide the actual implementation instead of the dummy one.

    return WorkerReplicationRequest::execute(incremental);
}

}}} // namespace lsst::qserv::replica_core

