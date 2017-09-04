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
#ifndef LSST_QSERV_REPLICA_CORE_COMMON_H
#define LSST_QSERV_REPLICA_CORE_COMMON_H

/// Common.h declares:
///
/// enum ExtendedCompletionStatus
/// (see individual class documentation for more information)

// System headers

#include <string>

// Qserv headers

#include "proto/replication.pb.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {

/// Extended completion status of the worker side file operations
enum ExtendedCompletionStatus {
    EXT_STATUS_NONE,            // unspecified problem
    EXT_STATUS_INVALID_PARAM,   // invalid parameter(s) of a request
    EXT_STATUS_INVALID_ID,      // an invalid request identifier
    EXT_STATUS_DUPLICATE,       // a duplicate request
    EXT_STATUS_FOLDER_STAT,     // failed to obtain fstat() for a folder
    EXT_STATUS_FILE_STAT,       // failed to obtain fstat() for a file
    EXT_STATUS_FILE_SIZE,       // failed to obtain a size of a file
    EXT_STATUS_FOLDER_READ,     // failed to read the contents of a folder
    EXT_STATUS_FILE_READ,       // failed to read the contents of a file
    EXT_STATUS_FILE_COPY,       // failed to copy a file
    EXT_STATUS_FILE_DELETE,     // failed to delete a file
    EXT_STATUS_FILE_RENAME,     // failed to rename a file
    EXT_STATUS_FILE_EXISTS,     // file already exists
    EXT_STATUS_SPACE_REQ,       // space inquery requst failed
    EXT_STATUS_NO_FOLDER,       // folder doesn't exist
    EXT_STATUS_NO_FILE,         // file doesn't exist
    EXT_STATUS_NO_ACCESS,       // no access to a file or a folder
    EXT_STATUS_NO_SPACE         // o spce left on a device as required by an operation
};

/// Return the string representation of the extended status
std::string status2string (ExtendedCompletionStatus status);

/// Translate Protobuf status into the transient one
ExtendedCompletionStatus translate (lsst::qserv::proto::ReplicationStatusExt status);

/// Translate transient extended status into the Protobuf one
lsst::qserv::proto::ReplicationStatusExt translate (ExtendedCompletionStatus status);

}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_COMMON_H