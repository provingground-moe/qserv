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

#include "replica_core/Common.h"

// System headers

#include <stdexcept>

// Qserv headers

#include "proto/replication.pb.h"

namespace proto = lsst::qserv::proto;

namespace lsst {
namespace qserv {
namespace replica_core {

std::string
status2string (ExtendedCompletionStatus status) {
    switch (status) {
        case ExtendedCompletionStatus::EXT_STATUS_NONE:        return "EXT_STATUS_NONE";
        case ExtendedCompletionStatus::EXT_STATUS_FOLDER_STAT: return "EXT_STATUS_FOLDER_STAT";
        case ExtendedCompletionStatus::EXT_STATUS_FILE_STAT:   return "EXT_STATUS_FILE_STAT";
        case ExtendedCompletionStatus::EXT_STATUS_FILE_SIZE:   return "EXT_STATUS_FILE_SIZE";
        case ExtendedCompletionStatus::EXT_STATUS_FOLDER_READ: return "EXT_STATUS_FOLDER_READ";
        case ExtendedCompletionStatus::EXT_STATUS_FILE_READ:   return "EXT_STATUS_FILE_READ";
        case ExtendedCompletionStatus::EXT_STATUS_FILE_COPY:   return "EXT_STATUS_FILE_COPY";
        case ExtendedCompletionStatus::EXT_STATUS_FILE_DELETE: return "EXT_STATUS_FILE_DELETE";
        case ExtendedCompletionStatus::EXT_STATUS_FILE_RENAME: return "EXT_STATUS_FILE_RENAME";
        case ExtendedCompletionStatus::EXT_STATUS_FILE_EXISTS: return "EXT_STATUS_FILE_EXISTS";
        case ExtendedCompletionStatus::EXT_STATUS_SPACE_REQ:   return "EXT_STATUS_SPACE_REQ";
        case ExtendedCompletionStatus::EXT_STATUS_NO_FOLDER:   return "EXT_STATUS_NO_FOLDER";
        case ExtendedCompletionStatus::EXT_STATUS_NO_FILE:     return "EXT_STATUS_NO_FILE";
        case ExtendedCompletionStatus::EXT_STATUS_NO_ACCESS:   return "EXT_STATUS_NO_ACCESS";
        case ExtendedCompletionStatus::EXT_STATUS_NO_SPACE:    return "EXT_STATUS_NO_SPACE";
    }
    throw std::logic_error("Common::status2string(ExtendedCompletionStatus) - unhandled status: " + std::to_string(status));
}

ExtendedCompletionStatus
translate (proto::ReplicationStatusExt status) {
    switch (status) {
        case proto::ReplicationStatusExt::NONE:        return ExtendedCompletionStatus::EXT_STATUS_NONE;
        case proto::ReplicationStatusExt::FOLDER_STAT: return ExtendedCompletionStatus::EXT_STATUS_FOLDER_STAT;
        case proto::ReplicationStatusExt::FILE_STAT:   return ExtendedCompletionStatus::EXT_STATUS_FILE_STAT;
        case proto::ReplicationStatusExt::FILE_SIZE:   return ExtendedCompletionStatus::EXT_STATUS_FILE_SIZE;
        case proto::ReplicationStatusExt::FOLDER_READ: return ExtendedCompletionStatus::EXT_STATUS_FOLDER_READ;
        case proto::ReplicationStatusExt::FILE_READ:   return ExtendedCompletionStatus::EXT_STATUS_FILE_READ;
        case proto::ReplicationStatusExt::FILE_COPY:   return ExtendedCompletionStatus::EXT_STATUS_FILE_COPY;
        case proto::ReplicationStatusExt::FILE_DELETE: return ExtendedCompletionStatus::EXT_STATUS_FILE_DELETE;
        case proto::ReplicationStatusExt::FILE_RENAME: return ExtendedCompletionStatus::EXT_STATUS_FILE_RENAME;
        case proto::ReplicationStatusExt::FILE_EXISTS: return ExtendedCompletionStatus::EXT_STATUS_FILE_EXISTS;
        case proto::ReplicationStatusExt::SPACE_REQ:   return ExtendedCompletionStatus::EXT_STATUS_SPACE_REQ;
        case proto::ReplicationStatusExt::NO_FOLDER:   return ExtendedCompletionStatus::EXT_STATUS_NO_FOLDER;
        case proto::ReplicationStatusExt::NO_FILE:     return ExtendedCompletionStatus::EXT_STATUS_NO_FILE;
        case proto::ReplicationStatusExt::NO_ACCESS:   return ExtendedCompletionStatus::EXT_STATUS_NO_ACCESS;
        case proto::ReplicationStatusExt::NO_SPACE:    return ExtendedCompletionStatus::EXT_STATUS_NO_SPACE;
    }
    throw std::logic_error("Common::translate(proto::ReplicationStatusExt) - unhandled status: " + std::to_string(status));
}

proto::ReplicationStatusExt
translate (ExtendedCompletionStatus status) {
    switch (status) {
        case ExtendedCompletionStatus::EXT_STATUS_NONE:        return proto::ReplicationStatusExt::NONE;
        case ExtendedCompletionStatus::EXT_STATUS_FOLDER_STAT: return proto::ReplicationStatusExt::FOLDER_STAT;
        case ExtendedCompletionStatus::EXT_STATUS_FILE_STAT:   return proto::ReplicationStatusExt::FILE_STAT;
        case ExtendedCompletionStatus::EXT_STATUS_FILE_SIZE:   return proto::ReplicationStatusExt::FILE_SIZE;
        case ExtendedCompletionStatus::EXT_STATUS_FOLDER_READ: return proto::ReplicationStatusExt::FOLDER_READ;
        case ExtendedCompletionStatus::EXT_STATUS_FILE_READ:   return proto::ReplicationStatusExt::FILE_READ;
        case ExtendedCompletionStatus::EXT_STATUS_FILE_COPY:   return proto::ReplicationStatusExt::FILE_COPY;
        case ExtendedCompletionStatus::EXT_STATUS_FILE_DELETE: return proto::ReplicationStatusExt::FILE_DELETE;
        case ExtendedCompletionStatus::EXT_STATUS_FILE_RENAME: return proto::ReplicationStatusExt::FILE_RENAME;
        case ExtendedCompletionStatus::EXT_STATUS_FILE_EXISTS: return proto::ReplicationStatusExt::FILE_EXISTS;
        case ExtendedCompletionStatus::EXT_STATUS_SPACE_REQ:   return proto::ReplicationStatusExt::SPACE_REQ;
        case ExtendedCompletionStatus::EXT_STATUS_NO_FOLDER:   return proto::ReplicationStatusExt::NO_FOLDER;
        case ExtendedCompletionStatus::EXT_STATUS_NO_FILE:     return proto::ReplicationStatusExt::NO_FILE;
        case ExtendedCompletionStatus::EXT_STATUS_NO_ACCESS:   return proto::ReplicationStatusExt::NO_ACCESS;
        case ExtendedCompletionStatus::EXT_STATUS_NO_SPACE:    return proto::ReplicationStatusExt::NO_SPACE;
    }
    throw std::logic_error("Common::translate(ExtendedCompletionStatus) - unhandled status: " + std::to_string(status));
}


}}} // namespace lsst::qserv::replica_core