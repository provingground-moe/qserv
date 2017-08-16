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

#include "replica_core/ReplicaDeleteInfo.h"

// System headers

// Qserv headers

#include "proto/replication.pb.h"


namespace proto = lsst::qserv::proto;

namespace {

/// State translation

void setInfoImpl (const lsst::qserv::replica_core::ReplicaDeleteInfo &rdi,
                  proto::ReplicationReplicaDeleteInfo                *info) {

    info->set_progress (rdi.progress());
}
}  // namespace

namespace lsst {
namespace qserv {
namespace replica_core {


ReplicaDeleteInfo::ReplicaDeleteInfo (float progress)
    :   _progress (progress) {
}

ReplicaDeleteInfo::ReplicaDeleteInfo (const proto::ReplicationReplicaDeleteInfo *info) {
    _progress = info->progress();
}


ReplicaDeleteInfo::ReplicaDeleteInfo (ReplicaDeleteInfo const &rdi) {
    _progress = rdi._progress;
}


ReplicaDeleteInfo&
ReplicaDeleteInfo::operator= (ReplicaDeleteInfo const &rdi) {
    if (this != &rdi) {
        _progress = rdi._progress;
    }
    return *this;
}


ReplicaDeleteInfo::~ReplicaDeleteInfo () {
}


proto::ReplicationReplicaDeleteInfo*
ReplicaDeleteInfo::info () const {
    proto::ReplicationReplicaDeleteInfo *ptr = new proto::ReplicationReplicaDeleteInfo;
    ::setInfoImpl(*this, ptr);
    return ptr;
}

void
ReplicaDeleteInfo::setInfo (lsst::qserv::proto::ReplicationReplicaDeleteInfo *info) const {
    ::setInfoImpl(*this, info);
}


std::ostream&
operator<< (std::ostream& os, const ReplicaDeleteInfo &rdi) {
    os  << "ReplicaDeleteInfo"
        << " progress: " << rdi.progress();
    return os;
}

}}} // namespace lsst::qserv::replica_core
