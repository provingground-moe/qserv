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
#ifndef LSST_QSERV_REPLICA_CORE_REPLICADELETEINFO_H
#define LSST_QSERV_REPLICA_CORE_REPLICADELETEINFO_H

/// ReplicaDeleteInfo.h declares:
///
/// struct ReplicaDeleteInfo
/// (see individual class documentation for more information)

// System headers

#include <ostream>
#include <string>

// Qserv headers


// Forward declarations

namespace lsst {
namespace qserv {
namespace proto {

class ReplicationReplicaDeleteInfo;

}}} // namespace lsst::qserv::proto


// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {

/**
  * Class ReplicaDeleteInfo represents an extended status of a replica deletion
  * request witghin the worker service.
  *
  * Note that this class can only be constructed from an object of
  * the corresponding protobuf type. And there is a complementary operation
  * for translating the state of this class's object into an object of
  * the same protobuf type.
  */
class ReplicaDeleteInfo {

public:

    /**
     * Construct with the specified state. This is also the default constructor.
     *
     * @param progress - the progress of the operation
     */
    explicit ReplicaDeleteInfo (float progress = 0.);

    /// Construct from a protobuf object
    explicit ReplicaDeleteInfo (const lsst::qserv::proto::ReplicationReplicaDeleteInfo *info);

    /// Copy constructor
    ReplicaDeleteInfo (ReplicaDeleteInfo const &rdi);

    /// Assignment operator
    ReplicaDeleteInfo& operator= (ReplicaDeleteInfo const &rdi);

    /// Destructor
    ~ReplicaDeleteInfo ();

    // Trivial accessors

    float progress () const { return _progress; }

    /**
     * Return a protobuf object
     *
     * OWNERSHIP TRANSFER NOTE:
     *    this method allocates a new object and returns a pointer along
     *    with its ownership.
     */
    lsst::qserv::proto::ReplicationReplicaDeleteInfo *info () const;

    /**
     * Initialize a protobuf object from the object's state
     */
    void setInfo (lsst::qserv::proto::ReplicationReplicaDeleteInfo *info) const;

private:

    // Data members

    float _progress;
};


/// Overloaded streaming operator for type ReplicaDeleteInfo
std::ostream& operator<< (std::ostream &os, const ReplicaDeleteInfo &rdi);


}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_REPLICADELETEINFO_H