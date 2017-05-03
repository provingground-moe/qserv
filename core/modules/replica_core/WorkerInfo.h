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
#ifndef LSST_QSERV_REPLICA_CORE_WORKERINFO_H
#define LSST_QSERV_REPLICA_CORE_WORKERINFO_H

/// WorkerInfo.h declares:
///
/// class WorkerInfo
/// (see individual class documentation for more information)

// System headers

#include <string>

// Qserv headers

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {

/**
  * Class WorkerInfo encapsulates various configuration information on worker
  * services.
  *
  * IMPLEMENTATION NOTES:
  *
  *   Though the current implementation is rather trivial it allows
  *   an easy migration to a more "dynamic" approach with the lazy loading
  *   of the information from some external source.
  */
class WorkerInfo {

public:

    // Default construction and copy semantics are prohibited

    WorkerInfo () = delete;
    WorkerInfo (WorkerInfo const&) = delete;
    WorkerInfo & operator= (WorkerInfo const&) = delete;

    /**
     * Construct the object
     */
    WorkerInfo (const std::string &name,
                const std::string &svcHost,
                const std::string &svcPort,
                const std::string &xrootdHost,
                const std::string &xrootdPort);

    // Accessors

    const std::string& name () const { return _name; }

    const std::string& svcHost () const { return _svcHost; }
    const std::string& svcPort () const { return _svcPort; }

    const std::string& xrootdHost () const { return _xrootdHost; }
    const std::string& xrootdPort () const { return _xrootdPort; }

private:

    std::string _name;          // the worker identity within Qserv

    std::string _svcHost;       // the host name on which the replication service is run
    std::string _svcPort;       // the port number of the worker replication service

    std::string _xrootdHost;    // the host name on which the XrootD service is run
    std::string _xrootdPort;    // the port number of the XrootD service
};

}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_WORKERINFO_H