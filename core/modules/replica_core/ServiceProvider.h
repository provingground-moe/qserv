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
#ifndef LSST_QSERV_REPLICA_CORE_SERVICEPROVIDER_H
#define LSST_QSERV_REPLICA_CORE_SERVICEPROVIDER_H

/// ServiceProvider.h declares:
///
/// class ServiceProvider
/// (see individual class documentation for more information)

// System headers

#include <memory>       // shared_ptr, enable_shared_from_this
#include <string>
#include <vector>

// Qserv headers

#include "replica_core/Configuration.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {

class WorkerInfo;

/**
  * Class ServiceProvider hosts various serviceses for the master server.
  */
class ServiceProvider
    :   public std::enable_shared_from_this<ServiceProvider>  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<ServiceProvider> pointer;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param configuration - the configuration service
     */
    static pointer create (Configuration::pointer configuration);

    // Default construction and copy semantics are proxibited

    ServiceProvider () = delete;
    ServiceProvider (ServiceProvider const&) = delete;
    ServiceProvider & operator= (ServiceProvider const&) = delete;

    /**
     * Return a pointer to the configuration service
     */
    Configuration::pointer config () const;

    /**
     * Return the names of known workers.
     */
    std::vector<std::string> workers () const;

    /**
     * Get the connection parameters of a worker
     */
    std::shared_ptr<WorkerInfo> workerInfo (const std::string& workerName) const;

private:

    /**
     * Construct the object with the pointer to the configuration service.
     *
     * @param configuration - the configuration service
     */
    ServiceProvider (Configuration::pointer configuration);

private:

    Configuration::pointer _configuration;
};

}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_SERVICEPROVIDER_H