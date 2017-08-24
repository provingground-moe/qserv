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

#include "replica_core/WorkerRequestFactory.h"

// System headers

#include <stdexcept>

// Qserv headers

#include "lsst/log/Log.h"
#include "replica_core/Configuration.h"
#include "replica_core/ServiceProvider.h"
#include "replica_core/WorkerDeleteRequest.h"
#include "replica_core/WorkerFindAllRequest.h"
#include "replica_core/WorkerFindRequest.h"
#include "replica_core/WorkerReplicationRequest.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica_core.WorkerRequestFactory");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica_core {


///////////////////////////////////////////////////////////////////
///////////////////// WorkerRequestFactoryBase ////////////////////
///////////////////////////////////////////////////////////////////

WorkerRequestFactoryBase::WorkerRequestFactoryBase (ServiceProvider &serviceProvider)
    :   _serviceProvider (serviceProvider) {}

WorkerRequestFactoryBase::~WorkerRequestFactoryBase () {}


///////////////////////////////////////////////////////////////////
///////////////////// WorkerRequestFactoryTest ////////////////////
///////////////////////////////////////////////////////////////////

/**
  * Class WorkerRequestFactory is a factory class constructing the test versions
  * of the request objects which make no persistent side effects.
  */
class WorkerRequestFactoryTest
    :   public WorkerRequestFactoryBase {

public:

    // Default construction and copy semantics are proxibited

    WorkerRequestFactoryTest () = delete;
    WorkerRequestFactoryTest (WorkerRequestFactoryTest const&) = delete;
    WorkerRequestFactoryTest & operator= (WorkerRequestFactoryTest const&) = delete;

    /// Normal constructor
    WorkerRequestFactoryTest (ServiceProvider &serviceProvider)
        :   WorkerRequestFactoryBase (serviceProvider) {}
    
    /// Destructor
    ~WorkerRequestFactoryTest () override {}

    /**
     * Implements the corresponding method of the base class
     *
     * @see WorkerReplicationRequestBase::technology
     */
    std::string technology () const { return "TEST"; }

    /**
     * Implements the corresponding method of the base class
     *
     * @see WorkerReplicationRequestBase::createReplicationRequest
     */
    WorkerReplicationRequest_pointer createReplicationRequest (
            const std::string &worker,
            const std::string &id,
            int                priority,
            const std::string &database,
            unsigned int       chunk,
            const std::string &sourceWorker) override {

        return WorkerReplicationRequest::create (
            _serviceProvider,
            worker,
            id,
            priority,
            database,
            chunk,
            sourceWorker);
    }

    /**
     * Implements the corresponding method of the base class
     *
     * @see WorkerReplicationRequestBase::createDeleteRequest
     */
    WorkerDeleteRequest_pointer createDeleteRequest (
            const std::string &worker,
            const std::string &id,
            int                priority,
            const std::string &database,
            unsigned int       chunk) override {

        return WorkerDeleteRequest::create (
            _serviceProvider,
            worker,
            id,
            priority,
            database,
            chunk);
    }

    /**
     * Implements the corresponding method of the base class
     *
     * @see WorkerReplicationRequestBase::createFindRequest
     */
    WorkerFindRequest_pointer createFindRequest (
            const std::string &worker,
             const std::string &id,
             int                priority,
             const std::string &database,
             unsigned int       chunk) override {

        return WorkerFindRequest::create (
            _serviceProvider,
            worker,
            id,
            priority,
            database,
            chunk);
    }

    /**
     * Implements the corresponding method of the base class
     *
     * @see WorkerReplicationRequestBase::createFindAllRequest
     */
    WorkerFindAllRequest_pointer createFindAllRequest (
            const std::string &worker,
            const std::string &id,
            int                priority,
            const std::string &database) override {

        return WorkerFindAllRequest::create (
            _serviceProvider,
            worker,
            id,
            priority,
            database);
    }
};


////////////////////////////////////////////////////////////////////
///////////////////// WorkerRequestFactoryPOSIX ////////////////////
////////////////////////////////////////////////////////////////////

/**
  * Class WorkerRequestFactoryPOSIX creates request objects based on the direct
  * manipulation of files on a POSIX file system.
  */
class WorkerRequestFactoryPOSIX
    :   public WorkerRequestFactoryBase {

public:

    // Default construction and copy semantics are proxibited

    WorkerRequestFactoryPOSIX () = delete;
    WorkerRequestFactoryPOSIX (WorkerRequestFactoryPOSIX const&) = delete;
    WorkerRequestFactoryPOSIX & operator= (WorkerRequestFactoryPOSIX const&) = delete;

    /// Normal constructor
    WorkerRequestFactoryPOSIX (ServiceProvider &serviceProvider)
        :   WorkerRequestFactoryBase (serviceProvider) {}
    
    /// Destructor
    ~WorkerRequestFactoryPOSIX () override {}

    /**
     * Implements the corresponding method of the base class
     *
     * @see WorkerReplicationRequestBase::technology
     */
    std::string technology () const { return "POSIX"; }

    /**
     * Implements the corresponding method of the base class
     *
     * @see WorkerReplicationRequestBase::createReplicationRequest
     */
    WorkerReplicationRequest_pointer createReplicationRequest (
            const std::string &worker,
            const std::string &id,
            int                priority,
            const std::string &database,
            unsigned int       chunk,
            const std::string &sourceWorker) override {

        return WorkerReplicationRequestPOSIX::create (
            _serviceProvider,
            worker,
            id,
            priority,
            database,
            chunk,
            sourceWorker);
    }

    /**
     * Implements the corresponding method of the base class
     *
     * @see WorkerReplicationRequestBase::createDeleteRequest
     */
    WorkerDeleteRequest_pointer createDeleteRequest (
            const std::string &worker,
            const std::string &id,
            int                priority,
            const std::string &database,
            unsigned int       chunk) override {

        return WorkerDeleteRequestPOSIX::create (
            _serviceProvider,
            worker,
            id,
            priority,
            database,
            chunk);
    }

    /**
     * Implements the corresponding method of the base class
     *
     * @see WorkerReplicationRequestBase::createFindRequest
     */
    WorkerFindRequest_pointer createFindRequest (
            const std::string &worker,
             const std::string &id,
             int                priority,
             const std::string &database,
             unsigned int       chunk) override {

        return WorkerFindRequestPOSIX::create (
            _serviceProvider,
            worker,
            id,
            priority,
            database,
            chunk);
    }

    /**
     * Implements the corresponding method of the base class
     *
     * @see WorkerReplicationRequestBase::createFindAllRequest
     */
    WorkerFindAllRequest_pointer createFindAllRequest (
            const std::string &worker,
            const std::string &id,
            int                priority,
            const std::string &database) override {

        return WorkerFindAllRequestPOSIX::create (
            _serviceProvider,
            worker,
            id,
            priority,
            database);
    }
};


////////////////////////////////////////////////////////////////
///////////////////// WorkerRequestFactoryX ////////////////////
////////////////////////////////////////////////////////////////


/**
  * Class WorkerRequestFactoryX creates request objects based on the XRootD
  * implementation of the file system operations.
  */
class WorkerRequestFactoryX
    :   public WorkerRequestFactoryBase {

public:

    // Default construction and copy semantics are proxibited

    WorkerRequestFactoryX () = delete;
    WorkerRequestFactoryX (WorkerRequestFactoryX const&) = delete;
    WorkerRequestFactoryX & operator= (WorkerRequestFactoryX const&) = delete;

    /// Normal constructor
    WorkerRequestFactoryX (ServiceProvider &serviceProvider)
        :   WorkerRequestFactoryBase (serviceProvider) {}
    
    /// Destructor
    ~WorkerRequestFactoryX () override {}

    /**
     * Implements the corresponding method of the base class
     *
     * @see WorkerReplicationRequestBase::technology
     */
    std::string technology () const { return "XROOTD"; }

    /**
     * Implements the corresponding method of the base class
     *
     * @see WorkerReplicationRequestBase::createReplicationRequest
     */
    WorkerReplicationRequest_pointer createReplicationRequest (
            const std::string &worker,
            const std::string &id,
            int                priority,
            const std::string &database,
            unsigned int       chunk,
            const std::string &sourceWorker) override {

        return WorkerReplicationRequestX::create (
            _serviceProvider,
            worker,
            id,
            priority,
            database,
            chunk,
            sourceWorker);
    }

    /**
     * Implements the corresponding method of the base class
     *
     * @see WorkerReplicationRequestBase::createDeleteRequest
     */
    WorkerDeleteRequest_pointer createDeleteRequest (
            const std::string &worker,
            const std::string &id,
            int                priority,
            const std::string &database,
            unsigned int       chunk) override {

        return WorkerDeleteRequestX::create (
            _serviceProvider,
            worker,
            id,
            priority,
            database,
            chunk);
    }

    /**
     * Implements the corresponding method of the base class
     *
     * @see WorkerReplicationRequestBase::createFindRequest
     */
    WorkerFindRequest_pointer createFindRequest (
            const std::string &worker,
             const std::string &id,
             int                priority,
             const std::string &database,
             unsigned int       chunk) override {

        return WorkerFindRequestX::create (
            _serviceProvider,
            worker,
            id,
            priority,
            database,
            chunk);
    }

    /**
     * Implements the corresponding method of the base class
     *
     * @see WorkerReplicationRequestBase::createFindAllRequest
     */
    WorkerFindAllRequest_pointer createFindAllRequest (
            const std::string &worker,
            const std::string &id,
            int                priority,
            const std::string &database) override {

        return WorkerFindAllRequestX::create (
            _serviceProvider,
            worker,
            id,
            priority,
            database);
    }
};


///////////////////////////////////////////////////////////////
///////////////////// WorkerRequestFactory ////////////////////
///////////////////////////////////////////////////////////////

WorkerRequestFactory::WorkerRequestFactory (ServiceProvider   &serviceProvider,
                                            const std::string &technology)
    :   WorkerRequestFactoryBase (serviceProvider) {
        
    const std::string finalTechnology =
        technology.empty() ? serviceProvider.config().workerTechnology() : technology;

    if      (finalTechnology == "TEST")   _ptr = new WorkerRequestFactoryTest  (serviceProvider);
    else if (finalTechnology == "POSIX")  _ptr = new WorkerRequestFactoryPOSIX (serviceProvider);
    else if (finalTechnology == "XROOTD") _ptr = new WorkerRequestFactoryX     (serviceProvider);
    else
        throw std::invalid_argument("WorkerRequestFactory::WorkerRequestFactory() unknown technology: '" +
                                    finalTechnology);
}

}}} // namespace lsst::qserv::replica_core

