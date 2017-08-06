// -*- LSST-C++ -*-
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
#ifndef LSST_QSERV_REPLICA_CORE_WORKERREQUESTFACTORY_H
#define LSST_QSERV_REPLICA_CORE_WORKERREQUESTFACTORY_H

/// WorkerRequestFactory.h declares:
///
/// class WorkerRequestFactory
/// class WorkerRequestFactoryX
/// (see individual class documentation for more information)

// System headers

#include <memory>   // shared_ptr
#include <string>

// Qserv headers

#include "replica_core/ServiceProvider.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {

// Forward declarations

class WorkerReplicationRequest;
class WorkerDeleteRequest;
class WorkerFindRequest;
class WorkerFindAllRequest;

/**
  * Class WorkerRequestFactory provides the default implementation of
  * the factory methods which creates dummy versions of the request
  * objects. These objects are ment to be used for testing the framework
  * operation w/o making any persistent side effects.
  */
class WorkerRequestFactory {

public:

    // Pointers to specific request types

    using WorkerReplicationRequest_pointer = std::shared_ptr<WorkerReplicationRequest>;
    using WorkerDeleteRequest_pointer      = std::shared_ptr<WorkerDeleteRequest>;
    using WorkerFindRequest_pointer        = std::shared_ptr<WorkerFindRequest>;
    using WorkerFindAllRequest_pointer     = std::shared_ptr<WorkerFindAllRequest>;

    // Default construction and copy semantics are proxibited

    WorkerRequestFactory () = delete;
    WorkerRequestFactory (WorkerRequestFactory const&) = delete;
    WorkerRequestFactory & operator= (WorkerRequestFactory const&) = delete;

    /**
     * The constructor of the class.
     */
    explicit WorkerRequestFactory (const ServiceProvider::pointer &serviceProvider);

    /// Destructor
    virtual ~WorkerRequestFactory ();

    /**
     * Create an instance of the replication request
     *
     * @see class WorkerReplicationRequest
     *
     * @return a pointer to the newely created object
     */
    virtual WorkerReplicationRequest_pointer createReplicationRequest (
            int                priority,
            const std::string &id,
            const std::string &database,
            unsigned int       chunk);

   /**
     * Create an instance of the replica deletion request
     *
     * @see class WorkerDeleteRequest
     *
     * @return a pointer to the newely created object
     */
    virtual WorkerDeleteRequest_pointer createDeleteRequest (
            int                priority,
            const std::string &id,
            const std::string &database,
            unsigned int       chunk);

   /**
     * Create an instance of the replica lookup request
     *
     * @see class WorkerFindRequest
     *
     * @return a pointer to the newely created object
     */
    virtual WorkerFindRequest_pointer createFindRequest (
            int                priority,
            const std::string &id,
            const std::string &database,
            unsigned int       chunk);

   /**
     * Create an instance of the replicas lookup request
     *
     * @see class WorkerFindAllRequest
     *
     * @return a pointer to the newely created object
     */
    virtual WorkerFindAllRequest_pointer createFindAllRequest (
            int                priority,
            const std::string &id,
            const std::string &database);

protected:

    // Parameters of the object

    ServiceProvider::pointer _serviceProvider;
};


/**
  * Class WorkerRequestFactoryX creates request objects based on the XRootD
  * implementation of the file system operations.
  */
class WorkerRequestFactoryX {

public:

    // Default construction and copy semantics are proxibited

    WorkerRequestFactoryX () = delete;
    WorkerRequestFactoryX (WorkerRequestFactoryX const&) = delete;
    WorkerRequestFactoryX & operator= (WorkerRequestFactoryX const&) = delete;

    /**
     * The constructor of the class.
     */
    explicit WorkerRequestFactoryX (const ServiceProvider::pointer &serviceProvider);

    /// Destructor
    ~WorkerRequestFactoryX () override;

    /**
     * Create an instance of the replication request
     *
     * @see class WorkerReplicationRequest
     *
     * @return a pointer to the newely created object
     */
    WorkerReplicationRequest_pointer createReplicationRequest (
            int                priority,
            const std::string &id,
            const std::string &database,
            unsigned int       chunk) override;

   /**
     * Create an instance of the replica deletion request
     *
     * @see class WorkerDeleteRequest
     *
     * @return a pointer to the newely created object
     */
    WorkerDeleteRequest_pointer createDeleteRequest (
            int                priority,
            const std::string &id,
            const std::string &database,
            unsigned int       chunk) override;

   /**
     * Create an instance of the replica lookup request
     *
     * @see class WorkerFindRequest
     *
     * @return a pointer to the newely created object
     */
    WorkerFindRequest_pointer createFindRequest (
            int                priority,
            const std::string &id,
            const std::string &database,
            unsigned int       chunk) override;

   /**
     * Create an instance of the replicas lookup request
     *
     * @see class WorkerFindAllRequest
     *
     * @return a pointer to the newely created object
     */
    WorkerFindAllRequest_pointer createFindAllRequest (
            int                priority,
            const std::string &id,
            const std::string &database) override;
};

}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_WORKERREQUESTFACTORY_H
