/*
 * LSST Data Management System
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
#ifndef LSST_QSERV_REPLICA_WORKERREQUESTFACTORY_H
#define LSST_QSERV_REPLICA_WORKERREQUESTFACTORY_H

// System headers
#include <memory>
#include <string>

// Qserv headers
#include "replica/ServiceProvider.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace replica {
    class ProtocolRequestSql;
    class WorkerDeleteRequest;
    class WorkerEchoRequest;
    class WorkerFindAllRequest;
    class WorkerFindRequest;
    class WorkerSqlRequest;
    class WorkerReplicationRequest;
}}}  // Forward declarations

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class WorkerRequestFactoryBase is an abstract base class for a family of
 * various implementations of factories for creating request objects.
 */
class WorkerRequestFactoryBase {

public:

    // Pointers to specific request types

    typedef std::shared_ptr<WorkerDeleteRequest>      WorkerDeleteRequestPtr;
    typedef std::shared_ptr<WorkerEchoRequest>        WorkerEchoRequestPtr;
    typedef std::shared_ptr<WorkerFindRequest>        WorkerFindRequestPtr;
    typedef std::shared_ptr<WorkerFindAllRequest>     WorkerFindAllRequestPtr;
    typedef std::shared_ptr<WorkerReplicationRequest> WorkerReplicationRequestPtr;
    typedef std::shared_ptr<WorkerSqlRequest>         WorkerSqlRequestPtr;

    // The default constructor and copy semantics are prohibited

    WorkerRequestFactoryBase() = delete;
    WorkerRequestFactoryBase(WorkerRequestFactoryBase const&) = delete;
    WorkerRequestFactoryBase& operator=(WorkerRequestFactoryBase const&) = delete;

    virtual ~WorkerRequestFactoryBase() = default;

    /// @return the name of a technology the factory is based upon
    virtual std::string technology() const = 0;

    /**
     * Create an instance of the replication request
     *
     * @see class WorkerReplicationRequest
     *
     * @return
     *   a pointer to the newly created object
     */
    virtual WorkerReplicationRequestPtr createReplicationRequest(
            std::string const& worker,
            std::string const& id,
            int priority,
            std::string const& database,
            unsigned int chunk,
            std::string const& sourceWorker) const = 0;

   /**
    * Create an instance of the replica deletion request
    *
    * @see class WorkerDeleteRequest
    *
    * @return
    *   a pointer to the newly created object
    */
    virtual WorkerDeleteRequestPtr createDeleteRequest(
            std::string const& worker,
            std::string const& id,
            int priority,
            std::string const& database,
            unsigned int chunk) const = 0;

   /**
    * Create an instance of the replica lookup request
    *
    * @see class WorkerFindRequest
    *
    * @return
    *   a pointer to the newly created object
    */
    virtual WorkerFindRequestPtr createFindRequest(
            std::string const& worker,
            std::string const& id,
            int priority,
            std::string const& database,
            unsigned int chunk,
            bool computeCheckSum) const = 0;

   /**
    * Create an instance of the replicas lookup request
    *
    * @see class WorkerFindAllRequest
    *
    * @return
    *   a pointer to the newly created object
    */
    virtual WorkerFindAllRequestPtr createFindAllRequest(
            std::string const& worker,
            std::string const& id,
            int priority,
            std::string const& database) const = 0;

    /**
     * Create an instance of the test request
     *
     * @see class WorkerEchoRequest
     *
     * @return
     *   a pointer to the newly created object
     */
    virtual WorkerEchoRequestPtr createEchoRequest(
            std::string const& worker,
            std::string const& id,
            int priority,
            std::string const& data,
            uint64_t delay) const = 0;
 
    /**
     * Create an instance of the query execution request
     *
     * @see class WorkerSqlRequest
     *
     * @return
     *   a pointer to the newly created object
     */
    virtual WorkerSqlRequestPtr createSqlRequest(
            std::string const& worker,
            std::string const& id,
            ProtocolRequestSql const& request) const = 0;
 
protected:

    /**
     * The constructor of the class.
     *
     * @param serviceProvider
     *   a provider of various services
     */
    explicit WorkerRequestFactoryBase(ServiceProvider::Ptr const& serviceProvider);

protected:

    ServiceProvider::Ptr const _serviceProvider;
};


/**
  * Class WorkerRequestFactory is a proxy class which is constructed with
  * a choice of a specific implementation of the factory.
  */
class WorkerRequestFactory : public WorkerRequestFactoryBase {

public:

    // Default construction and copy semantics are prohibited

    WorkerRequestFactory() = delete;
    WorkerRequestFactory(WorkerRequestFactory const&) = delete;
    WorkerRequestFactory& operator=(WorkerRequestFactory const&) = delete;

    /**
     * The constructor of the class.
     *
     * The technology name must be valid. Otherwise std::invalid_argument will
     * be thrown. If the default value of the parameter is assumed then the one
     * from the current configuration will be assumed.
     *
     * This is the list of technologies which are presently supported:
     *
     *   'TEST'   - request objects which are meant to be used for testing the framework
     *              operation w/o making any persistent side effects.
     *
     *   'POSIX'  - request objects based on the direct manipulation of files
     *              on a POSIX file system.
     *
     *   'FS'     - request objects based on the direct manipulation of local files
     *              on a POSIX file system and for reading remote files using
     *              the built-into-worker simple file server.
     *
     * @param serviceProvider
     *   provider of various services (including configurations)
     *
     * @param technology
     *   (optional) the name of a technology
     */
    explicit WorkerRequestFactory(ServiceProvider::Ptr const& serviceProvider,
                                  std::string const& technology=std::string());

    ~WorkerRequestFactory() final { delete _ptr; }

    /// @see WorkerReplicationRequestBase::technology()
    std::string technology() const final { return _ptr->technology(); }

    /// @see WorkerReplicationRequestBase::createReplicationRequest()
    WorkerReplicationRequestPtr createReplicationRequest(
            std::string const& worker,
            std::string const& id,
            int priority,
            std::string const& database,
            unsigned int chunk,
            std::string const& sourceWorker) const final {

        return _ptr->createReplicationRequest(
            worker,
            id,
            priority,
            database,
            chunk,
            sourceWorker);
    }

   /// @see WorkerReplicationRequestBase::createDeleteRequest()
    WorkerDeleteRequestPtr createDeleteRequest(
            std::string const& worker,
            std::string const& id,
            int priority,
            std::string const& database,
            unsigned int chunk) const final {

        return _ptr->createDeleteRequest(
            worker,
            id,
            priority,
            database,
            chunk);
    }

   /// @see WorkerReplicationRequestBase::createFindRequest()
    WorkerFindRequestPtr createFindRequest(
            std::string const& worker,
            std::string const& id,
            int priority,
            std::string const& database,
            unsigned int chunk,
            bool computeCheckSum) const final {
        
        return _ptr->createFindRequest(
            worker,
            id,
            priority,
            database,
            chunk,
            computeCheckSum);
    }

   /// @see WorkerReplicationRequestBase::createFindAllRequest()
    WorkerFindAllRequestPtr createFindAllRequest(
            std::string const& worker,
            std::string const& id,
            int priority,
            std::string const& database) const final {
        
        return _ptr->createFindAllRequest(
            worker,
            id,
            priority,
            database);
    }

    /// @see WorkerReplicationRequestBase::createEchoRequest()
    WorkerEchoRequestPtr createEchoRequest(
            std::string const& worker,
            std::string const& id,
            int priority,
            std::string const& data,
            uint64_t delay) const final {

        return _ptr->createEchoRequest(
            worker,
            id,
            priority,
            data,
            delay);
    }

    /// @see WorkerReplicationRequestBase::createSqlRequest()
    WorkerSqlRequestPtr createSqlRequest(
            std::string const& worker,
            std::string const& id,
            ProtocolRequestSql const& request) const final {

        return _ptr->createSqlRequest(
            worker,
            id,
            request);
    }

protected:

    /// Pointer to the final implementation of the factory
    WorkerRequestFactoryBase const* _ptr;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_WORKERREQUESTFACTORY_H
