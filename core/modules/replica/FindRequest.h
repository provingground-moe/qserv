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
#ifndef LSST_QSERV_REPLICA_FINDREQUEST_H
#define LSST_QSERV_REPLICA_FINDREQUEST_H

// System headers
#include <functional>
#include <memory>
#include <string>

// Qserv headers
#include "replica/Common.h"
#include "replica/protocol.pb.h"
#include "replica/ReplicaInfo.h"
#include "replica/RequestMessenger.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace replica {
    class Messenger;
}}}  // Forward declarations

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
  * Class FindRequest represents a transient state of the replica lookup
  * requests within the master controller for deleting replicas.
  */
class FindRequest : public RequestMessenger  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<FindRequest> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    // Default construction and copy semantics are prohibited

    FindRequest() = delete;
    FindRequest(FindRequest const&) = delete;
    FindRequest& operator=(FindRequest const&) = delete;

    ~FindRequest() final = default;

    // Trivial get methods

    std::string const& database() const        { return _database; }
    unsigned int       chunk() const           { return _chunk; }
    bool               computeCheckSum() const { return _computeCheckSum; }

    /// @return target request specific parameters
    FindRequestParams const& targetRequestParams() const { return _targetRequestParams; }

    /**
     * @return
     *   a reference to a result obtained from a remote service.
     *
     * @note
     *   This operation will return a sensible result only if the operation
     *   finishes with status FINISHED::SUCCESS
     */
    ReplicaInfo const& responseData() const;

    /**
     * Create a new request with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider 
     *   a host of services for various communications
     *
     * @param worker
     *   the identifier of a worker node (the one where the chunk is
     *   expected to be located) at a destination of the chunk
     *
     * @param database
     *   the name of a database
     *
     * @param chunk
     *   the number of a chunk to find (implies all relevant tables)
     *
     * @param computeCheckSum
     *   tell a worker server to compute check/control sum on each file
     *
     * @param onFinish
     *   an optional callback function to be called upon a completion of
     *   the request.
     *
     * @param priority
     *   a priority level of the request
     *
     * @param keepTracking
     *   keep tracking the request before it finishes or fails
     *
     * @param messenger
     *   an interface for communicating with workers
     *
     * @return
     *   pointer to the created object
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      boost::asio::io_service& io_service,
                      std::string const& worker,
                      std::string const& database,
                      unsigned int chunk,
                      bool computeCheckSum,
                      CallbackType const& onFinish,
                      int priority,
                      bool keepTracking,
                      std::shared_ptr<Messenger> const& messenger);

    /// @see Request::extendedPersistentState()
    std::list<std::pair<std::string,std::string>> extendedPersistentState() const final;

    /// @see Request::startImpl()
    void startImpl(util::Lock const& lock) final;

protected:

    /// @see Request::notify()
    void notify(util::Lock const& lock) final;

    /// @see Request::savePersistentState()
    void savePersistentState(util::Lock const& lock) final;

private:

    FindRequest(ServiceProvider::Ptr const& serviceProvider,
                boost::asio::io_service& io_service,
                std::string const& worker,
                std::string const& database,
                unsigned int chunk,
                bool computeCheckSum,
                CallbackType const& onFinish,
                int priority,
                bool keepTracking,
                std::shared_ptr<Messenger> const& messenger);

    /**
     * Start the timer before attempting the previously failed
     * or successful (if a status check is needed) step.
     *
     * @param lock
     *   a lock on Request::_mtx must be acquired before calling this method
     */
    void _wait(util::Lock const& lock);

    /**
     * Callback handler for the asynchronous operation
     *
     * @param ec
     *   error code to be checked
     */
    void _awaken(boost::system::error_code const& ec);

    /**
     * Send the serialized content of the buffer to a worker
     *
     * @param lock
     *   a lock on Request::_mtx must be acquired before calling this method
     */
    void _send(util::Lock const& lock);

    /**
     * Process the completion of the requested operation
     *
     * @param success
     *   'true' indicates a successful response from a worker
     *
     * @param message
     *   response from a worker (if success)
     */
    void _analyze(bool success,
                  ProtocolResponseFind const& message);


    // Input parameters

    std::string  const _database;
    unsigned int const _chunk;
    bool const         _computeCheckSum;
    CallbackType       _onFinish;       /// @note is reset when the request finishes

    /// Request-specific parameters of the target request
    FindRequestParams _targetRequestParams;

    /// The results reported by a worker service
    ReplicaInfo _replicaInfo;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_FINDREQUEST_H
