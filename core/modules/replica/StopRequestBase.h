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
#ifndef LSST_QSERV_REPLICA_STOPREQUESTBASE_H
#define LSST_QSERV_REPLICA_STOPREQUESTBASE_H

// System headers

#include <memory>
#include <string>

// Qserv headers
#include "replica/Common.h"
#include "replica/Messenger.h"
#include "replica/protocol.pb.h"
#include "replica/RequestMessenger.h"
#include "replica/ServiceProvider.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
  * Class StopRequestBase represents the base class for a family of requests
  * stopping an on-going operation.
  */
class StopRequestBase : public RequestMessenger {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<StopRequestBase> Ptr;

    // Default construction and copy semantics are prohibited

    StopRequestBase() = delete;
    StopRequestBase(StopRequestBase const&) = delete;
    StopRequestBase& operator=(StopRequestBase const&) = delete;

    ~StopRequestBase() override = default;

    /// @return an identifier of the target request
    std::string const& targetRequestId() const { return _targetRequestId; }

    /// @return the performance info of the target operation (if available)
    Performance const& targetPerformance() const { return _targetPerformance; }

    /// @see Request::extendedPersistentState()
    std::list<std::pair<std::string,std::string>> extendedPersistentState() const override;

protected:

    /**
     * Construct the request
     *
     * @param serviceProvider
     *   a host of services for accessing Configuration, saving request's
     *   state in the database, etc.
     * 
     * @param io_service
     *   network communication service
     * 
     * @param requestName
     *   the name of a request (used in reporting messages to the log stream,
     *   and when saving its state in the database)
     * 
     * @param worker
     *   the name of a worker node (the one to be affected by the request)
     * 
     * @param targetRequestId
     *   an identifier of the target request whose remote status
     *   is going to be inspected
     *
     * @param targetRequestType
     *   the sub-type of the replication request (if applies for the general
     *   type above)
     *
     * @param priority
     *   priority level of the request
     *
     * @param keepTracking
     *   keep tracking the request before it finishes or fails
     *
     * @param messenger
     *   an interface for communicating with workers
     */
    StopRequestBase(ServiceProvider::Ptr const& serviceProvider,
                    boost::asio::io_service& io_service,
                    char const* requestName,
                    std::string const& worker,
                    std::string const& targetRequestId,
                    ProtocolQueuedRequestType targetRequestType,
                    int priority,
                    bool keepTracking,
                    std::shared_ptr<Messenger> const& messenger);

    /// @see Request::startImpl()
    void startImpl(util::Lock const& lock) final;

    /**
     * Initiate request-specific send. This method must be implemented
     * by subclasses.
     *
     * @param lock
     *   a lock on Request::_mtx must be acquired before calling this method
     */
    virtual void send(util::Lock const& lock) = 0;

    /**
     * Process the worker response to the requested operation.
     *
     * @param success
     *   'true' indicates a successful response from a worker
     *
     * @param status
     *   a response from the worker service (only valid if success is 'true')
     */
    void analyze(bool success,
                 ProtocolStatus status=ProtocolStatus::FAILED);

    /**
     * Initiate request-specific operation with the persistent state
     * service to store replica status.
     *
     * This method must be implemented by subclasses.
     */
    virtual void saveReplicaInfo() = 0;

    /// @see Request::savePersistentState()
    void savePersistentState(util::Lock const& lock) final;

    /// The performance of the target operation (this object is updated by subclasses)
    Performance _targetPerformance;

private:

    /**
     * Serialize request data into a network buffer and send the message to a worker
     *
     * @param lock
     *   a lock on Request::_mtx must be acquired before calling this method
     */
    void _sendImpl(util::Lock const& lock);

    /**
     * Start the timer before attempting the previously failed
     * or successful (if a status check is needed) step.
     *
     * @param lock
     *   a lock on Request::_mtx must be acquired before calling this method
     */
    void _wait(util::Lock const& lock);

    /// Callback handler for the asynchronous operation
    void _awaken(boost::system::error_code const& ec);

    // Input parameters

    std::string const _targetRequestId;

    ProtocolQueuedRequestType const _targetRequestType;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_STOPREQUESTBASE_H
