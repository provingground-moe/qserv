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
#ifndef LSST_QSERV_REPLICA_WORKERECHOREQUEST_H
#define LSST_QSERV_REPLICA_WORKERECHOREQUEST_H

// System headers
#include <string>

// Qserv headers
#include "replica/protocol.pb.h"
#include "replica/WorkerRequest.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
  * Class WorkerEchoRequest implements test requests within the worker servers.
  * Requests of this type don't have any side effects (in terms of modifying
  * any files or databases).
  */
class WorkerEchoRequest : public WorkerRequest {

public:

    /// Pointer to self
    typedef std::shared_ptr<WorkerEchoRequest> Ptr;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider
     *   provider is needed to access the Configuration of a setup
     *   and for validating the input parameters
     *
     * @param worker
     *   the name of a worker. The name must match the worker which
     *   is going to execute the request.
     * 
     * @param id
     *   an identifier of a client request
     *
     * @param priority
     *   indicates the importance of the request
     *
     * @param data
     *   the data string to be echoed back
     *
     * @param delay
     *   the desired minimum execution time (milliseconds) of a request
     *
     * @return
     *   pointer to the created object
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      std::string const& worker,
                      std::string const& id,
                      int priority,
                      std::string const& data,
                      uint64_t delay);

    // Default construction and copy semantics are prohibited

    WorkerEchoRequest() = delete;
    WorkerEchoRequest(WorkerEchoRequest const&) = delete;
    WorkerEchoRequest& operator=(WorkerEchoRequest const&) = delete;

    ~WorkerEchoRequest() override = default;

    // Trivial get methods

    std::string const& data() const { return _data; }

    uint64_t delay() const { return _delay; }
    
    /**
     * Extract request status into the Protobuf response object.
     *
     * @param response
     *   Protobuf response to be initialized
     */
    void setInfo(ProtocolResponseEcho& response) const;

    /// @see WorkerRequest::execute
    bool execute() override;

protected:

    /// @see WorkerEchoRequest::create()
    WorkerEchoRequest(ServiceProvider::Ptr const& serviceProvider,
                      std::string const& worker,
                      std::string const& id,
                      int priority,
                      std::string const& data,
                      uint64_t delay);

    // Input parameters

    std::string const _data;
    uint64_t    const _delay;

    /// The amount of the initial delay which is still left
    uint64_t _delayLeft;
};

/// Class WorkerEchoRequest provides an actual implementation
typedef WorkerEchoRequest WorkerEchoRequestFS;

/// Class WorkerEchoRequest provides an actual implementation
typedef WorkerEchoRequest WorkerEchoRequestPOSIX;

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_WORKERECHOREQUEST_H
