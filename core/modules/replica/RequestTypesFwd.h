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
#ifndef LSST_QSERV_REPLICA_REQUESTTYPESFWD_H
#define LSST_QSERV_REPLICA_REQUESTTYPESFWD_H

/**
 * This header files provides  Forward declarations for smart pointers and
 * calback functions corresponding to specific requests.
 */

// System headers
#include <functional>
#include <memory>

// Qserv headers
#include "replica/Common.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

////////////////////////////////////////////
// Replica creation and deletion requests //
////////////////////////////////////////////

class ReplicationRequest;
class DeleteRequest;

typedef std::shared_ptr<ReplicationRequest> ReplicationRequestPtr;
typedef std::shared_ptr<DeleteRequest>      DeleteRequestPtr;

typedef std::function<void(ReplicationRequestPtr)> ReplicationRequestCallbackType;
typedef std::function<void(DeleteRequestPtr)>      DeleteRequestCallbackType;

/////////////////////////////
// Replica lookup requests //
/////////////////////////////

class FindRequest;
class FindAllRequest;

typedef std::shared_ptr<FindRequest>    FindRequestPtr;
typedef std::shared_ptr<FindAllRequest> FindAllRequestPtr;

typedef std::function<void(FindRequestPtr)>    FindRequestCallbackType;
typedef std::function<void(FindAllRequestPtr)> FindAllRequestCallbackType;

/////////////////////////////////////////////////////////
// Protocol and Replication framework testing requests //
////////////////////////////////////////////////////////

class EchoRequest;

typedef std::shared_ptr<EchoRequest> EchoRequestPtr;

typedef std::function<void(EchoRequestPtr)> EchoRequestCallbackType;

/////////////////////////////////////////////////
// Operations agaist worker databases requests //
/////////////////////////////////////////////////

class SqlRequest;

typedef std::shared_ptr<SqlRequest> SqlRequestPtr;

typedef std::function<void(SqlRequestPtr)> SqlRequestCallbackType;

////////////////////////////////////
// Replication request management //
////////////////////////////////////

class StopReplicationRequestPolicy;
class StopDeleteRequestPolicy;
class StopFindRequestPolicy;
class StopFindAllRequestPolicy;
class StopEchoRequestPolicy;
class StopSqlRequestPolicy;

template <typename POLICY> class StopRequest;

using StopReplicationRequest = StopRequest<StopReplicationRequestPolicy>;
using StopDeleteRequest      = StopRequest<StopDeleteRequestPolicy>;
using StopFindRequest        = StopRequest<StopFindRequestPolicy>;
using StopFindAllRequest     = StopRequest<StopFindAllRequestPolicy>;
using StopEchoRequest        = StopRequest<StopEchoRequestPolicy>;
using StopSqlRequest         = StopRequest<StopSqlRequestPolicy>;

typedef std::shared_ptr<StopReplicationRequest> StopReplicationRequestPtr;
typedef std::shared_ptr<StopDeleteRequest>      StopDeleteRequestPtr;
typedef std::shared_ptr<StopFindRequest>        StopFindRequestPtr;
typedef std::shared_ptr<StopFindAllRequest>     StopFindAllRequestPtr;
typedef std::shared_ptr<StopEchoRequest>        StopEchoRequestPtr;
typedef std::shared_ptr<StopSqlRequest>         StopSqlRequestPtr;

typedef std::function<void(StopReplicationRequestPtr)> StopReplicationRequestCallbackType;
typedef std::function<void(StopDeleteRequestPtr)>      StopDeleteRequestCallbackType;
typedef std::function<void(StopFindRequestPtr)>        StopFindRequestCallbackType;
typedef std::function<void(StopFindAllRequestPtr)>     StopFindAllRequestCallbackType;
typedef std::function<void(StopEchoRequestPtr)>        StopEchoRequestCallbackType;
typedef std::function<void(StopSqlRequestPtr)>         StopSqlRequestCallbackType;

////////////////////////////////////

class StatusReplicationRequestPolicy;
class StatusDeleteRequestPolicy;
class StatusFindRequestPolicy;
class StatusFindAllRequestPolicy;
class StatusEchoRequestPolicy;
class StatusSqlRequestPolicy;

template <typename POLICY> class StatusRequest;

using StatusReplicationRequest = StatusRequest<StatusReplicationRequestPolicy>;
using StatusDeleteRequest      = StatusRequest<StatusDeleteRequestPolicy>;
using StatusFindRequest        = StatusRequest<StatusFindRequestPolicy>;
using StatusFindAllRequest     = StatusRequest<StatusFindAllRequestPolicy>;
using StatusEchoRequest        = StatusRequest<StatusEchoRequestPolicy>;
using StatusSqlRequest         = StatusRequest<StatusSqlRequestPolicy>;

typedef std::shared_ptr<StatusReplicationRequest> StatusReplicationRequestPtr;
typedef std::shared_ptr<StatusDeleteRequest>      StatusDeleteRequestPtr;
typedef std::shared_ptr<StatusFindRequest>        StatusFindRequestPtr;
typedef std::shared_ptr<StatusFindAllRequest>     StatusFindAllRequestPtr;
typedef std::shared_ptr<StatusEchoRequest>        StatusEchoRequestPtr;
typedef std::shared_ptr<StatusSqlRequest>         StatusSqlRequestPtr;

typedef std::function<void(StatusReplicationRequestPtr)> StatusReplicationRequestCallbackType;
typedef std::function<void(StatusDeleteRequestPtr)>      StatusDeleteRequestCallbackType;
typedef std::function<void(StatusFindRequestPtr)>        StatusFindRequestCallbackType;
typedef std::function<void(StatusFindAllRequestPtr)>     StatusFindAllRequestCallbackType;
typedef std::function<void(StatusEchoRequestPtr)>        StatusEchoRequestCallbackType;
typedef std::function<void(StatusSqlRequestPtr)>         StatusSqlRequestCallbackType;


////////////////////////////////////////
// Worker service management requests //
////////////////////////////////////////

class ServiceSuspendRequestPolicy;
class ServiceResumeRequestPolicy;
class ServiceStatusRequestPolicy;
class ServiceRequestsRequestPolicy;
class ServiceDrainRequestPolicy;

template <typename POLICY> class ServiceManagementRequest;

using ServiceSuspendRequest  = ServiceManagementRequest<ServiceSuspendRequestPolicy>;
using ServiceResumeRequest   = ServiceManagementRequest<ServiceResumeRequestPolicy>;
using ServiceStatusRequest   = ServiceManagementRequest<ServiceStatusRequestPolicy>;
using ServiceRequestsRequest = ServiceManagementRequest<ServiceRequestsRequestPolicy>;
using ServiceDrainRequest    = ServiceManagementRequest<ServiceDrainRequestPolicy>;

typedef std::shared_ptr<ServiceSuspendRequest>  ServiceSuspendRequestPtr;
typedef std::shared_ptr<ServiceResumeRequest>   ServiceResumeRequestPtr;
typedef std::shared_ptr<ServiceStatusRequest>   ServiceStatusRequestPtr;
typedef std::shared_ptr<ServiceRequestsRequest> ServiceRequestsRequestPtr;
typedef std::shared_ptr<ServiceDrainRequest>    ServiceDrainRequestPtr;

typedef std::function<void(ServiceSuspendRequestPtr)>  ServiceSuspendRequestCallbackType;
typedef std::function<void(ServiceResumeRequestPtr)>   ServiceResumeRequestCallbackType;
typedef std::function<void(ServiceStatusRequestPtr)>   ServiceStatusRequestCallbackType;
typedef std::function<void(ServiceRequestsRequestPtr)> ServiceRequestsRequestCallbackType;
typedef std::function<void(ServiceDrainRequestPtr)>    ServiceDrainRequestCallbackType;

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_REQUESTTYPESFWD_H
