#include <cstdlib>
#include <iostream>
#include <map>
#include <string>

#include "lsst/log/Log.h"
#include "replica_core/BlockPost.h"
#include "replica_core/Configuration.h"
#include "replica_core/DeleteRequest.h"
#include "replica_core/FindRequest.h"
#include "replica_core/FindAllRequest.h"
#include "replica_core/MasterServer.h"
#include "replica_core/ReplicationRequest.h"
#include "replica_core/ServiceProvider.h"
#include "replica_core/StatusRequest.h"
#include "replica_core/StopRequest.h"

namespace rc = lsst::qserv::replica_core;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.replica_master_one");

const char* usage =
    "Usage:\n"
    "  <config> <operation> [<parameters>]\n"
    "\n"
    "Supported operations:\n"
    "  REPLICATE            <db> <chunk>\n"
    "  REPLICATE_AND_CANCEL <db> <chunk>\n"
    "  DELETE_REPLICA       <db> <chunk>\n"
    "  FIND_REPLICA         <db> <chunk>\n"
    "  FIND_ALL_REPLICAS    <db>\n"
    "  REPLICATION_STATUS   <id>\n"
    "  STOP_REPLICATION     <id>";

bool assertArguments (int argc, int minArgc) {
    if (argc < minArgc) {
        std::cerr << usage << std::endl;
        std::exit(1);
    }
    return true;
}

/**
 * Return the name of any known worker from the server configuration.
 * Throw std::runtime_error if no worker found.
 */
std::string getAnyWorker (rc::ServiceProvider &provider) {
    for (const std::string &workerName : provider.workers()) {
        return workerName;
    }
    throw std::runtime_error ("replica_master: no single worker found in the configuration");
}

template <class T>
void printRequest (typename T::pointer &request) {
    LOGS(_log, LOG_LVL_INFO, request->id() << "  DONE");
}

/**
 * Run the test
 */
bool test (const std::string &configFileName,
           const std::string &operation,
           const std::string &id_or_db,
           uint32_t           chunk = 0) {

    try {

        rc::Configuration   config  {configFileName};
        rc::ServiceProvider provider{config};

        rc::MasterServer::pointer server = rc::MasterServer::create(provider);

        // Configure the generator of requests 

        const std::string worker = getAnyWorker(provider);

        // Start the server in its own thread before injecting any requests

        server->run();

        rc::Request::pointer request;

        if ("REPLICATE" == operation) {
            request = server->replicate (
                id_or_db, chunk, worker, worker,
                [] (rc::ReplicationRequest::pointer request) {
                    printRequest<rc::ReplicationRequest>(request);
                });

        } else if ("REPLICATE_AND_CANCEL" == operation) {
            request = server->replicate (
                id_or_db, chunk, worker, worker,
                [] (rc::ReplicationRequest::pointer request) {
                    printRequest<rc::ReplicationRequest>(request);
                });

            rc::BlockPost blockPost (0, 500);
            blockPost.wait();

            request->cancel();

        } else if ("DELETE_REPLICA" == operation) {
            request = server->deleteReplica (
                id_or_db, chunk, worker,
                [] (rc::DeleteRequest::pointer request) {
                    printRequest<rc::DeleteRequest>(request);
                });

        } else if ("FIND_REPLICA" == operation) {
            request = server->findReplica (
                id_or_db, chunk, worker,
                [] (rc::FindRequest::pointer request) {
                    printRequest<rc::FindRequest>(request);
                });

        } else if ("FIND_ALL_REPLICAS" == operation) {
            request = server->findAllReplicas (
                id_or_db, worker,
                [] (rc::FindAllRequest::pointer request) {
                    printRequest<rc::FindAllRequest>(request);
                });

        } else if ("REPLICATION_STATUS"  == operation) {
            request = server->statusOfReplication (
                worker, id_or_db,
                [] (rc::StatusReplicationRequest::pointer request) {
                    printRequest<rc::StatusReplicationRequest>(request);
                });

        } else if ("STOP_REPLICATION"  == operation) {
            request = server->stopReplication (
                worker, id_or_db,
                [] (rc::StopReplicationRequest::pointer request) {
                    printRequest<rc::StopReplicationRequest>(request);
                });

        } else {
            return false;
        }


        // Wait before the request is finished. Then stop the master server

        rc::BlockPost blockPost (0, 5000);  // for random delays (milliseconds) between iterations

        while (request->state() != rc::Request::State::FINISHED) {
            blockPost.wait();
        }
        server->stop();

        // Block the current thread indefinitively or untill the server is cancelled.

        LOGS(_log, LOG_LVL_INFO, "waiting for: server->join()");
        server->join();

    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
    }
    return true;
}
} /// namespace

int main (int argc, const char* const argv[]) {

    ::assertArguments (argc, 4);

    const std::string configFileName (argv[1]);
    const std::string operation      (argv[2]);
    const std::string id_or_db       (argv[3]);
    
    if      ("REPLICATE" == operation)
        ::assertArguments (argc, 5) &&
        ::test (configFileName, operation, id_or_db, std::stoul(argv[4]));

    else if ("REPLICATE_AND_CANCEL" == operation)
        ::assertArguments (argc, 5) &&
        ::test (configFileName, operation, id_or_db, std::stoul(argv[4]));

    else if ("DELETE_REPLICA" == operation)
        ::assertArguments (argc, 5) &&
        ::test (configFileName, operation, id_or_db, std::stoul(argv[4]));

    else if ("FIND_REPLICA" == operation)
        ::assertArguments (argc, 5) &&
        ::test (configFileName, operation, id_or_db, std::stoul(argv[4]));

    else if ("FIND_ALL_REPLICAS" == operation)
        ::assertArguments (argc, 4) &&
        ::test (configFileName, operation, id_or_db);

    else if ("REPLICATION_STATUS" == operation)
        ::test (configFileName, operation, id_or_db);

    else if ("STOP_REPLICATION" == operation)
        ::test (configFileName, operation, id_or_db);

    else {
        std::cerr << ::usage << std::endl;
        return 1;
    }
    return 0;
}