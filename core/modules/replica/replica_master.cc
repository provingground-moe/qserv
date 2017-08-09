#include <stdexcept>

#include <iostream>
#include <list>
#include <string>
#include <thread>

#include "lsst/log/Log.h"
#include "replica_core/BlockPost.h"
#include "replica_core/Configuration.h"
#include "replica_core/MasterServer.h"
#include "replica_core/ReplicationRequest.h"
#include "replica_core/ServiceProvider.h"
#include "replica_core/StatusRequest.h"
#include "replica_core/StopRequest.h"

namespace rc = lsst::qserv::replica_core;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.replica_master");

/**
 * The helper class for generating various requests. The main purpose
 * of the class is to reduce code duplication in the tests.
 *
 * THREAD SAFETY: an implementation of this class is as thread safe
 * as any objec used by it.
 */
class RequestGenerator {

public:

    /// A collection of REPLICATION requsts
    typedef std::vector<rc::ReplicationRequest::pointer> replication_requests;

    /// A collection of STATUS requsts
    typedef std::vector<rc::StatusReplicationRequest::pointer> status_requests;

    /// A collection of STOP requsts
    typedef std::vector<rc::StopReplicationRequest::pointer> stop_requests;

    // Default construction and copy semantics are proxibited

    RequestGenerator () = delete;
    RequestGenerator (RequestGenerator const&) = delete;
    RequestGenerator & operator= (RequestGenerator const&) = delete;


    /**
     * The normal constructor.
     *
     * @param server            - the server API for initiating requests
     * @param database          - the database which is a subject of teh replication
     * @param sourceWorker      - the name of a worker node for chunks to be replicated
     * @param destinationWorker - the name of a worker node for new replicas
     */
    RequestGenerator (rc::MasterServer::pointer  server,
                      const std::string         &database,
                      const std::string         &sourceWorker,
                      const std::string         &destinationWorker) noexcept
        :   _server            (server),
            _database          (database),
            _sourceWorker      (sourceWorker),
            _destinationWorker (destinationWorker)
    {}

    /**
     * Initiate the specified numder of replication requests
     * and return a collection of pointers to them. The requests will
     * address a contigous range of chunk numbers staring with the one
     * specified as a parameter of the method.
     *
     * @param num        - the total number of requests to launch
     * @param firstChunk - the number of the first chunk in a series
     * @param blockPost  - an optional pointer to the BlockPost object
     *                     if a random delay before(!!!) generating each request
     *                     is required.
     *                     
     * @return - a collection of pointers to the requests.
     */
    replication_requests replicate (size_t         num,
                                    unsigned int   firstChunk,
                                    rc::BlockPost *blockPost=nullptr) {

        replication_requests requests;
        
        unsigned int chunk = firstChunk;

        for (size_t i = 0; i < num; ++i) {

            // Delay the request generation if needed.

            if (blockPost) blockPost->wait();

            rc::ReplicationRequest::pointer request =
                _server->replicate (
                    _database,
                    chunk++,
                    _sourceWorker,
                    _destinationWorker,
                    [] (rc::ReplicationRequest::pointer request) {
                        LOGS(_log, LOG_LVL_INFO, request->context() << "** DONE **"
                            << "  chunk: " << request->chunk());
                    }
                );
            requests.push_back(request);
        }
        return requests;
    }

    /**
     * Initiate status inqueries for the specified replication requests.
     *
     * @param replicationRequests - a collection of pointers to the REPLICATION requests
     * 
     * @return - a collection of pointers to the STATUS requests.
     */
    status_requests status (const replication_requests& replicationRequests) {

        status_requests requests;

        for (auto request : replicationRequests)
            requests.push_back (
                _server->statusOfReplication (
                    request->worker(),
                    request->id(),
                    [] (rc::StatusReplicationRequest::pointer request) {
                        LOGS(_log, LOG_LVL_INFO, request->context() << "** DONE **"
                            << "  targetRequestId: " << request->targetRequestId());
                    }
                )
            );

        return requests;
    }

    /**
     * Initiate stop commands for the specified replication requests.
     *
     * @param replicationRequests - a collection of pointers to the REPLICATION requests
     * 
     * @return - a collection of pointers to the STOP requests.
     */
    stop_requests stop (const replication_requests& replicationRequests) {

        stop_requests requests;

        for (auto request : replicationRequests)
            requests.push_back (
                _server->stopReplication (
                    request->worker(),
                    request->id(),
                    [] (rc::StopReplicationRequest::pointer request) {
                        LOGS(_log, LOG_LVL_INFO, request->context() << "** DONE **"
                            << "  targetRequestId: " << request->targetRequestId());
                    }
                )
            );

        return requests;
    }

private:    

    // Parameters of the object

    rc::MasterServer::pointer _server;

    std::string _database;        
    std::string _sourceWorker;
    std::string _destinationWorker;
};

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

/**
 * Print a status of the server
 */
void reportServerStatus (rc::MasterServer::pointer server) {
    LOGS(_log, LOG_LVL_INFO, "server is " << (server->isRunning() ? "" : "NOT ") << "running");
}

/**
 * Run the test
 */
void test (const std::string &configFileName) {

    const std::string database = "wise_00";

    try {

        rc::BlockPost blockPost (0, 5000);  // for random delays (milliseconds) between operations

        rc::Configuration   config  {configFileName};
        rc::ServiceProvider provider{config};

        rc::MasterServer::pointer server = rc::MasterServer::create(provider);

        // Configure the generator of requests 

        RequestGenerator requestGenerator (
            server,
            "wise_00",
            getAnyWorker(provider),
            getAnyWorker(provider));
        
        // Start the server in its own thread before injecting any requests

        reportServerStatus(server);
        server->run();
        reportServerStatus(server);

        // Create the first bunch of requests which are to be launched
        // right away.

        requestGenerator.replicate(10, 0);
      
        // Inject the second bunch of requests delayed one from another
        // by a random interval of time.

        requestGenerator.replicate(10, 10, &blockPost);
        
        // Do a proper clean up of the service by stopping it. This way of stopping
        // the service will guarantee that all outstanding opeations will finish
        // and not aborted.
        //
        // NOTE: Joining to the server's thread is not needed because
        //       this will always be done internally inside the stop method.
        //       The only reason for joining would be for having an option of
        //       integrating the server into a larger application.

        reportServerStatus(server);
        //server->stop();
        reportServerStatus(server);

        //server->run();
        reportServerStatus(server);

        //requestGenerator.replicate(1000, 100, &blockPost);

        // Launch another thread which will test injecting requests from there.
        // 
        // NOTE: The thread may (and will) finish when the specified number of
        // requests will be launched because the requests are execured in
        // a context of the server thread.
        std::thread another (
            [&requestGenerator, &blockPost] () {

                const size_t num = 1000;
                const unsigned int chunk = 100;

                requestGenerator.replicate(num, chunk, &blockPost);

            }
        );

        // Continue injecting requests on the periodic bases, one at a time for
        // each known worker.

        RequestGenerator::replication_requests requests =
            requestGenerator.replicate(10, 30, &blockPost);

        // Launch STATUS and STOP requests for each of the previously
        // generated REPLICATION request.

        LOGS(_log, LOG_LVL_INFO, "checking status of " <<  requests.size() << " requests");
        requestGenerator.status(requests);

        LOGS(_log, LOG_LVL_INFO, "stopping " <<  requests.size() << " requests");
        requestGenerator.stop  (requests);

        // Wait before the request launching thread finishes

        reportServerStatus(server);
        LOGS(_log, LOG_LVL_INFO, "waiting for: another.join()");
        another.join();
        
        // This should block the current thread indefinitively or
        // untill the server is cancelled.

        while (true) {
            blockPost.wait();
            LOGS(_log, LOG_LVL_INFO, "HEARTBEAT  active replications: " << server->numActiveReplicationRequests());
        }
        LOGS(_log, LOG_LVL_INFO, "waiting for: server->join()");
        server->join();
        LOGS(_log, LOG_LVL_INFO, "past: server->join()");

    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
    }
}
}

int main (int argc, const char* const argv[]) {

    if (argc != 2) {
        std::cerr << "Usage: <config>" << std::endl;
        return 1;
    }
    const std::string configFileName(argv[1]);
    
    ::test (configFileName);

    return 0;
}