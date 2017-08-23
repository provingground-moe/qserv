#include <iostream>
#include <string>

#include "lsst/log/Log.h"
#include "proto/replication.pb.h"
#include "replica_core/BlockPost.h"
#include "replica_core/Configuration.h"
#include "replica_core/Controller.h"
#include "replica_core/ReplicationRequest.h"
#include "replica_core/ServiceProvider.h"
#include "replica_core/WorkerRequestFactory.h"
#include "replica_core/WorkerServer.h"

namespace rc = lsst::qserv::replica_core;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.replica_worker");


/**
 * Launch all worker servers in dedicated detached threads. Also run
 * one extra thread per each worked for the 'hearbeat' monitoring.
 */
void runAllWorkers (rc::ServiceProvider      &provider,
                    rc::WorkerRequestFactory &requestFactory) {

    for (const std::string &workerName : provider.config().workers()) {
        
        // Create the server and run it within a dedicated thread

        rc::WorkerServer::pointer server =
            rc::WorkerServer::create (provider, requestFactory, workerName);

        std::thread workerSvcThread ([server]() {
            server->run();
        });
        workerSvcThread.detach();
        
        // Run the heartbeat monitor for the server within another thread
 
        std::thread heartbeatThread ([server]() {
            rc::BlockPost blockPost(1000, 5000);
            while (true) {
                blockPost.wait();
                LOGS(_log, LOG_LVL_INFO, "<WORKER:" << server->worker() << " HEARTBEAT> "
                    << " processor state: " << rc::WorkerProcessor::state2string(server->processor().state())
                    << " new:"              << server->processor().numNewRequests()
                    << " in-progress: "     << server->processor().numInProgressRequests()
                    << " finished: "        << server->processor().numFinishedRequests());
            }
        });
        heartbeatThread.detach();
    }
}

/**
 * Find the next worker to the specified one in an iteration sequence
 * over all known worker names. Roll over to the very first one if
 * the specified work is the last in the sequence. Ensure there are
 * at least two worker in the configuration.
 */
std::string findSourceWorkerFor (rc::ServiceProvider &serviceProvider,
                                 const std::string   &worker) {

    bool thisWorkerFound = false;
    for (const std::string &name : serviceProvider.config().workers()) {
        if (name == worker) thisWorkerFound = true;
        else if (thisWorkerFound) return name;
    }

    // Roll over to the very first worker in the sequence. Also check
    // this is not the only worker in the configuration.
    for (const std::string &name : serviceProvider.config().workers()) {
        if (name == worker) break;
        return name;
    }
    return std::string();   // the only worker in the sequence has no other
                            // partners for replication
}

/**
 * Launch the specified number of chuk replication requsts by distributing
 * them (ussing the round-wobin algorithm) among all workers.
 */
void launchRequests (rc::ServiceProvider     &serviceProvider,
                     rc::Controller::pointer &controller,
                     const std::string       &database,
                     unsigned int             chunk,
                     unsigned int             maxChunk) {

    while (chunk < maxChunk) {
        for (const std::string &worker : serviceProvider.config().workers()) {    

            rc::ReplicationRequest::pointer request = controller->replicate (
                worker,
                findSourceWorkerFor(serviceProvider,
                                    worker),
                database,
                chunk++,
                [] (rc::ReplicationRequest::pointer request) {
                    LOGS(_log, LOG_LVL_INFO, request->context() << "** DONE **"
                        << "  worker: "       << request->worker()
                        << "  sourceWorker: " << request->sourceWorker()
                        << "  database: "     << request->database()
                        << "  chunk: "        << request->chunk()
                        << "  "               << request->performance());
                }
            );
            if (chunk >= maxChunk) break;
        }
    }
}


/**
 * Instantiate and run all threads. Then block the current thread in
 * a series of repeated timeouts.
 */
void run (const std::string &configFileName,
          unsigned int       maxChunk) {
    
    try {
        rc::Configuration        config         {configFileName};
        rc::ServiceProvider      serviceProvider{config};
        rc::WorkerRequestFactory requestFactory{serviceProvider};

        // Firts, run the worker servers

        runAllWorkers (serviceProvider, requestFactory);

        // Then run the client-side

        rc::Controller::pointer controller = rc::Controller::create(serviceProvider);
        controller->run();

        launchRequests (serviceProvider, controller, "db1", 1, maxChunk);

        // Block the thread forewer or until an exception happens

        rc::BlockPost blockPost(1000, 5000);
        while (true) {
            blockPost.wait();
            LOGS(_log, LOG_LVL_INFO, "<CONTROLLER HEARTBEAT>  active requests: " << controller->numActiveRequests());
        }
        controller->join();

    } catch (std::exception& e) {
        LOGS(_log, LOG_LVL_ERROR, e.what());
    }
}
}  /// namespace

int main (int argc, const char* const argv[]) {

    // Verify that the version of the library that we linked against is
    // compatible with the version of the headers we compiled against.

    GOOGLE_PROTOBUF_VERIFY_VERSION;
 
    if (argc != 3) {
        std::cerr << "usage: <config> <max-chunk>" << std::endl;
        return 1;
    }
    const std::string  configFileName =            argv[1];
    const unsigned int maxChunk       = std::stoul(argv[2]);

    ::run (configFileName, maxChunk);

    return 0;
}
