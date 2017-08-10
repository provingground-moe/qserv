#include <stdexcept>

#include <iostream>
#include <string>

#include "lsst/log/Log.h"
#include "proto/replication.pb.h"
#include "replica_core/BlockPost.h"
#include "replica_core/Configuration.h"
#include "replica_core/Controller.h"
#include "replica_core/ServiceManagementRequest.h"
#include "replica_core/ServiceProvider.h"

namespace rc = lsst::qserv::replica_core;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.replica_admin");

const char* usage = "Usage: <config> {SUSPEND | RESUME | STATUS}";

/**
 * Return the name of any known worker from the controller configuration.
 * Throw std::runtime_error if no worker found.
 */
std::string getAnyWorker (rc::ServiceProvider &provider) {
    for (const std::string &workerName : provider.workers()) {
        return workerName;
    }
    throw std::runtime_error ("getAnyWorker: no single worker found in the configuration");
}

/**
 * Print a status of the service
 */
void printRequest (const rc::ServiceManagementRequestBase::pointer &request) {

    const rc::ServiceManagementRequestBase::ServiceState& serviceState =
        request->getServiceState ();

    LOGS(_log, LOG_LVL_INFO, request->id() << "  ** DONE **"
        << "  service: " << serviceState.state2string()
        << "  new, in-progress, finished: "
        << serviceState.numNewRequests << ", "
        << serviceState.numInProgressRequests << ", "
        << serviceState.numFinishedRequests);
}

/**
 * Run the test
 */
void test (const std::string &configFileName,
           const std::string &operation) {

    try {

        rc::Configuration   config  {configFileName};
        rc::ServiceProvider provider{config};

        rc::Controller::pointer controller = rc::Controller::create(provider);

        // Configure the generator of requests 

        const std::string worker = getAnyWorker(provider);

        // Start the controller in its own thread before injecting any requests

        controller->run();

        rc::ServiceManagementRequestBase::pointer request;

        if      ("SUSPEND" == operation) request = controller->suspendWorkerService (
                worker,
                [] (rc::ServiceSuspendRequest::pointer request) {
                    printRequest (request);
                });

        else if ("RESUME"  == operation) request = controller->resumeWorkerService (
                worker,
                [] (rc::ServiceResumeRequest::pointer request) {
                    printRequest (request);
                });

        else if ("STATUS"  == operation) request = controller->statusOfWorkerService (
                worker,
                [] (rc::ServiceStatusRequest::pointer request) {
                    printRequest (request);
                });
        else {
            std::cerr << usage << std::endl;
            return;
        }

        // Wait before the request is finished. Then stop the master controller

        rc::BlockPost blockPost (0, 5000);  // for random delays (milliseconds) between iterations

        while (request->state() != rc::Request::State::FINISHED) {
            blockPost.wait();
        }
        controller->stop();

        // Block the current thread indefinitively or untill the controller is cancelled.

        LOGS(_log, LOG_LVL_INFO, "waiting for: controller->join()");
        controller->join();

    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
    }
}
}

int main (int argc, const char* const argv[]) {

    // Verify that the version of the library that we linked against is
    // compatible with the version of the headers we compiled against.

    GOOGLE_PROTOBUF_VERIFY_VERSION;

    if (argc != 3) {
        std::cerr << ::usage << std::endl;
        return 1;
    }
    const std::string configFileName (argv[1]),
                      operation      (argv[2]);
    
    ::test (configFileName, operation);

    return 0;
}