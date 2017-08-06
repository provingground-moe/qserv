
#include <iostream>
#include <string>

#include "lsst/log/Log.h"

#include "replica_core/BlockPost.h"
#include "replica_core/Configuration.h"
#include "replica_core/ServiceProvider.h"
#include "replica_core/WorkerProcessor.h"
#include "replica_core/WorkerRequestFactory.h"
#include "replica_core/WorkerServer.h"

namespace rc = lsst::qserv::replica_core;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.replica_worker");

/**
 * Instantiate and launch the service in its own thread. Then block
 * the current thread in a series of repeated timeouts.
 */
void service (const std::string &configFileName) {
    
    try {
        rc::Configuration  ::pointer config    = rc::Configuration  ::create (configFileName);
        rc::ServiceProvider::pointer provider  = rc::ServiceProvider::create (config);
        rc::WorkerRequestFactory     requestFactory{provider};
        rc::WorkerProcessor::pointer processor = rc::WorkerProcessor::create (provider, requestFactory);
        rc::WorkerServer   ::pointer server    = rc::WorkerServer   ::create (provider, processor);

        std::thread requestsAcceptorThread (
            [&server]() { server->run(); }
        );
        rc::BlockPost blockPost (1000, 5000);
        while (true) {
            blockPost.wait();
            LOGS(_log, LOG_LVL_INFO, "HEARTBEAT"
                << "  processor: " << rc::WorkerProcessor::state2string(processor->state())
                << "  new, in-progress, finished: "
                << processor->numNewRequests() << ", "
                << processor->numInProgressRequests() << ", "
                << processor->numFinishedRequests());
        }
        requestsAcceptorThread.join();

    } catch (std::exception& e) {
        LOGS(_log, LOG_LVL_ERROR, e.what());
    }
}
}  /// namespace

int main (int argc, const char* const argv[]) {

    if (argc != 2) {
        std::cerr << "usage: <config>" << std::endl;
        return 1;
    }
    const std::string configFileName(argv[1]);

    ::service(configFileName);

    return 0;
}
