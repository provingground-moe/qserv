
#include <iostream>
#include <string>

#include "lsst/log/Log.h"
#include "proto/replication.pb.h"
#include "replica_core/BlockPost.h"
#include "replica_core/Configuration.h"
#include "replica_core/FileServer.h"
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
void service (const std::string &configFileName,
              const std::string &workerName) {
    
    try {
        rc::Configuration        config        {configFileName};
        rc::ServiceProvider      provider      {config};
        rc::WorkerRequestFactory requestFactory{provider};

        rc::WorkerServer::pointer reqProcSvr =
            rc::WorkerServer::create (provider,
                                      requestFactory,
                                      workerName);
        std::thread reqProcSvrThread ([reqProcSvr]() {
            reqProcSvr->run();
        });

        rc::FileServer::pointer fileSvr =
            rc::FileServer::create (provider,
                                    workerName);
        std::thread fileSvrThread ([fileSvr]() {
            fileSvr->run();
        });
        rc::BlockPost blockPost (1000, 5000);
        while (true) {
            blockPost.wait();
            LOGS(_log, LOG_LVL_INFO, "HEARTBEAT"
                << "  worker: " << reqProcSvr->worker()
                << "  processor: " << rc::WorkerProcessor::state2string(reqProcSvr->processor().state())
                << "  new, in-progress, finished: "
                << reqProcSvr->processor().numNewRequests() << ", "
                << reqProcSvr->processor().numInProgressRequests() << ", "
                << reqProcSvr->processor().numFinishedRequests());
        }
        reqProcSvrThread.join();
        fileSvrThread.join();

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
        std::cerr << "usage: <config> <worker>" << std::endl;
        return 1;
    }
    const std::string configFileName (argv[1]);
    const std::string workerName     (argv[2]);

    ::service(configFileName, workerName);

    return 0;
}
