//#include <chrono>
#include <iostream>
//#include <ratio>        // std::milli
#include <string>
//#include <thread>

#include "replica_core/BlockPost.h"
#include "replica_core/Configuration.h"
#include "replica_core/ServiceProvider.h"
#include "replica_core/WorkerRequestProcessor.h"
#include "replica_core/WorkerServer.h"

namespace rc = lsst::qserv::replica_core;

namespace {

    /**
     * Instantiate and launch the service in its own thread. Then block
     * the current thread in a series of repeated timeouts.
     */
    void service (const std::string &configFileName) {
        
        try {
            rc::Configuration         ::pointer config    = rc::Configuration         ::create (configFileName);
            rc::ServiceProvider       ::pointer provider  = rc::ServiceProvider       ::create (config);
            rc::WorkerRequestProcessor::pointer processor = rc::WorkerRequestProcessor::create (provider);
            rc::WorkerServer          ::pointer server    = rc::WorkerServer          ::create (provider, processor);

            std::thread requestsAcceptorThread (
                [&server]() { server->run(); }
            );
            const int minIvalMilliSec = 1000,
                      maxIvalMilliSec = 5000;
            rc::BlockPost blockPost (minIvalMilliSec, maxIvalMilliSec);
            while (true) {
                std::cout << "heartbeat of " << blockPost.wait() << " milliseconds" << std::endl;
                // const std::chrono::duration<long, std::milli> interval = std::chrono::milliseconds(10000);
                // std::this_thread::sleep_for(interval);
                // std::cout << "heartbeat of " << interval.count() << " milliseconds" << std::endl;
            }
            requestsAcceptorThread.join();

        } catch (std::exception& e) {
            std::cerr << e.what() << std::endl;
        }
    }
}

int main (int argc, const char* const argv[]) {

    if (argc != 2) {
        std::cerr << "usage: <config>" << std::endl;
        return 1;
    }
    const std::string configFileName(argv[1]);

    ::service(configFileName);

    return 0;
}
