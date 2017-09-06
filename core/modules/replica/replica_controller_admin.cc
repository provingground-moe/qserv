#include <atomic>
#include <iomanip>
#include <iostream>
#include <vector>
#include <stdexcept>
#include <string>

#include "proto/replication.pb.h"
#include "replica_core/BlockPost.h"
#include "replica_core/Configuration.h"
#include "replica_core/Controller.h"
#include "replica_core/ServiceManagementRequest.h"
#include "replica_core/ServiceProvider.h"

namespace rc = lsst::qserv::replica_core;

namespace {

const char* usage =
    "Usage:\n"
    "  <config> { STATUS | SUSPEND | RESUME }\n";


// Command line parameters

std::string configFileName;
std::string operation;

/// Return 'true' if the specified value is found in the collection
bool found_in (const std::string &val,
               const std::vector<std::string> &col) {
    return col.end() != std::find(col.begin(), col.end(), val);
}

/// Run the test
bool test () {

    try {

        rc::Configuration   config  {configFileName};
        rc::ServiceProvider provider{config};

        rc::Controller::pointer controller = rc::Controller::create(provider);

        // Start the controller in its own thread before injecting any requests
        controller->run();

        
        // Get the names of all workers and databases from the configuration,
        // and ask each worker which replicas it has.

        const auto workerNames = config.workers();

        // Registry of all requests       
        std::vector<rc::ServiceManagementRequestBase::pointer> requests;

        // The counter of requests which will be updated
        std::atomic<size_t> numSuccess(0);
        std::atomic<size_t> numFailure(0);
        std::atomic<size_t> numTotal  (0);

        // Launch requests against all workers
        //
        // ATTENTION: calbacks on the request completion callbacks of the requests will
        //            be executed within the Contoller's thread. Watch for proper
        //            synchronization when inspecting/updating shared variables.

        std::cout
            << "\n"
            << "WORKERS:";
        for (const auto &worker: workerNames) {
            std::cout << " " << worker;
        }
        std::cout
            << "\n"
            << std::endl;

        for (const auto &worker: workerNames) {
            numTotal++;

            if (operation == "STATUS")
                requests.push_back (
                    controller->statusOfWorkerService (
                        worker,
                        [&numSuccess, &numFailure] (rc::ServiceStatusRequest::pointer request) {
                            if (request->extendedState() == rc::Request::ExtendedState::SUCCESS)
                                numSuccess++;
                            else
                                numFailure++;
                        }));
            else if (operation == "SUSPEND")
                requests.push_back (
                    controller->suspendWorkerService (
                        worker,
                        [&numSuccess, &numFailure] (rc::ServiceSuspendRequest::pointer request) {
                            if (request->extendedState() == rc::Request::ExtendedState::SUCCESS)
                                numSuccess++;
                            else
                                numFailure++;
                        }));
            else if (operation == "RESUME")
                requests.push_back (
                    controller->resumeWorkerService (
                        worker,
                        [&numSuccess, &numFailure] (rc::ServiceResumeRequest::pointer request) {
                            if (request->extendedState() == rc::Request::ExtendedState::SUCCESS)
                                numSuccess++;
                            else
                                numFailure++;
                        }));
        }

        // Wait before all request are finished

        rc::BlockPost blockPost (100, 200);
        while (numSuccess + numFailure < numTotal) {
            std::cout << "success / failure / total: "
                << numSuccess << " / "
                << numFailure << " / "
                << numTotal   << std::endl;
            blockPost.wait();
        }
        std::cout << "success / failure / total: "
            << numSuccess << " / "
            << numFailure << " / "
            << numTotal   << std::endl;

        // Analyse and display results

        std::cout
            << "----------+-----------------------+---------------------+-------------+-------------+-------------\n"
            << "   worker | started (seconds ago) | state               |         new | in-progress |    finished \n"
            << "----------+-----------------------+---------------------+-------------+-------------+-------------\n";

        for (const auto &ptr: requests) {

            if ((ptr->state()         == rc::Request::State::FINISHED) &&
                (ptr->extendedState() == rc::Request::ExtendedState::SUCCESS)) {

                const uint32_t startedSecondsAgo = (rc::PerformanceUtils::now() - ptr->getServiceState().startTime) / 1000.0f;
                std::cout
                    << " "   << std::setw (8) << ptr->worker()
                    << " | " << std::setw(21) << startedSecondsAgo
                    << " | " << std::setw(19) << ptr->getServiceState().state2string()
                    << " | " << std::setw(11) << ptr->getServiceState().numNewRequests
                    << " | " << std::setw(11) << ptr->getServiceState().numInProgressRequests
                    << " | " << std::setw(11) << ptr->getServiceState().numFinishedRequests
                    << "\n";
            } else {
                std::cout
                    << " "   << std::setw (8) << ptr->worker()
                    << " | " << std::setw(21) << "*"
                    << " | " << std::setw(19) << "*"
                    << " | " << std::setw(11) << "*"
                    << " | " << std::setw(11) << "*"
                    << " | " << std::setw(11) << "*"
                    << "\n";
            }
        }
        std::cout
            << "----------+-----------------------+---------------------+-------------+-------------+-------------\n"
            << std::endl;
            
        // Shutdown the controller and join with its thread

        controller->stop();
        controller->join();

    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
    }
    return true;
}
} /// namespace

int main (int argc, const char* const argv[]) {

    // Verify that the version of the library that we linked against is
    // compatible with the version of the headers we compiled against.

    GOOGLE_PROTOBUF_VERIFY_VERSION;

    if (argc < 3) {
        std::cerr << ::usage << std::endl;
        return 1;
    }
    ::configFileName = argv[1];
    ::operation      = argv[2];
    if (!::found_in(::operation, {"STATUS","SUSPEND","RESUME"})) {
        std::cerr << "illegal operation: " << ::operation << "\n"
            << ::usage << std::endl;
        return 1;
    }
 
    ::test();
    return 0;
}