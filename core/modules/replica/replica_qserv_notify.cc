// System header
#include <iostream>
#include <stdexcept>
#include <string>

// Third party headers
#include "XrdSsi/XrdSsiProvider.hh"
#include "XrdSsi/XrdSsiService.hh"

// Qserv headers
#include "proto/worker.pb.h"
#include "replica/CmdParser.h"
#include "replica_core/BlockPost.h"
#include "replica_core/ReloadChunkListQservRequest.h"
#include "replica_core/TestEchoQservRequest.h"

/// This C++ symbol is provided by the SSI shared library
extern XrdSsiProvider* XrdSsiProviderClient;

namespace r  = lsst::qserv::replica;
namespace rc = lsst::qserv::replica_core;

namespace {

// Command line parameters

std::string serviceProviderLocation;
std::string workerResourceName;

int test () {

    XrdSsiErrInfo errInfo;
    auto serviceProvider =
        XrdSsiProviderClient->GetService(errInfo,
                                         serviceProviderLocation);

    if (!serviceProvider) {
        std::cerr
            << "failed to contact service provider at: " << serviceProviderLocation
            << ", error: " << errInfo.Get() << std::endl;
        return 1;
    }
    std::cout << "connected to service provider at: " << serviceProviderLocation << std::endl;

    XrdSsiResource resource (workerResourceName);

    rc::BlockPost blockPost (1000, 2000);
    while (true) {

        auto request1 =
            new rc::ReloadChunkListQservRequest (
                    [] (bool success,
                        rc::ReloadChunkListQservRequest::ChunkCollection const& added,
                        rc::ReloadChunkListQservRequest::ChunkCollection const& removed) {
                        std::cout << "# chunks added:   " << added.size()   << std::endl;
                        std::cout << "# chuks  removed: " << removed.size() << std::endl;
                    });
        serviceProvider->ProcessRequest(*request1, resource);

        auto request2 =
            new rc::TestEchoQservRequest (
                    "12345678",
                    [] (bool success,
                        std::string const& sent,
                        std::string const& received) {
                        std::cout << "# value sent:     " << sent     << std::endl;
                        std::cout << "# value received: " << received << std::endl;
                    });
        serviceProvider->ProcessRequest(*request2, resource);

        blockPost.wait(500);

        break;
    }

    return 0;
}
} // namespace

int main (int argc, const char* const argv[]) {

    // Verify that the version of the library that we linked against is
    // compatible with the version of the headers we compiled against.

    GOOGLE_PROTOBUF_VERIFY_VERSION;

    // Parse command line parameters
    try {
        r::CmdParser parser (
            argc,
            argv,
            "\n"
            "Usage:\n"
            "  <provider> <resource>\n"
            "\n"
            "Parameters:\n"
            "  <provider>  - location of a service provider     (example: 'localhost:1094')\n"
            "  <resource>  - path to a worker-specific resource (example: '/worker/worker-id-1')\n");

        ::serviceProviderLocation = parser.parameter<std::string> (1);
        ::workerResourceName      = parser.parameter<std::string> (2);

    } catch (std::exception const& ex) {
        return 1;
    } 
    return ::test ();
}
