#include "proto/worker.pb.h"
#include "replica/CmdParser.h"
#include "replica_core/BlockPost.h"

#include "XrdSsi/XrdSsiProvider.hh"
#include "XrdSsi/XrdSsiRequest.hh"
#include "XrdSsi/XrdSsiService.hh"

#include <algorithm>
#include <memory>       // std::unique_ptr
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <string>

extern XrdSsiProvider* XrdSsiProviderClient;

namespace proto = lsst::qserv::proto;
namespace r     = lsst::qserv::replica;
namespace rc    = lsst::qserv::replica_core;

namespace {

// Command line parameters

std::string serviceProviderLocation;
std::string workerResourceName;

class QServWorkerRequest : public XrdSsiRequest {

public:

    QServWorkerRequest() {
        proto::WorkerCmdMsg msg;
        msg.set_cmd(proto::WorkerCmdMsg::RELOAD_CHUNK_LIST);
        msg.SerializeToString(&_msg);
    }

    ~QServWorkerRequest() override {}

    char* GetRequest (int& dlen) override { dlen = _msg.size(); return const_cast<char*>(_msg.data()); }

    bool ProcessResponse (const XrdSsiErrInfo&  eInfo,
                          const XrdSsiRespInfo& rInfo) override {
        if (eInfo.hasError()) {
            std::cerr << "QServWorkerRequest::ProcessResponse  ** FAILED **, error: " << rInfo.eMsg << std::endl;
            return false;
        }
        std::cout
            << "QServWorkerRequest::ProcessResponse"
            << "  eInfo.rType: " << rInfo.rType << "(" << rInfo.State() << ")"
            << ", eInfo.blen: " << rInfo.blen << std::endl;

        switch (rInfo.rType) {

            case XrdSsiRespInfo::isData:
            case XrdSsiRespInfo::isStream:

                std::cout
                    << "QServWorkerRequest::ProcessResponse  ** requesting response data **" << std::endl;

                GetResponseData(_buf, _bufSize);
                return true;
        
            default:
                return false;
        }
    }

    PRD_Xeq ProcessResponseData(const XrdSsiErrInfo& eInfo,
                                char*                buff,
                                int                  blen,
                                bool                 last) override {

        std::cerr
            << "QServWorkerRequest::(PRD_Xeq)ProcessResponseData"
            << "  eInfo.isOK(): " << (eInfo.isOK() ? "yes" : "no");

        if (!eInfo.isOK()) {
            std::cerr
                << ", eInfo.Get(): "    << eInfo.Get()
                << ", eInfo.GetArg(): " << eInfo.GetArg()
                << std::endl;
        } else {
            std::cerr
                << ", blen: " << blen
                << ", last: " << (last ? "yes" : "no")
                << std::endl;
            std::cout << buff << std::endl;
            //std::copy(buff, buff + blen, std::ostream_iterator<char>(std::cout));

            if (last) Finished();
            else      GetResponseData(_buf, _bufSize);  // keep receiving results
        }
        return PRD_Normal;
    }


private:

    /// Message to be sent
    std::string _msg;

    /// Length of the response buffer
    static int const _bufSize{1024};

    /// Buffer for response data
    char _buf[_bufSize];
};


int test () {

    //std::unique_ptr<char> recordPtr (new char[recordSizeBytes]);

    XrdSsiErrInfo  errInfo;
    XrdSsiService* serviceProvider =
        XrdSsiProviderClient->GetService(errInfo,
                                         serviceProviderLocation);

    if (!serviceProvider) {
        std::cerr
            << "failed to contact service provider at: " << serviceProviderLocation
            << ", error: " << errInfo.Get() << std::endl;
        return 1;
    }
    std::cout << "connected to service provider at: " << serviceProviderLocation << std::endl;

    XrdSsiRequest* request = new QServWorkerRequest();
    XrdSsiResource resource(workerResourceName);

    serviceProvider->ProcessRequest(*request, resource);

    rc::BlockPost blockPost (1000, 5000);
    while (true) {
        blockPost.wait();
        std::cout << "HEARTBEAT" << std::endl;
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
