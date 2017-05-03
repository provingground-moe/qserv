#include <fstream>
#include <iostream>
#include <arpa/inet.h>

#include "proto/replication.pb.h"

namespace proto = lsst::qserv::proto;

namespace {

    template <typename T>
    bool read_message (std::istream &inFile,
                       T &message) {

        // Read the length of the message (which will be read next)
        //
        uint32_t netBytes;
        inFile.read(reinterpret_cast<char*>(&netBytes), sizeof netBytes);
        const uint32_t hostBytes = ntohl(netBytes);

        // Read and deserialize the message itself
        //
        char* data = new char[hostBytes];
        inFile.read(data, hostBytes);

        bool status = true;
        if (!message.ParseFromArray(data, hostBytes)) {
            std::cerr << "Failed to read the message." << std::endl;
            status = false;
        }
        delete [] data;

        return status;
    }

    bool read_message_stream (const std::string &filename) {

        std::fstream inFile(filename, std::ios::in |
                                      std::ios::binary);
        if (!inFile) {
            std::cerr << "Failed to open the input file." << std::endl;
            return false;                
        }
        for (int i = 0; i < 3; ++i) {

            proto::ReplicationRequestHeader hdr;
            if (!read_message(inFile, hdr)) return false;

            switch (hdr.type()) {

                case proto::ReplicationRequestHeader::REPLICATE:
                    {
                        std::cout << "request <REPLICATE> : " << std::endl;

                        proto::ReplicationRequestReplicate request;
                        if (!read_message(inFile, request)) return false;
    
                        std::cout << "  database : " << request.database() << "\n"
                                  << "  chunk    : " << request.chunk()    << "\n"
                                  << "  id       : " << request.id()       << std::endl;

                        proto::ReplicationResponseReplicate response;
                        if (!read_message(inFile, response)) return false;
    
                        std::cout << "response : " << "\n"
                                  << "  status : " << response.status() << "\n"
                                  << std::endl;
                    }
                    break;

                case proto::ReplicationRequestHeader::STOP:
                    {
                        std::cout << "request <STOP> : " << std::endl;

                        proto::ReplicationRequestStop request;
                        if (!read_message(inFile, request)) return false;
    
                        std::cout << "    id   : " << request.id() << std::endl;

                        proto::ReplicationResponseReplicate response;
                        if (!read_message(inFile, response)) return false;
    
                        std::cout << "response : " << "\n"
                                  << "  status : " << response.status() << "\n"
                                  << std::endl;
                    }
                    break;

                case proto::ReplicationRequestHeader::STATUS:
                    {
                        std::cout << "request <STATUS> : " << std::endl;

                        proto::ReplicationRequestStatus request;
                        if (!read_message(inFile, request)) return false;

                        std::cout << "    id   : " << request.id() << std::endl;

                        proto::ReplicationResponseReplicate response;
                        if (!read_message(inFile, response)) return false;
    
                        std::cout << "response : " << "\n"
                                  << "  status : " << response.status() << "\n"
                                  << std::endl;
                    }
                    break;

                default:
                    std::cerr << "Unknown header type: " << hdr.type() << std::endl;
                    return false;
            }
        }
        return true;
    }
}

int main (int argc, const char* const argv[]) {

    // Verify that the version of the library that we linked against is
    // compatible with the version of the headers we compiled against.

    GOOGLE_PROTOBUF_VERIFY_VERSION;

    if (argc != 2) {
        std::cout << "usage: <filename>" << std::endl;
        return 1;
    }
    const bool status = ::read_message_stream(argv[1]);

    // It's always a good practice to come clean when finishing working with
    // the library. This will release the data structures, deallocate memory
    // and avoid confusing the memory leak checkers.

    google::protobuf::ShutdownProtobufLibrary();

    return status ? 0 : 1;
}