#include <arpa/inet.h>

#include <fstream>
#include <iostream>
#include <string>

#include "boost/uuid/uuid.hpp"
#include "boost/uuid/uuid_generators.hpp"
#include "boost/uuid/uuid_io.hpp"

#include "proto/replication.pb.h"

namespace proto = lsst::qserv::proto;

namespace {

    std::string generate_uuid () {
        boost::uuids::uuid id = boost::uuids::random_generator()();
        return boost::uuids::to_string(id);
    }

    template <typename T>
    bool write_message (std::ostream &outFile,
                        T& message) {

        const uint32_t dataLenHostBytes = message.ByteSize();
        const uint32_t dataLenNetBytes  = htonl(dataLenHostBytes);
        
        char *data = new char[dataLenHostBytes];
        message.SerializeToArray(data, dataLenHostBytes);

        outFile.write(reinterpret_cast<const char*>(&dataLenNetBytes), sizeof dataLenNetBytes);
        outFile.write(data, dataLenHostBytes);

        delete [] data;

        return true;
    }
    bool write_header (std::ostream &outFile,
                       const proto::ReplicationRequestHeader::Type type) {
        proto::ReplicationRequestHeader message;
        message.set_type(type);
        return write_message(outFile, message);
    }

    bool write_request_replicate (std::ostream &outFile) {
        proto::ReplicationRequestReplicate message;
        message.set_database("sdss_stripe82_00");
        message.set_chunk(123);
        message.set_id(generate_uuid());
        return write_message(outFile, message);
    }
    bool write_response_replicate (std::ostream &outFile) {
        proto::ReplicationResponseReplicate message;
        message.set_status(proto::ReplicationStatus::SUCCESS);
        return write_message(outFile, message);
    }

    bool write_request_stop (std::ostream &outFile) {
        proto::ReplicationRequestStop message;
        message.set_id(generate_uuid());
        return write_message(outFile, message);
    }
    bool write_response_stop (std::ostream &outFile) {
        proto::ReplicationResponseStop message;
        message.set_status(proto::ReplicationStatus::QUEUED);
        return write_message(outFile, message);
    }

    bool write_request_status (std::ostream &outFile) {
        proto::ReplicationRequestStatus message;
        message.set_id(generate_uuid());
        return write_message(outFile, message);
    }
    bool write_response_status (std::ostream &outFile) {
        proto::ReplicationResponseStatus message;
        message.set_status(proto::ReplicationStatus::FAILED);
        return write_message(outFile, message);
    }

    bool write_message_stream (const std::string &filename) {

        std::fstream outFile(filename, std::ios::out |
                                       std::ios::trunc |
                                       std::ios::binary);
        if (!outFile) {
            std::cerr << "Failed to open/create the output file." << std::endl;
            return false;                
        }
        bool status =
            write_header            (outFile, proto::ReplicationRequestHeader::REPLICATE) &&
            write_request_replicate (outFile) &&
            write_response_replicate(outFile) &&
            write_header            (outFile, proto::ReplicationRequestHeader::STOP) &&
            write_request_stop      (outFile) &&
            write_response_stop     (outFile) &&
            write_header            (outFile, proto::ReplicationRequestHeader::STATUS) &&
            write_request_status    (outFile) &&
            write_response_status   (outFile);

        outFile.flush();

        google::protobuf::ShutdownProtobufLibrary();

        return status;
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
    const bool status = ::write_message_stream(argv[1]);

    // It's always a good practice to come clean when finishing work with
    // the library. This will release the data structures, deallocate memory
    // and avoid confusing the memory leak checkers.

    google::protobuf::ShutdownProtobufLibrary();

    return status ? 0 : 1;
}