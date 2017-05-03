#include <arpa/inet.h>  // htonl

#include <chrono>
#include <ctime>
#include <iostream>
#include <string>
#include <thread>

#include <boost/array.hpp>
#include <boost/asio.hpp>

#include "boost/uuid/uuid.hpp"
#include "boost/uuid/uuid_generators.hpp"
#include "boost/uuid/uuid_io.hpp"

#include "proto/replication.pb.h"

namespace proto = lsst::qserv::proto;

using namespace std;
using namespace boost::asio;

namespace {

    std::string generate_uuid () {
        boost::uuids::uuid id = boost::uuids::random_generator()();
        return boost::uuids::to_string(id);
    }

    template <typename T>
    bool write_message (ip::tcp::socket &socket,
                        T& message) {

        // First send the frame header of an agreed length and format to notify
        // an opposite party on a length of the message itsef.

        const uint32_t dataLenHostBytes = message.ByteSize();
        const uint32_t dataLenNetBytes  = htonl(dataLenHostBytes);

        boost::system::error_code err;
        write (
            socket,
            buffer(reinterpret_cast<const char*>(&dataLenNetBytes),
                   sizeof dataLenNetBytes),
            err
        );
        if (err == boost::asio::error::eof) return false;   // Connection closed cleanly by peer.
        else if (err)
            throw boost::system::system_error(err);         // Some other error.

        // Setialize the message and sent the one as well

        char *data = new char[dataLenHostBytes];
        message.SerializeToArray(data, dataLenHostBytes);

        write (
            socket,
            buffer(data,
                   dataLenHostBytes),
            err
        );
        delete [] data;

        if (err == boost::asio::error::eof) return false;   // Connection closed cleanly by peer.
        else if (err)
            throw boost::system::system_error(err);         // Some other error.

        return true;
    }

    template <typename T>
    bool read_message (ip::tcp::socket &socket,
                      T& message) {

        // First read the frame header of an agreed length and format so that
        // we knew what an opposite party is going to send next.

        uint32_t dataLenNetBytes;
        
        boost::system::error_code err;
        boost::asio::read (
            socket,
            boost::asio::buffer (
                reinterpret_cast<char*>(&dataLenNetBytes),
                sizeof dataLenNetBytes
            ),
            boost::asio::transfer_at_least(sizeof dataLenNetBytes),
            err
        );
        if (err == boost::asio::error::eof) {
            std::cerr
                << "read_message"
                << "  ** ERROR ** Connection closed cleanly by peer."
                << std::endl;
            return false;

        } else if (err) {
            std::cerr
                << "read_message"
                << "  ** ERROR ** failed to read " << sizeof dataLenNetBytes << "bytes "
                << " from the scoket due to: " << err
                << std::endl;
            return false;
            //throw boost::system::system_error(err);
        }
        const uint32_t dataLenHostBytes  = ntohl(dataLenNetBytes);
        std::cout
            << "read_message:"
            << "  dataLenNetBytes=" << dataLenNetBytes
            << "  dataLenHostBytes=" << dataLenHostBytes
            << std::endl;

        // Now allocate anough buffer space for the message itself,
        // read the one and deserialize it into the message object.
        
        char* data = new char[dataLenHostBytes];

        boost::asio::read (
            socket,
            boost::asio::buffer (
                data,
                dataLenHostBytes
            ),
            boost::asio::transfer_at_least(dataLenHostBytes),
            err
        );
        if (err == boost::asio::error::eof) {
            std::cerr
                << "read_message"
                << "  ** ERROR ** Connection closed cleanly by peer."
                << std::endl;
            delete [] data;
            return false;

        } else if (err) {
            std::cerr
                << "read_message"
                << "  ** ERROR ** failed to read " << dataLenHostBytes << "bytes "
                << " from the scoket due to: " << err
                << std::endl;
            delete [] data;
            return false;
            //throw boost::system::system_error(err);
        }
        if (!message.ParseFromArray(data, dataLenHostBytes)) {
            std::cerr
                << "read_message"
                << "  ** ERROR **  failed to deserialize the message into a protobuf object."
                << std::endl;
            delete [] data;
            return false;
            //throw boost::system::system_error();
        }
        delete [] data;
        return true;        
    }
    bool write_header (ip::tcp::socket &socket,
                       const proto::ReplicationRequestHeader::Type type) {
        proto::ReplicationRequestHeader message;
        message.set_type(type);
        return write_message(socket, message);
    }
    bool write_request_replicate (ip::tcp::socket &socket) {
        proto::ReplicationRequestReplicate message;
        message.set_database("sdss_stripe82_00");
        message.set_chunk(123);
        message.set_id(generate_uuid());
        return write_message(socket, message);
    }
    bool read_response_replicate (ip::tcp::socket &socket) {
        proto::ReplicationResponseReplicate response;
        if (read_message(socket, response)) {
            std::cout
                << "response : " << "\n"
                << "  status : " << response.status() << "\n"
                << std::endl;
        }
        return true;
    }
    bool write_request_stop (ip::tcp::socket &socket) {
        proto::ReplicationRequestStop message;
        message.set_id(generate_uuid());
        return write_message(socket, message);
    }
    bool read_response_stop (ip::tcp::socket &socket) {
        proto::ReplicationResponseStop response;
        if (read_message(socket, response)) {
            std::cout
                << "response : " << "\n"
                << "  status : " << response.status() << "\n"
                << std::endl;
        }
        return true;
    }
    bool write_request_status (ip::tcp::socket &socket) {
        proto::ReplicationRequestStatus message;
        message.set_id(generate_uuid());
        return write_message(socket, message);
    }
    bool read_response_status (ip::tcp::socket &socket) {
        proto::ReplicationResponseStatus response;
        if (read_message(socket, response)) {
            std::cout
                << "response : " << "\n"
                << "  status : " << response.status() << "\n"
                << std::endl;
        }
        return true;
    }

    void client (const char *host) {
        try {
            io_service io_service;

            ip::tcp::resolver           resolver(io_service);
            ip::tcp::resolver::query    query(host, "50000");
            ip::tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);

            ip::tcp::socket socket(io_service);
            connect(socket, endpoint_iterator);

            for (;;) {

                bool status =
                    write_header           (socket, proto::ReplicationRequestHeader::REPLICATE) &&
                    write_request_replicate(socket) &&
                    read_response_replicate(socket) &&
                    write_header           (socket, proto::ReplicationRequestHeader::STOP) &&
                    write_request_stop     (socket) &&
                    read_response_stop     (socket) &&
                    write_header           (socket, proto::ReplicationRequestHeader::STATUS) &&
                    write_request_status   (socket) &&
                    read_response_status   (socket);

                if (status) break;

                this_thread::sleep_for(chrono::milliseconds(2000));
            }
        } catch (exception& e) {
            cerr << e.what() << endl;
        }
    }
}

int main (int argc, const char* const argv[]) {
    if (argc != 2) {
        cerr << "Usage: <host>" << endl;
        return 1;
    }
    ::client(argv[1]);
    return 0;
}