#include <arpa/inet.h>  // ntohl

#include <cstring>      // strncmp
#include <ctime>        // time_t, time, ctime
#include <iostream>
#include <memory>       // shared_ptr, enable_shared_from_this
#include <string>

#include <boost/array.hpp>
#include <boost/asio.hpp>
#include <boost/bind.hpp>

using namespace std;
using namespace boost::asio;

namespace {

    /**
     * An instance of this class is created for each incomming
     * connection.
     */
    class tcp_connection
        : public enable_shared_from_this<tcp_connection> {

    public:

        typedef shared_ptr<tcp_connection> pointer;

        /**
         * Static factory method is needed to prevent issue with the lifespan
         * and memory management of instances created otherwise (as values or via
         * low-level pointers).
         */
        static pointer create (io_service& io_service) {
            return pointer(new tcp_connection(io_service));
        }

        ip::tcp::socket& socket () { return _socket; }

        /**
         * Begin communicating asynchroniously with a client. This is essentially
         * an RPC protocol which runs an asynchronos loop (a chain) of three steps:
         * 
         *   - ASYNC: read a frame header of a request
         *   -  SYNC: read the request itself 
         *   - ASYNC: write a frame header of a reply to the request
         *            and the reply itself
         *
         * NOTES: A reason why the read phase is split into two steps is
         *        that a client is expected to send both the header and
         *        the request at once. This means that the whole incomming message
         *        will be already available on the server's host memory when
         *        an asyncronous handler will fire. However, due to a variable length
         *        of the request we should know its length before attempting
         *        to read the request itself as this (the later) will require
         *        two things: 1) to ensure enough we have enough buffer space
         *        allocated, and 2) to tell the asynchrnous reader function
         *        how many bytes exactly are we going to read.
         * 
         * The chain ends when a client disconnects or when an error condition
         * is met.
         */
        void async_communicate () {
            cout << "tcp_connection::  async_communicate" << endl;
            _read();
        }
        
    private:

        /**
         * Helper method
         */
        static string make_daytime_string () {
            time_t now = time(0);
            return ctime(&now);
        }

    private:

        /**
         * The constructor of the class.
         */
        tcp_connection (boost::asio::io_service& io_service)
            : _socket(io_service) {
            cout << "tcp_connection::" << endl;
        }

        /**
         * Begin reading (asynchronosly) the frame header of a new request
         *
         * The frame header is presently a 32-bit unsigned integer
         * representing the length of the subsequent message.
         */
        void _read () {
            cout << "tcp_connection::    _read" << endl;

            async_read(_socket,
                       buffer(reinterpret_cast<char*>(&_requestLenNet), sizeof _requestLenNet),
                       boost::bind(&tcp_connection::handle_read,
                                   shared_from_this(),
                                   boost::asio::placeholders::error,
                                   boost::asio::placeholders::bytes_transferred));
        }

        /**
         * Begin write ((asynchronosly) a result
         */
        void _write () {
            cout << "tcp_connection::    _write" << endl;

            _messageSent = make_daytime_string();

            async_write(_socket,
                        buffer(_messageSent),
                        boost::bind(&tcp_connection::handle_written,
                                    shared_from_this(),
                                    boost::asio::placeholders::error,
                                    boost::asio::placeholders::bytes_transferred));
        }

        /**
         * The placeholder for calback on finishing (either successfully
         * or not) aynchronious reads.
         */
        void handle_read (const boost::system::error_code& error,
                          size_t                           bytes_transferred) {

            const uint32_t requestLenHost = ntohl(_requestLenNet);

            cout << "tcp_connection::      handle_read"
                 << "  **"
                 << "  bytes_transferred: " << bytes_transferred
                 << "  _requestLenNet: "    << _requestLenNet
                 << "  requestLenHost: "    << requestLenHost
                 << "  **" << endl;

            if      (error == error::eof) return;           // Connection closed cleanly by peer.
            else if (error)
                throw boost::system::system_error(error);   // Some other error.

            _write();
        }
        
        /**
         * The placeholder for calback on finishing (either successfully
         * or not) aynchronious writes.
         */
        void handle_written (const boost::system::error_code& /*error*/,
                             size_t                           bytes_transferred) {
            cout << "tcp_connection::      handle_written ** bytes_transferred: " << bytes_transferred << " ** " << endl;
            
            _read();
        }

    private:

        ip::tcp::socket _socket;

        // Cache messages here while waiting for a completion of
        // the asyncromous read/write operations in order to prevent
        // them from being automatically destroyed in a context where
        // the data transfer operations are initiated.

        uint32_t _requestLenNet;    // the length of the request in the Network order
        
        string _messageSent;
    };
    
    /**
     * A dedicated instance of this class acceppts connection requests
     * on a dedicated instance of the IO service run on a thread.
     */
    class tcp_server {

    public:

        tcp_server ()
            : _io_service(),
              _acceptor(_io_service,
                        ip::tcp::endpoint(ip::tcp::v4(), 50000)) {

            cout << "tcp_server::" << endl;
        }

        /**
         * Configure the service to accept the specified numer of connections
         * simultaneously. Then run the server.
         */
        void run (uint32_t numConnections=1) {

            for (uint32_t connIdx = 0; connIdx < numConnections; ++connIdx) {
                start_accept(connIdx);
            }
            _io_service.run();
        }
        
    private:
        
        void start_accept (uint32_t connIdx) {

            cout << "tcp_server::start_accept::" << connIdx << endl;

            tcp_connection::pointer new_connection =
                tcp_connection::create(_io_service);
                
            _acceptor.async_accept (
                new_connection->socket(),
                boost::bind(&tcp_server::handle_accept,
                            this,
                            new_connection,
                            boost::asio::placeholders::error,
                            connIdx));
        }
        void handle_accept (tcp_connection::pointer          new_connection,
                            const boost::system::error_code& err,
                            uint32_t                         connIdx) {

            cout << "tcp_server::handle_accept::" << connIdx << endl;

            if (!err) {
                new_connection->async_communicate();
            } else {
                cerr << "tcp_server::handle_accept err:" << err << endl;
            }
            start_accept(connIdx);
        }

    private:

        io_service        _io_service;
        ip::tcp::acceptor _acceptor;
    };

    void run_server (uint32_t numConnections) {
        try {
            tcp_server server;
            server.run(numConnections);

        } catch (exception& e) {
            cerr << e.what() << endl;
        }
    }
}

int main (int argc, const char* const argv[]) {
    ::run_server(argc > 1 ? stoul(argv[1]) : 1);
    return 0;
}