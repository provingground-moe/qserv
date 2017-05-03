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
         * Initiated sending a reply asynchronously.
         */
        void send () {
            cout << "tcp_connection::send" << endl;

            _messageSent = make_daytime_string();

            async_write(_socket,
                        buffer(_messageSent),
                        boost::bind(&tcp_connection::handle_write,
                                    shared_from_this(),
                                    boost::asio::placeholders::error,
                                    boost::asio::placeholders::bytes_transferred));
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
         * The placeholder for calback on finishing (either successfully
         * or not) aynchronious writes.
         */
        void handle_write (const boost::system::error_code& /*err*/,
                           size_t                           /*bytes_transferred*/) {
            cout << "tcp_connection::handle_write" << endl;
        }

    private:

        ip::tcp::socket _socket;

        // Cache messages here while waiting for a completion of
        // the asyncromous read/write operations in order to prevent
        // them from being automatically destroyed in a context where
        // the data transfer operations are initiated.

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

            // Accepting up to 2 connections at a time

            start_accept(1);
            start_accept(2);
        }

        void run () {
            _io_service.run();
        }
        
    private:
        
        void start_accept (int num) {

            cout << "tcp_server::start_accept::" << num << endl;

            tcp_connection::pointer new_connection =
                tcp_connection::create(_io_service);
                
            _acceptor.async_accept (
                new_connection->socket(),
                boost::bind(&tcp_server::handle_accept,
                            this,
                            new_connection,
                            boost::asio::placeholders::error,
                            num));
        }
        void handle_accept (tcp_connection::pointer          new_connection,
                            const boost::system::error_code& err,
                            int                              num) {

            cout << "tcp_server::handle_accept::" << num << endl;

            if (!err) {
                new_connection->send();
            } else {
                cerr << "tcp_server::handle_accept err:" << err << endl;
            }
            start_accept(num);
        }

    private:

        io_service        _io_service;
        ip::tcp::acceptor _acceptor;
    };

    void run_server () {
        try {
            tcp_server server;
            server.run();

        } catch (exception& e) {
            cerr << e.what() << endl;
        }
    }
}

int main (int argc, const char* const argv[]) {
    ::run_server();
    return 0;
}