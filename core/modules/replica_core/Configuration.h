/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
 *
 * This product includes software developed by the
 * LSST Project (http://www.lsst.org/).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the LSST License Statement and
 * the GNU General Public License along with this program.  If not,
 * see <http://www.lsstcorp.org/LegalNotices/>.
 */
#ifndef LSST_QSERV_REPLICA_CORE_CONFIGURATION_H
#define LSST_QSERV_REPLICA_CORE_CONFIGURATION_H

/// Configuration.h declares:
///
/// class Configuration
/// (see individual class documentation for more information)

// System headers

#include <string>
#include <vector>

// Qserv headers

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {

/**
  * Class Configuration provides configuration services for all servers.
  * 
  * The implementation of this class relies upon the basic parser
  * of the INI-style configuration files. In addition to the basic parser,
  * this class also:
  * 
  *   - enforces a specific schema of the INI file
  *   - ensures all required parameters are found in the file
  *   - sets default values for the optional parameters
  *   - caches parameters in memory
  */
class Configuration {

public:

    // Default construction and copy semantics are proxibited

    Configuration () = delete;
    Configuration (Configuration const&) = delete;
    Configuration & operator= (Configuration const&) = delete;

    /**
     * Construct the object
     *
     * @param configFile - the name of a configuraiton file
     */
    explicit Configuration (const std::string &configFile);

    /// Destructor
    ~Configuration();
    
    // --------------------------------------------------------------------
    // -- Common configuration parameters of both the master and workers --
    // --------------------------------------------------------------------

    /// The names of known workers.
    std::vector<std::string> workers () const { return _workers; }

    /// The maximum size of the request buffers in bytes.
    size_t requestBufferSizeBytes () const { return _requestBufferSizeBytes; }

    /// Default timeout in seconds for the network retry operations
    unsigned int defaultRetryTimeoutSec () const { return _defaultRetryTimeoutSec; }

    // ----------------------------------------------------
    // -- Configuration parameters of the master service --
    // ----------------------------------------------------

    /// The port number for the master's HTTP server
    uint16_t masterHttpPort () const { return _masterHttpPort; }

    /// The number of threads to run within the master's HTTP server
    size_t masterHttpThreads () const { return _masterHttpThreads; }

    unsigned int masterRequestTimeoutSec () const { return _masterRequestTimeoutSec; }

    // -----------------------------------------------------
    // -- Configuration parameters of the worker services --
    // -----------------------------------------------------

    /// The port number for the worker services
    uint16_t workerSvcPort () const {return _workerSvcPort; }

    /// The port number for the worker XRootD services
    uint16_t workerXrootdPort () const { return _workerXrootdPort; }

    /// The maximum number of paralle network connections allowed by each worker
    size_t workerNumConnectionsLimit () const { return _workerNumConnectionsLimit; }

    size_t workerNumProcessingThreads () const { return _workerNumProcessingThreads; }

private:

    /**
     * Analyze the configuration and initialize the cache of parameters.
     *
     * The method will throw one of these exceptions:
     *
     *   std::runtime_error
     *      the configuration is not consistent with expectations of the application
     */
    void loadConfiguration ();

private:

    // Parameters of the object

    const std::string _configFile;

    // Cached values of the parameters

    std::vector<std::string> _workers;

    size_t       _requestBufferSizeBytes;
    unsigned int _defaultRetryTimeoutSec;

    uint16_t     _masterHttpPort;
    size_t       _masterHttpThreads;
    unsigned int _masterRequestTimeoutSec;

    uint16_t     _workerSvcPort;
    uint16_t     _workerXrootdPort;
    size_t       _workerNumConnectionsLimit;
    size_t       _workerNumProcessingThreads;
};

}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_CONFIGURATION_H