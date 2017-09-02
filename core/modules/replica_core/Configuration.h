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

#include <map>
#include <string>
#include <vector>

// Qserv headers

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {

/// The descriptor of a worker
struct WorkerInfo {

    /// The logical name of a worker
    std::string name;

    /// The host name (or IP address) of the worker service
    std::string svcHost;

    /// The port number of the worker service
    uint16_t svcPort;

    /// The port number for the file service run on a worker node
    uint16_t fsPort;

    /// The host name (or IP address) of the XRootD service
    std::string xrootdHost;

    /// The port number of the XRootD service
    uint16_t xrootdPort;

    /// An absolute path to the data directory under which the MySQL database
    /// folders are residing.
    std::string dataDir;
};

/// The descriptor of a database
struct DatabaseInfo {

    /// The name of a database
    std::string name;

    /// The names of the partitioned tables
    std::vector<std::string> partitionedTables;

    /// The list of fully replicated tables
    std::vector<std::string> regularTables;
};

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
     * The configuration can be used by controller or worker services.
     *
     * @param configFile - the name of a configuraiton file
     */
    explicit Configuration (const std::string &configFile);

    /// Destructor
    virtual ~Configuration();
    
    // ------------------------------------------------------------------------
    // -- Common configuration parameters of both the controller and workers --
    // ------------------------------------------------------------------------

    /// The names of known workers
    const std::vector<std::string>& workers () const { return _workers; }

    /// The names of known databases
    const std::vector<std::string>& databases () const { return _databases; }

    /// The maximum size of the request buffers in bytes
    size_t requestBufferSizeBytes () const { return _requestBufferSizeBytes; }

    /// A timeout in seconds for the network retry operations
    unsigned int retryTimeoutSec () const { return _retryTimeoutSec; }

    // --------------------------------------------------------
    // -- Configuration parameters of the controller service --
    // --------------------------------------------------------

    /// The port number for the controller's HTTP server
    uint16_t controllerHttpPort () const { return _controllerHttpPort; }

    /// The number of threads to run within the controller's HTTP server
    size_t controllerHttpThreads () const { return _controllerHttpThreads; }

    unsigned int controllerRequestTimeoutSec () const { return _controllerRequestTimeoutSec; }

    
    // ---------------------------------------------------
    // -- Configuration parameters related to databases --
    // ---------------------------------------------------

    /**
     * Return 'true' if the specified database is known to the configuraion
     *
     * @param name - the name of a database
     */
    bool isKnownDatabase (const std::string &name) const;

    /**
     * Return parameters of the specified database
     *
     * The method will throw std::out_of_range if the specified database was not
     * found in the configuration.
     *
     * @param name - the name of a database
     */
    const DatabaseInfo& databaseInfo (const std::string &name) const;

    // -----------------------------------------------------
    // -- Configuration parameters of the worker services --
    // -----------------------------------------------------

    /**
     * Return 'true' if the specified worker is known to the configuraion
     *
     * @param name - the name of a worker
     */
    bool isKnownWorker (const std::string &name) const;

    /**
     * Return parameters of the specified worker
     *
     * The method will throw std::out_of_range if the specified worker was not
     * found in the configuration.
     *
     * @param name - the name of a worker
     */
    const WorkerInfo& workerInfo (const std::string &name) const;

    /// Return the name of the default technology for implementing requests
    const std::string& workerTechnology () const { return _workerTechnology; }

    /// The maximum number of paralle network connections allowed by each worker
    size_t workerNumConnectionsLimit () const { return _workerNumConnectionsLimit; }

    /// The number of request processing threads in each worker service
    size_t workerNumProcessingThreads () const { return _workerNumProcessingThreads; }

    /// The number of request processing threads in each worker's file service
    size_t workerNumFsProcessingThreads () const { return _workerNumFsProcessingThreads; }

    /// Return the buffer size for the file I/O operations
    size_t workerFsBufferSizeBytes () const { return _workerFsBufferSizeBytes; }

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
    std::vector<std::string> _databases;

    size_t       _requestBufferSizeBytes;
    unsigned int _retryTimeoutSec;

    uint16_t     _controllerHttpPort;
    size_t       _controllerHttpThreads;
    unsigned int _controllerRequestTimeoutSec;

    std::string  _workerTechnology;
    size_t       _workerNumConnectionsLimit;
    size_t       _workerNumProcessingThreads;
    size_t       _workerNumFsProcessingThreads;
    size_t       _workerFsBufferSizeBytes;
    
    std::map<std::string, DatabaseInfo> _databaseInfo;
    std::map<std::string, WorkerInfo>   _workerInfo;
};

}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_CONFIGURATION_H