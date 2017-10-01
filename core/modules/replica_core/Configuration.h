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
  * Class Configuration is a base class for a family of concrete classes
  * providing configuration services for the components of the Replication
  * system.
  */
class Configuration {

public:

    // Copy semantics is prohibited

    Configuration (Configuration const&) = delete;
    Configuration& operator= (Configuration const&) = delete;

    /// Destructor
    virtual ~Configuration();

    /**
     * Return the original (minus security-related info) path to the configuration
     * source.
     */
    virtual std::string configUrl () const=0;


    // ------------------------------------------------------------------------
    // -- Common configuration parameters of both the controller and workers --
    // ------------------------------------------------------------------------

    /// The names of known workers
    std::vector<std::string> const& workers () const { return _workers; }

    /// The names of known databases
    std::vector<std::string> const& databases () const { return _databases; }

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
    bool isKnownDatabase (std::string const& name) const;

    /**
     * Return parameters of the specified database
     *
     * The method will throw std::out_of_range if the specified database was not
     * found in the configuration.
     *
     * @param name - the name of a database
     */
    DatabaseInfo const& databaseInfo (std::string const& name) const;


    // -----------------------------------------------------
    // -- Configuration parameters of the worker services --
    // -----------------------------------------------------

    /**
     * Return 'true' if the specified worker is known to the configuraion
     *
     * @param name - the name of a worker
     */
    bool isKnownWorker (std::string const& name) const;

    /**
     * Return parameters of the specified worker
     *
     * The method will throw std::out_of_range if the specified worker was not
     * found in the configuration.
     *
     * @param name - the name of a worker
     */
    WorkerInfo const& workerInfo (std::string const& name) const;

    /// Return the name of the default technology for implementing requests
    std::string const& workerTechnology () const { return _workerTechnology; }

    /// The number of request processing threads in each worker service
    size_t workerNumProcessingThreads () const { return _workerNumProcessingThreads; }

    /// The number of request processing threads in each worker's file service
    size_t workerNumFsProcessingThreads () const { return _workerNumFsProcessingThreads; }

    /// Return the buffer size for the file I/O operations
    size_t workerFsBufferSizeBytes () const { return _workerFsBufferSizeBytes; }

protected:

    // Default values of some parameters are used by both the default constructor
    // of this class as well as by subclasses when initializing the configuration
    // object.

    static const size_t       defaultRequestBufferSizeBytes;
    static const unsigned int defaultRetryTimeoutSec;
    static const uint16_t     defaultControllerHttpPort;
    static const size_t       defaultControllerHttpThreads;
    static const unsigned int defaultControllerRequestTimeoutSec;
    static const std::string  defaultWorkerTechnology;
    static const size_t       defaultWorkerNumProcessingThreads;
    static const size_t       defaultWorkerNumFsProcessingThreads;
    static const size_t       defaultWorkerFsBufferSizeBytes;
    static const std::string  defaultWorkerSvcHost;
    static const uint16_t     defaultWorkerSvcPort;
    static const uint16_t     defaultWorkerFsPort;
    static const std::string  defaultWorkerXrootdHost;
    static const uint16_t     defaultWorkerXrootdPort;
    static const std::string  defaultDataDir;

    /**
     * Construct the object
     *
     * The constructor will initialize the configuration parameters with
     * some default states, some of which are probably meaninless.
     */
    Configuration ();

protected:

    // Cached values of the parameters

    std::vector<std::string> _workers;
    std::vector<std::string> _databases;

    size_t       _requestBufferSizeBytes;
    unsigned int _retryTimeoutSec;

    uint16_t     _controllerHttpPort;
    size_t       _controllerHttpThreads;
    unsigned int _controllerRequestTimeoutSec;

    std::string  _workerTechnology;

    size_t _workerNumProcessingThreads;
    size_t _workerNumFsProcessingThreads;
    size_t _workerFsBufferSizeBytes;
    
    std::map<std::string, DatabaseInfo> _databaseInfo;
    std::map<std::string, WorkerInfo>   _workerInfo;
};

}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_CONFIGURATION_H