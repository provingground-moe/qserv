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
#ifndef LSST_QSERV_REPLICA_CORE_FILEUTILS_H
#define LSST_QSERV_REPLICA_CORE_FILEUTILS_H

/// Configuration.h declares:
///
/// class FileUtils
/// (see individual class documentation for more information)

// System headers

#include <string>
#include <tuple>
#include <vector>

// Qserv headers

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {

// Forward declarations
class DatabaseInfo;

/**
  * The utility class encapsulating various operations with file systems.
  *
  * ATTENTION: this class can't be instantiated
  */
class FileUtils {

public:

    // Default construction and copy semantics are prohibited

    FileUtils () = delete;
    FileUtils (FileUtils const&) = delete;
    FileUtils & operator= (FileUtils const&) = delete;

    ~FileUtils () = delete;

    /**
     * Return a list of all file names representing partitioned tables
     * of a database and a chunk.
     *
     * @param databaseInfo - the description of the database and tables
     * @param chunk        - the chunk number
     */
    static std::vector<std::string> partitionedFiles (const DatabaseInfo &databaseInfo,
                                                      unsigned int        chunk);

    /**
     * Return a list of all file names representing regular tables
     * of a database.
     *
     * @param databaseInfo - the description of the database and tables
     */
    static std::vector<std::string> regularFiles (const DatabaseInfo &databaseInfo);


    /**
     * Parse the file name and if successfull fill in a tuple with components of
     * the name. The file name are expected to matche one of the following patterns:
     *
     *   <table>_<chunk>.<ext>
     *   <table>FullOverlap_<chunk>.<ext>
     *
     * Where:
     *
     *   <table> - is the name of a valid partitioned table as per the database info
     *   <chunk> - is a numeric chunk number
     *   <ext>   - is one of the database file extentions
     *
     * @param parsed       - the tuple to be initialized upon the successfull completion
     * @param fileName     - the name of a file (no directory name) including its extention
     * @param databaseInfo - the database specification
     *
     * @return 'true' if the file name matches one of the expected pattens. The tuple's elements
     * will be (in the order of their definition): the name of a table (including 'FullOverlap'
     * where applies), the number of a chunk, adm its extention (w/o the dot)
     */
    static bool parsePartitionedFile (std::tuple<std::string, unsigned int, std::string> &parsed,
                                      const std::string                                  &fileName,
                                      const DatabaseInfo                                 &databaseInfo);
};

}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_FILEUTILS_H
