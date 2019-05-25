/*
 * LSST Data Management System
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
#ifndef LSST_QSERV_REPLICA_SQLSCHEMAUTILS_H
#define LSST_QSERV_REPLICA_SQLSCHEMAUTILS_H

// System headers
#include <list>
#include <string>
#include <tuple>

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Utility class SqlSchemaUtils hosts tools for manipulating schema(s).
 */
class SqlSchemaUtils {
public:

    // Objects of this class aren't allowed to be created

    SqlSchemaUtils() = delete;
    ~SqlSchemaUtils() = delete;

    /**
     * Read column definitions from a text file. Each column is defined
     * on a separate line of a file. And the format of the file looks
     * like this:
     * 
     *   <column-name> <column-type-definition>
     *
     * @param fileName  the name of a file to be parsed
     *
     * @return a collection of pairs representing the name of a column and its
     *   MySQL type definition
     *
     * @throws std::invalid_argument  if the file can't be open/read
     *   or if it has a non-valid format
     */
    static std::list<std::pair<std::string,std::string>> readFromTextFile(
            std::string const& fileName);
};

}}} // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_SQLSCHEMAUTILS_H */
