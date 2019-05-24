// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015-2018 LSST Corporation.
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

#ifndef LSST_QSERV_CCONTROL_USERQUERYSELECT_H
#define LSST_QSERV_CCONTROL_USERQUERYSELECT_H
/**
  * @file
  *
  * @brief Umbrella container for user query state
   *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <cstdint>
#include <memory>
#include <mutex>

// Third-party headers

// Qserv headers
#include "ccontrol/UserQuery.h"
#include "css/StripingParams.h"
#include "qmeta/QInfo.h"
#include "qmeta/QStatus.h"
#include "qmeta/types.h"
#include "qproc/ChunkSpec.h"
#include "query/Constraint.h"

// Forward decl
namespace lsst {
namespace qserv {
namespace qdisp {
class Executive;
class MessageStore;
}
namespace qmeta {
class QMeta;
}
namespace qproc {
class QuerySession;
class SecondaryIndex;
}
namespace rproc {
class InfileMerger;
class InfileMergerConfig;
}}}

namespace lsst {
namespace qserv {

namespace qdisp {
class QdispPool;
}

namespace ccontrol {

/// UserQuerySelect : implementation of the UserQuery for regular SELECT statements.
class UserQuerySelect : public UserQuery {
public:

    UserQuerySelect(std::shared_ptr<qproc::QuerySession> const& qs,
                    std::shared_ptr<qdisp::MessageStore> const& messageStore,
                    std::shared_ptr<qdisp::Executive> const& executive,
                    std::shared_ptr<rproc::InfileMergerConfig> const& infileMergerConfig,
                    std::shared_ptr<qproc::SecondaryIndex> const& secondaryIndex,
                    std::shared_ptr<qmeta::QMeta> const& queryMetadata,
                    std::shared_ptr<qmeta::QStatus> const& queryStatsData,
                    qmeta::CzarId czarId,
                    std::shared_ptr<qdisp::QdispPool> const& qdispPool,
                    std::string const& errorExtra,
                    bool async);

    UserQuerySelect(UserQuerySelect const&) = delete;
    UserQuerySelect& operator=(UserQuerySelect const&) = delete;

    /**
     *  @param resultLocation:  Result location, if empty use result table with unique
     *                          name generated from query ID.
     *  @param msgTableName:  Message table name.
     */
    void qMetaRegister(std::string const& resultLocation, std::string const& msgTableName);

    // Accessors

    /// @return a non-empty string describing the current error state
    /// Returns an empty string if no errors have been detected.
    std::string getError() const override;

    /// Begin execution of the query over all ChunkSpecs added so far.
    void submit() override;

    /// Wait until the query has completed execution.
    /// @return the final execution state.
    QueryState join() override;

    /// Stop a query in progress (for immediate shutdowns)
    void kill() override;

    /// Release resources related to user query
    void discard() override;

    // Delegate objects
    std::shared_ptr<qdisp::MessageStore> getMessageStore() override {
        return _messageStore; }

    /// @return Name of the result table for this query, can be empty
    std::string getResultTableName() const override { return _resultTable; }

    /// @return Result location for this query, can be empty
    std::string getResultLocation() const override { return _resultLoc; }

    /// @return ORDER BY part of SELECT statement to be executed by proxy
    std::string getProxyOrderBy() const override;

    /// @return get the SELECT part of the SELECT statement to be executed by proxy
    std::string getResultSelectList() const override;

    std::string getQueryIdString() const override;

    /// @return this query's QueryId.
    QueryId getQueryId() const override { return _qMetaQueryId; }

    /// @return True if query is async query
    bool isAsync() const override { return _async; }

    void setupChunking();

    /// set up the merge table (stores results from workers)
    /// @throw UserQueryError if the merge table can't be set up (maybe the user query is not valid?). The
    /// exception's what() message will be returned to the user.
    void setupMerger() override;

private:
    void _discardMerger();
    void _qMetaUpdateStatus(qmeta::QInfo::QStatus qStatus);
    void _qMetaAddChunks(std::vector<int> const& chunks);

    // Delegate classes
    std::shared_ptr<qproc::QuerySession> _qSession;
    std::shared_ptr<qdisp::MessageStore> _messageStore;
    std::shared_ptr<qdisp::Executive> _executive;
    std::shared_ptr<rproc::InfileMergerConfig> _infileMergerConfig;
    std::shared_ptr<rproc::InfileMerger> _infileMerger;
    std::shared_ptr<qproc::SecondaryIndex> _secondaryIndex;
    std::shared_ptr<qmeta::QMeta> _queryMetadata;
    std::shared_ptr<qmeta::QStatus> _queryStatsData;

    qmeta::CzarId _qMetaCzarId; ///< Czar ID in QMeta database
    QueryId _qMetaQueryId{0};      ///< Query ID in QMeta database
    std::shared_ptr<qdisp::QdispPool> _qdispPool;
    /// QueryId in a standard string form, initially set to unknown.
    std::string _queryIdStr{QueryIdHelper::makeIdStr(0, true)};
    bool _killed{false};
    std::mutex _killMutex;
    std::string _errorExtra;    ///< Additional error information
    std::string _resultTable;   ///< Result table name
    std::string _resultLoc;     ///< Result location
    bool _async;                ///< true for async query
};

}}} // namespace lsst::qserv:ccontrol

#endif // LSST_QSERV_CCONTROL_USERQUERYSELECT_H
