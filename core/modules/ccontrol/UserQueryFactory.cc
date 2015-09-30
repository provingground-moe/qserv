// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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

// Class header
#include "ccontrol/UserQueryFactory.h"

// System headers
#include <cassert>
#include <stdlib.h>
#include <string>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "ccontrol/ConfigMap.h"
#include "ccontrol/ConfigError.h"
#include "ccontrol/UserQuery.h"
#include "ccontrol/userQueryProxy.h"
#include "css/CssAccess.h"
#include "css/KvInterfaceImplMem.h"
#include "mysql/MySqlConfig.h"
#include "qdisp/Executive.h"
#include "qmeta/QMetaMysql.h"
#include "qproc/QuerySession.h"
#include "qproc/SecondaryIndex.h"
#include "rproc/InfileMerger.h"

namespace lsst {
namespace qserv {
namespace ccontrol {

LOG_LOGGER UserQueryFactory::_log = LOG_GET("lsst.qserv.ccontrol.UserQueryFactory");

/// Implementation class (PIMPL-style) for UserQueryFactory.
class UserQueryFactory::Impl {
public:
    /// Import non-CSS-related config from caller
    void readConfig(StringMap const& m);

    /// Import CSS config and construct CSS
    void readConfigCss(StringMap const& m,
                       std::shared_ptr<css::KvInterface> kvi);

    void initCss(std::string const& cssTech, std::string const& cssConn,
                    int timeout,
                    std::string const& emptyChunkPath);

    void initCss(std::shared_ptr<css::KvInterface> kvi,
                    std::string const& emptyChunkPath);

    void initMergerTemplate(); ///< Construct template config for merger

    /// State shared between UserQueries
    qdisp::Executive::Config::Ptr executiveConfig;
    std::shared_ptr<css::CssAccess> css;
    rproc::InfileMergerConfig infileMergerConfigTemplate;
    std::shared_ptr<qproc::SecondaryIndex> secondaryIndex;
    std::shared_ptr<qmeta::QMeta> queryMetadata;
    qmeta::CzarId qMetaCzarId;   ///< Czar ID in QMeta database
};

////////////////////////////////////////////////////////////////////////
UserQueryFactory::UserQueryFactory(StringMap const& m,
                                   std::string const& czarName,
                                   std::shared_ptr<css::KvInterface> kvi)
    :  _impl(std::make_shared<Impl>()) {
    ::putenv((char*)"XRDDEBUG=1");
    assert(_impl);
    _impl->readConfig(m);
    _impl->readConfigCss(m, kvi);

    // register czar in QMeta
    // TODO: check that czar with the same name is not active already?
    _impl->qMetaCzarId = _impl->queryMetadata->registerCzar(czarName);
}

std::pair<int,std::string>
UserQueryFactory::newUserQuery(std::string const& query,
                               std::string const& defaultDb,
                               std::string const& resultTable) {
    bool sessionValid = true;
    std::string errorExtra;
    qproc::QuerySession::Ptr qs = std::make_shared<qproc::QuerySession>(_impl->css);
    try {
        qs->setResultTable(resultTable);
        qs->setDefaultDb(defaultDb);
        qs->analyzeQuery(query);
    } catch (...) {
        errorExtra = "Unknown failure occured setting up QuerySession (query is invalid).";
        LOGF(_log, LOG_LVL_ERROR, errorExtra);
        sessionValid = false;
    }
    if(!qs->getError().empty()) {
        LOGF(_log, LOG_LVL_ERROR, "Invalid query: %1%" % qs->getError());
        sessionValid = false;
    }
    UserQuery* uq = new UserQuery(qs, _impl->qMetaCzarId);
    int sessionId = UserQuery_takeOwnership(uq);
    uq->_sessionId = sessionId;
    uq->_secondaryIndex = _impl->secondaryIndex;
    uq->_queryMetadata = _impl->queryMetadata;
    if(sessionValid) {
        uq->_executive = std::make_shared<qdisp::Executive>(_impl->executiveConfig, uq->_messageStore);
        rproc::InfileMergerConfig* ict = new rproc::InfileMergerConfig(_impl->infileMergerConfigTemplate);
        ict->targetTable = resultTable;
        uq->_infileMergerConfig.reset(ict);
        uq->_setupChunking();
    } else {
        uq->_errorExtra += errorExtra;
    }
    return std::make_pair(sessionId, qs->getProxyOrderBy());
}

void UserQueryFactory::Impl::readConfig(StringMap const& m) {
    ConfigMap cm(m);
    /// localhost:1094 is the most reasonable default, even though it is
    /// the wrong choice for all but small developer installations.
    std::string serviceUrl = cm.get(
        "frontend.xrootd", // czar.serviceUrl
        "WARNING! No xrootd spec. Using localhost:1094",
        "localhost:1094");
    executiveConfig = std::make_shared<qdisp::Executive::Config>(serviceUrl);
    // This should be overriden by the installer properly.
    infileMergerConfigTemplate.socket = cm.get(
        "resultdb.unix_socket",
        "Error, resultdb.unix_socket not found. Using /u1/local/mysql.sock.",
        "/u1/local/mysql.sock");
    infileMergerConfigTemplate.user = cm.get(
        "resultdb.user",
        "Error, resultdb.user not found. Using qsmaster.",
        "qsmaster");
    infileMergerConfigTemplate.targetDb = cm.get(
        "resultdb.db",
        "Error, resultdb.db not found. Using qservResult.",
        "qservResult");
    mysql::MySqlConfig mc;
    mc.username = infileMergerConfigTemplate.user;
    mc.dbName = infileMergerConfigTemplate.targetDb; // any valid db is ok.
    mc.socket = infileMergerConfigTemplate.socket;
    secondaryIndex = std::make_shared<qproc::SecondaryIndex>(mc);

    // get config parameters for qmeta db
    mysql::MySqlConfig qmetaConfig;
    qmetaConfig.hostname = cm.get(
        "qmeta.host",
        "Error, qmeta.host not found. Using empty host name.",
        "");
    qmetaConfig.port = cm.getTyped<unsigned>(
        "qmeta.port",
        "Error, qmeta.port not found. Using 0 for port.",
        0U);
    qmetaConfig.username = cm.get(
        "qmeta.user",
        "Error, qmeta.user not found. Using qsmaster.",
        "qsmaster");
    qmetaConfig.password = cm.get(
        "qmeta.passwd",
        "Error, qmeta.passwd not found. Using empty string.",
        "");
    qmetaConfig.socket = cm.get(
        "qmeta.unix_socket",
        "Error, qmeta.unix_socket not found. Using empty string.",
        "");
    qmetaConfig.dbName = cm.get(
        "qmeta.db",
        "Error, qmeta.db not found. Using qservMeta.",
        "qservMeta");
    queryMetadata = std::make_shared<qmeta::QMetaMysql>(qmetaConfig);
}

void UserQueryFactory::Impl::readConfigCss(
    StringMap const& m,
    std::shared_ptr<css::KvInterface> kvi) {
    ConfigMap cm(m);

    std::string emptyChunkPath = cm.get(
        "partitioner.emptychunkpath",
        "Error, missing path for Empty chunk file, using '.'.",
        ".");
    if (!kvi) {
        std::string cssTech = cm.get(
            "css.technology",
            "Error, css.technology not found.",
            "invalid");
        std::string cssConn = cm.get(
            "css.connection",
            "Error, css.connection not found.",
            "");
        int cssTimeout = cm.getTyped<int>(
            "css.timeout",
            "Error, css.timeout not found.",
            10000);

        initCss(cssTech, cssConn, cssTimeout, emptyChunkPath);
    } else {
        initCss(kvi, emptyChunkPath);
    }
}

void UserQueryFactory::Impl::initCss(std::string const& cssTech,
                                     std::string const& cssConn,
                                     int timeout_msec,
                                     std::string const& emptyChunkPath) {
    if (cssTech == "mem") {
        LOGF(_log, LOG_LVL_INFO, "Initializing memory-based css, with %1%" % cssConn);
        css = css::CssAccess::makeMemCss(cssConn, emptyChunkPath);
    } else {
        LOGF(_log, LOG_LVL_ERROR, "Unable to determine css technology, check config file.");
        throw ConfigError("Invalid css technology, check config file.");
    }
}

void UserQueryFactory::Impl::initCss(std::shared_ptr<css::KvInterface> kvi,
                                     std::string const& emptyChunkPath) {
    css = css::CssAccess::makeKvCss(kvi, emptyChunkPath);
    LOGF(_log, LOG_LVL_INFO, "Initializing cache-based css");
}

}}} // lsst::qserv::ccontrol
