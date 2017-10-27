SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 ;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 ;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL' ;


-- -----------------------------------------------------
-- Table `controller`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `controller` ;

CREATE TABLE IF NOT EXISTS `controller` (

  `id`  VARCHAR(255) NOT NULL ,

  `hostname`  VARCHAR(255) NOT NULL ,
  `pid`       INT          NOT NULL ,

  `start_time`  BIGINT UNSIGNED NOT NULL ,

  PRIMARY KEY (`id`)
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `job`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `job` ;

CREATE TABLE IF NOT EXISTS `job` (

  `id`             VARCHAR(255) NOT NULL ,
  `controller_id`  VARCHAR(255) NOT NULL ,

  `type` ENUM ('REPLICATE',
               'PURGE',
               'REBALANCE',
               'DELETE_WORKER',
               'ADD_WORKER') NOT NULL ,

  `state`      VARCHAR(255) NOT NULL ,
  `ext_state`  VARCHAR(255) DEFAULT '' ,

  `begin_time`  BIGINT UNSIGNED NOT NULL ,
  `end_time`    BIGINT UNSIGNED NOT NULL ,

  PRIMARY KEY (`id`) ,

  CONSTRAINT `job_fk_1`
    FOREIGN KEY (`controller_id` )
    REFERENCES `controller` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;

-- -----------------------------------------------------
-- Table `job_fixup`
-- -----------------------------------------------------
--
-- Extended parameters of the 'FIXUP' jobs
--
DROP TABLE IF EXISTS `job_fixup` ;

CREATE TABLE IF NOT EXISTS `job_fixup` (

  `job_id`  VARCHAR(255) NOT NULL ,

  `database_family`  VARCHAR(255) NOT NULL ,

  CONSTRAINT `job_fixup_fk_1`
    FOREIGN KEY (`job_id` )
    REFERENCES `job` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;

-- -----------------------------------------------------
-- Table `job_replicate`
-- -----------------------------------------------------
--
-- Extended parameters of the 'REPLICATE' jobs
--
DROP TABLE IF EXISTS `job_replicate` ;

CREATE TABLE IF NOT EXISTS `job_replicate` (

  `job_id`  VARCHAR(255) NOT NULL ,

  `database_family`  VARCHAR(255) NOT NULL ,
  `num_replicas`     INT          NOT NULL ,

  CONSTRAINT `job_replicate_fk_1`
    FOREIGN KEY (`job_id` )
    REFERENCES `job` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `job_purge`
-- -----------------------------------------------------
--
-- Extended parameters of the 'PURGE' jobs
--
DROP TABLE IF EXISTS `job_purge` ;

CREATE TABLE IF NOT EXISTS `job_purge` (

  `job_id`  VARCHAR(255) NOT NULL ,

  `database_family`  VARCHAR(255) NOT NULL ,
  `num_replicas`     INT          NOT NULL ,

  CONSTRAINT `job_purge_fk_1`
    FOREIGN KEY (`job_id` )
    REFERENCES `job` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `job_rebalance`
-- -----------------------------------------------------
--
-- Extended parameters of the 'REBALANCE' jobs
--
DROP TABLE IF EXISTS `job_rebalance` ;

CREATE TABLE IF NOT EXISTS `job_rebalance` (

  `job_id`  VARCHAR(255) NOT NULL ,

  `database_family`  VARCHAR(255) NOT NULL ,
  `num_replicas`     INT NOT NULL ,

  CONSTRAINT `job_rebalance_fk_1`
    FOREIGN KEY (`job_id` )
    REFERENCES `job` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `job_delete_worker`
-- -----------------------------------------------------
--
-- Extended parameters of the 'DELETE_WORKER' jobs
--
DROP TABLE IF EXISTS `job_delete_worker` ;

CREATE TABLE IF NOT EXISTS `job_delete_worker` (

  `job_id`  VARCHAR(255) NOT NULL ,

  `worker`        VARCHAR(255) NOT NULL ,
  `num_replicas`  INT          NOT NULL ,

  CONSTRAINT `job_delete_worker_fk_1`
    FOREIGN KEY (`job_id` )
    REFERENCES `job` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `job_add_worker`
-- -----------------------------------------------------
--
-- Extended parameters of the 'ADD_WORKER' jobs
--
DROP TABLE IF EXISTS `job_add_worker` ;

CREATE TABLE IF NOT EXISTS `job_add_worker` (

  `job_id`  VARCHAR(255) NOT NULL ,

  `worker`        VARCHAR(255) NOT NULL ,
  `num_replicas`  INT NOT NULL ,

  CONSTRAINT `job_add_worker_fk_1`
    FOREIGN KEY (`job_id` )
    REFERENCES `job` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `request`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `request` ;

CREATE TABLE IF NOT EXISTS `request` (

  `id`      VARCHAR(255) NOT NULL ,
  `job_id`  VARCHAR(255) NOT NULL ,

  `name` ENUM ('REPLICA_CREATE',
               'REPLICA_DELETE') NOT NULL ,

  `worker`   VARCHAR(255) NOT NULL ,
  `priority` INT NOT NULL ,

  `state`          VARCHAR(255) NOT NULL ,
  `ext_state`      VARCHAR(255) DEFAULT '' ,
  `server_status`  VARCHAR(255) DEFAULT '' ,

  `c_create_time`   BIGINT UNSIGNED NOT NULL ,
  `c_start_time`    BIGINT UNSIGNED NOT NULL ,
  `w_receive_time`  BIGINT UNSIGNED NOT NULL ,
  `w_start_time`    BIGINT UNSIGNED NOT NULL ,
  `w_finish_time`   BIGINT UNSIGNED NOT NULL ,
  `c_finish_time`   BIGINT UNSIGNED NOT NULL ,

  PRIMARY KEY (`id`) ,

  CONSTRAINT `request_fk_1`
    FOREIGN KEY (`job_id` )
    REFERENCES `job` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `request_replica_create`
-- -----------------------------------------------------
--
-- Extended parameters of the 'REPLICA_CREATE' requests
--
DROP TABLE IF EXISTS `request_replica_create` ;

CREATE TABLE IF NOT EXISTS `request_replica_create` (

  `request_id`  VARCHAR(255) NOT NULL ,

  `database` VARCHAR(255) NOT NULL ,
  `chunk`    INT UNSIGNED NOT NULL ,
  
  `source_worker`  VARCHAR(255) NOT NULL ,

  CONSTRAINT `request_replica_create_fk_1`
    FOREIGN KEY (`request_id` )
    REFERENCES `request` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `request_replica_delete`
-- -----------------------------------------------------
--
-- Extended parameters of the 'REPLICA_DELETE' requests
--
DROP TABLE IF EXISTS `request_replica_delete` ;

CREATE TABLE IF NOT EXISTS `request_replica_delete` (

  `request_id`  VARCHAR(255) NOT NULL ,

  `database` VARCHAR(255) NOT NULL ,
  `chunk`    INT UNSIGNED NOT NULL ,

  CONSTRAINT `request_replica_delete_fk_1`
    FOREIGN KEY (`request_id` )
    REFERENCES `request` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `replica`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `replica` ;

CREATE TABLE IF NOT EXISTS `replica` (

  `id`  INT NOT NULL AUTO_INCREMENT ,

  `worker`   VARCHAR(255) NOT NULL ,
  `database` VARCHAR(255) NOT NULL ,
  `chunk`    INT UNSIGNED NOT NULL ,

  `begin_create_time`  BIGINT UNSIGNED NOT NULL ,
  `end_create_time`    BIGINT UNSIGNED NOT NULL ,

  PRIMARY KEY           (`id`) ,
  UNIQUE  KEY `replica` (`worker`,`database`,`chunk`)
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `replica_file`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `replica_file` ;

CREATE TABLE IF NOT EXISTS `replica_file` (

  `replica_id`  INT NOT NULL ,

  `name`  VARCHAR(255) NOT NULL ,
  `cs`    VARCHAR(255) NOT NULL ,
  `size`  BIGINT UNSIGNED NOT NULL ,

  `begin_create_time`  BIGINT UNSIGNED NOT NULL ,
  `end_create_time`    BIGINT UNSIGNED NOT NULL ,

  UNIQUE  KEY `file` (`replica_id`,`name`) ,

  CONSTRAINT `replica_file_fk_1`
    FOREIGN KEY (`replica_id` )
    REFERENCES `replica` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;

-- --------------------------------------------------------------
-- Table `config`
-- --------------------------------------------------------------
--
-- The common parameters and defaults shared by all components
-- of the replication system. It also provides default values
-- for some critical parameters of the worker-side services.

DROP TABLE IF EXISTS `config` ;

CREATE TABLE IF NOT EXISTS `config` (

  `category` VARCHAR(255) NOT NULL ,
  `param`    VARCHAR(255) NOT NULL ,
  `value`    VARCHAR(255) NOT NULL ,

  UNIQUE  KEY (`category`,`param`)
)
ENGINE = InnoDB;


-- Common parameters of all types of servers

INSERT INTO `config` VALUES ('common', 'request_buf_size_bytes',     '1024');
INSERT INTO `config` VALUES ('common', 'request_retry_interval_sec', '1');

-- Controller-specific parameters

INSERT INTO `config` VALUES ('controller', 'http_server_port',    '80');
INSERT INTO `config` VALUES ('controller', 'http_server_threads', '1');
INSERT INTO `config` VALUES ('controller', 'request_timeout_sec', '600');

-- Default parameters for all workers unless overwritten in worker-specific
-- tables

INSERT INTO `config` VALUES ('worker', 'technology',                 'FS');
INSERT INTO `config` VALUES ('worker', 'svc_port',                   '50000');
INSERT INTO `config` VALUES ('worker', 'fs_port',                    '50001');
INSERT INTO `config` VALUES ('worker', 'num_svc_processing_threads', '10');
INSERT INTO `config` VALUES ('worker', 'num_fs_processing_threads',  '16');
INSERT INTO `config` VALUES ('worker', 'fs_buf_size_bytes',          '1048576');
INSERT INTO `config` VALUES ('worker', 'data_dir',                   '/datasets/gapon/test/replication/{worker}');

-- -----------------------------------------------------
-- Table `config_worker`
-- -----------------------------------------------------
--
-- Worker-specific configuration parameters and overrides
-- of the corresponidng default values if needed

DROP TABLE IF EXISTS `config_worker` ;

CREATE TABLE IF NOT EXISTS `config_worker` (

  `name`         VARCHAR(255)       NOT NULL ,      -- the name of the worker

  `is_enabled`   BOOLEAN            NOT NULL ,      -- is enabled for replication
  `is_read_only` BOOLEAN            NOT NULL ,      -- a subclass of 'is_enabled' which restricts use of
                                                    -- the worker for reading replicas. No new replicas can't be
                                                    -- placed onto this class of workers.

  `svc_host`     VARCHAR(255)       NOT NULL ,      -- the host name on which the worker server runs
  `svc_port`     SMALLINT UNSIGNED  DEFAULT NULL ,  -- override for the global default

  `fs_host`      VARCHAR(255)       NOT NULL ,      -- the host name on which the built-in FileServer runs
  `fs_port`      SMALLINT UNSIGNED  DEFAULT NULL ,  -- override for the global default

  `data_dir`     VARCHAR(255)       DEFAULT NULL ,  -- a file system path to the databases

  PRIMARY KEY (`name`) ,

  UNIQUE  KEY (`svc_host`, `svc_port`) ,
  UNIQUE  KEY (`fs_host`,  `fs_port`)
)
ENGINE = InnoDB;


-- Preload parameters for runnig all services on the same host

INSERT INTO `config_worker` VALUES ('one',   1, 0, 'lsst-dev01', '50001', 'lsst-dev01', '50101', NULL);
INSERT INTO `config_worker` VALUES ('two',   1, 0, 'lsst-dev01', '50002', 'lsst-dev01', '50102', NULL);
INSERT INTO `config_worker` VALUES ('three', 1, 0, 'lsst-dev01', '50003', 'lsst-dev01', '50103', NULL);
INSERT INTO `config_worker` VALUES ('four',  1, 0, 'lsst-dev01', '50004', 'lsst-dev01', '50104', NULL);
INSERT INTO `config_worker` VALUES ('five',  1, 0, 'lsst-dev01', '50005', 'lsst-dev01', '50105', NULL);
INSERT INTO `config_worker` VALUES ('six',   1, 0, 'lsst-dev01', '50006', 'lsst-dev01', '50106', NULL);
INSERT INTO `config_worker` VALUES ('seven', 1, 0, 'lsst-dev01', '50007', 'lsst-dev01', '50107', NULL);


-- --------------------------------------------------------------
-- Table `config_worker_ext`
-- --------------------------------------------------------------
--
-- The additional parameters overriding the defaults for individual
-- worker services.

DROP TABLE IF EXISTS `config_worker_ext` ;

CREATE TABLE IF NOT EXISTS `config_worker_ext` (

  `worker_name`  VARCHAR(255) NOT NULL ,
  `param`        VARCHAR(255) NOT NULL ,
  `value`        VARCHAR(255) NOT NULL ,

  UNIQUE  KEY (`worker_name`, `param`, `value`) ,

  CONSTRAINT `config_worker_ext_fk_1`
    FOREIGN KEY (`worker_name` )
    REFERENCES `config_worker` (`name` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `config_database_family`
-- -----------------------------------------------------
--
-- Groups of databases which require coordinated replication
-- efforts (the number of replicas, chunk collocation)
--
-- NOTE: chunk collocation is implicitly determined
--       by database membership within a family

DROP TABLE IF EXISTS `config_database_family` ;

CREATE TABLE IF NOT EXISTS `config_database_family` (

  `name`                   VARCHAR(255)  NOT NULL ,
  `min_replication_level`  INT UNSIGNED  NOT NULL ,    -- minimum number of replicas per chunk

  UNIQUE  KEY (`name`)
)
ENGINE = InnoDB;

-- -----------------------------------------------------
-- Table `config_database`
-- -----------------------------------------------------
--
-- Databases which are managd by the replication system
--
-- NOTE: Each database belongs to exctly one family, even
--       if that family has the only members (that database).

DROP TABLE IF EXISTS `config_database` ;

CREATE TABLE IF NOT EXISTS `config_database` (

  `database`     VARCHAR(255)  NOT NULL ,
  `family_name`  VARCHAR(255)  NOT NULL ,

  -- Each database is allowed to belong to one family only
  --
  UNIQUE  KEY (`database`) ,

  CONSTRAINT `config_database_fk_1`
    FOREIGN KEY (`family_name` )
    REFERENCES `config_database_family` (`name` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;

-- -----------------------------------------------------
-- Table `config_database_table`
-- -----------------------------------------------------
--
-- Database tables

DROP TABLE IF EXISTS `config_database_table` ;

CREATE TABLE IF NOT EXISTS `config_database_table` (

  `database`  VARCHAR(255)  NOT NULL ,
  `table`     VARCHAR(255)  NOT NULL ,

  `is_partitioned` BOOLEAN NOT NULL ,

  UNIQUE  KEY (`database`, `table`) ,

  CONSTRAINT `config_database_table_fk_1`
    FOREIGN KEY (`database` )
    REFERENCES `config_database` (`database` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;

--------------------------------------------
-- Preload parameters for testing purposes
--------------------------------------------

-- This database lives witin its own family

INSERT INTO `config_database_family` VALUES ('db1', 1);
INSERT INTO `config_database`        VALUES ('db1', 'db1');
INSERT INTO `config_database_table`  VALUES ('db1', 'Object',       1);
INSERT INTO `config_database_table`  VALUES ('db1', 'Source',       1);
INSERT INTO `config_database_table`  VALUES ('db1', 'ForcedSource', 1);
INSERT INTO `config_database_table`  VALUES ('db1', 'Filter',       0);

-- This family has two members for which the replication activities need
-- to be coordinated.

INSERT INTO `config_database_family` VALUES ('production', 3);
INSERT INTO `config_database`        VALUES ('db2', 'production');
INSERT INTO `config_database`        VALUES ('db3', 'production');

INSERT INTO `config_database_table`  VALUES ('db2', 'Main', 1);

INSERT INTO `config_database_table`  VALUES ('db3', 'Object',       1);
INSERT INTO `config_database_table`  VALUES ('db3', 'Source',       1);
INSERT INTO `config_database_table`  VALUES ('db3', 'ForcedSource', 1);




SET SQL_MODE=@OLD_SQL_MODE ;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS ;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS ;