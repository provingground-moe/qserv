ET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 ;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 ;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL' ;

CREATE SCHEMA IF NOT EXISTS `replication` ;
USE `replication` ;


-- -----------------------------------------------------
-- Table `replication`.`controller`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `replication`.`controller` ;

CREATE  TABLE IF NOT EXISTS `replication`.`controller` (

  `id`  VARCHAR(255) NOT NULL ,

  `hostname`  VARCHAR(255) NOT NULL ,
  `pid`       INT          NOT NULL ,

  `create_time`  BIGINT UNSIGNED NOT NULL ,

  PRIMARY KEY (`id`)
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `replication`.`job`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `replication`.`job` ;

CREATE  TABLE IF NOT EXISTS `replication`.`job` (

  `id`             VARCHAR(255) NOT NULL ,
  `controller_id`  VARCHAR(255) NOT NULL ,

  `type` ENUM ('REPLICATE',
               'PURGE',
               'REBALANCE',
               'DELETE_WORKER',
               'ADD_WORKER',
               'OTHER') NOT NULL ,

  `status`      VARCHAR(255) NOT NULL ,
  `ext_status`  VARCHAR(255) DEFAULT '' ,

  `begin_time`  BIGINT UNSIGNED NOT NULL ,
  `end_time`    BIGINT UNSIGNED NOT NULL ,

  PRIMARY KEY (`id`) ,

  CONSTRAINT `job_fk_1`
    FOREIGN KEY (`controller_id` )
    REFERENCES `replication`.`controller` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `replication`.`job_replicate`
-- -----------------------------------------------------
--
-- Extended parameters of the 'REPLICATE' jobs
--
DROP TABLE IF EXISTS `replication`.`job_replicate` ;

CREATE  TABLE IF NOT EXISTS `replication`.`job_replicate` (

  `job_id`  VARCHAR(255) NOT NULL ,

  `num_replicas`  INT NOT NULL ,

  CONSTRAINT `job_replicate_fk_1`
    FOREIGN KEY (`job_id` )
    REFERENCES `replication`.`job` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `replication`.`job_purge`
-- -----------------------------------------------------
--
-- Extended parameters of the 'PURGE' jobs
--
DROP TABLE IF EXISTS `replication`.`job_purge` ;

CREATE  TABLE IF NOT EXISTS `replication`.`job_purge` (

  `job_id`  VARCHAR(255) NOT NULL ,

  `num_replicas`  INT NOT NULL ,

  CONSTRAINT `job_purge_fk_1`
    FOREIGN KEY (`job_id` )
    REFERENCES `replication`.`job` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `replication`.`job_rebalance`
-- -----------------------------------------------------
--
-- Extended parameters of the 'REBALANCE' jobs
--
DROP TABLE IF EXISTS `replication`.`job_rebalance` ;

CREATE  TABLE IF NOT EXISTS `replication`.`job_rebalance` (

  `job_id`  VARCHAR(255) NOT NULL ,

  `num_replicas`  INT NOT NULL ,

  CONSTRAINT `job_rebalance_fk_1`
    FOREIGN KEY (`job_id` )
    REFERENCES `replication`.`job` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `replication`.`job_delete_worker`
-- -----------------------------------------------------
--
-- Extended parameters of the 'DELETE_WORKER' jobs
--
DROP TABLE IF EXISTS `replication`.`job_delete_worker` ;

CREATE  TABLE IF NOT EXISTS `replication`.`job_delete_worker` (

  `job_id`  VARCHAR(255) NOT NULL ,

  `worker`        VARCHAR(255) NOT NULL ,
  `num_replicas`  INT NOT NULL ,

  CONSTRAINT `job_delete_worker_fk_1`
    FOREIGN KEY (`job_id` )
    REFERENCES `replication`.`job` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `replication`.`job_add_worker`
-- -----------------------------------------------------
--
-- Extended parameters of the 'ADD_WORKER' jobs
--
DROP TABLE IF EXISTS `replication`.`job_add_worker` ;

CREATE  TABLE IF NOT EXISTS `replication`.`job_add_worker` (

  `job_id`  VARCHAR(255) NOT NULL ,

  `worker`        VARCHAR(255) NOT NULL ,
  `num_replicas`  INT NOT NULL ,

  CONSTRAINT `job_add_worker_fk_1`
    FOREIGN KEY (`job_id` )
    REFERENCES `replication`.`job` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `replication`.`request`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `replication`.`request` ;

CREATE  TABLE IF NOT EXISTS `replication`.`request` (

  `id`      VARCHAR(255) NOT NULL ,
  `job_id`  VARCHAR(255) NOT NULL ,

  `name` ENUM ('REPLICA_CREATE',
               'REPLICA_DELETE') NOT NULL ,

  `begin_time`  BIGINT UNSIGNED NOT NULL ,
  `end_time`    BIGINT UNSIGNED NOT NULL ,

  `status`         VARCHAR(255) NOT NULL ,
  `ext_status`     VARCHAR(255) DEFAULT '' ,
  `server_status`  VARCHAR(255) DEFAULT '' ,

  PRIMARY KEY (`id`) ,

  CONSTRAINT `request_fk_1`
    FOREIGN KEY (`job_id` )
    REFERENCES `replication`.`job` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `replication`.`request_replica_create`
-- -----------------------------------------------------
--
-- Extended parameters of the 'REPLICA_CREATE' requests
--
DROP TABLE IF EXISTS `replication`.`request_replica_create` ;

CREATE  TABLE IF NOT EXISTS `replication`.`request_replica_create` (

  `request_id`  VARCHAR(255) NOT NULL ,

  `worker`  VARCHAR(255) NOT NULL ,
  `db`      VARCHAR(255) NOT NULL ,
  `chunk`   INT UNSIGNED NOT NULL ,
  
  `source_worker`  VARCHAR(255) NOT NULL ,

  CONSTRAINT `request_replica_create_fk_1`
    FOREIGN KEY (`request_id` )
    REFERENCES `replication`.`request` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `replication`.`request_replica_delete`
-- -----------------------------------------------------
--
-- Extended parameters of the 'REPLICA_DELETE' requests
--
DROP TABLE IF EXISTS `replication`.`request_replica_delete` ;

CREATE  TABLE IF NOT EXISTS `replication`.`request_replica_delete` (

  `request_id`  VARCHAR(255) NOT NULL ,

  `worker`  VARCHAR(255) NOT NULL ,
  `db`      VARCHAR(255) NOT NULL ,
  `chunk`   INT UNSIGNED NOT NULL ,

  CONSTRAINT `request_replica_delete_fk_1`
    FOREIGN KEY (`request_id` )
    REFERENCES `replication`.`request` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `replication`.`replica`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `replication`.`replica` ;

CREATE  TABLE IF NOT EXISTS `replication`.`replica` (

  `id`  INT NOT NULL AUTO_INCREMENT ,

  `worker`  VARCHAR(255) NOT NULL ,
  `db`      VARCHAR(255) NOT NULL ,
  `chunk`   INT UNSIGNED NOT NULL ,

  `create_time`  BIGINT UNSIGNED NOT NULL ,

  PRIMARY KEY           (`id`) ,
  UNIQUE  KEY `replica` (`worker`,`db`,`chunk`)
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `replication`.`replica_file`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `replication`.`replica_file` ;

CREATE  TABLE IF NOT EXISTS `replication`.`replica_file` (

  `replica_id`  INT NOT NULL ,

  `name`  VARCHAR(255) NOT NULL ,
  `cs`    VARCHAR(255) NOT NULL ,
  `size`  BIGINT UNSIGNED NOT NULL ,

  `begin_create_time`  BIGINT UNSIGNED NOT NULL ,
  `end_create_time`    BIGINT UNSIGNED NOT NULL ,

  UNIQUE  KEY `file` (`replica_id`,`name`) ,

  CONSTRAINT `replica_file_fk_1`
    FOREIGN KEY (`replica_id` )
    REFERENCES `replication`.`replica` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


SET SQL_MODE=@OLD_SQL_MODE ;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS ;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS ;