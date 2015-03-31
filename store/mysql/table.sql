-- MySQL dump 10.13  Distrib 5.6.21-70.1, for Linux (x86_64)
--
-- Host: 127.0.0.1    Database: kite
-- ------------------------------------------------------
-- Server version	5.6.21-70.1-log

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `kite_msg_0`
--

DROP TABLE IF EXISTS `kite_msg_0`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `kite_msg_0` (
  `header` mediumblob,
  `body` mediumblob,
  `msg_type` tinyint(3) unsigned DEFAULT NULL,
  `message_id` varchar(255) NOT NULL,
  `topic` varchar(255) DEFAULT NULL,
  `message_type` varchar(255) DEFAULT NULL,
  `publish_group` varchar(255) DEFAULT NULL,
  `commit` tinyint(1) DEFAULT NULL,
  `expired_time` bigint(13) DEFAULT NULL,
  `publish_time` bigint(13) DEFAULT NULL,
  `deliver_count` int(11) DEFAULT NULL,
  `deliver_limit` int(11) DEFAULT NULL,
  `kite_server` varchar(255) DEFAULT NULL,
  `fail_groups` varchar(255) DEFAULT NULL,
  `succ_groups` varchar(255) DEFAULT NULL,
  `next_deliver_time` bigint(13) DEFAULT NULL,
  PRIMARY KEY (`message_id`),
  KEY `idx_commit` (`commit`),
  KEY `idx_kite_server` (`kite_server`),
  KEY `idx_expired_time` (`expired_time`),
  KEY `idx_recover_a` (`next_deliver_time`,`kite_server`,`expired_time`,`deliver_count`,`deliver_limit`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `kite_msg_1`
--

DROP TABLE IF EXISTS `kite_msg_1`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `kite_msg_1` (
  `header` mediumblob,
  `body` mediumblob,
  `msg_type` tinyint(3) unsigned DEFAULT NULL,
  `message_id` varchar(255) NOT NULL,
  `topic` varchar(255) DEFAULT NULL,
  `message_type` varchar(255) DEFAULT NULL,
  `publish_group` varchar(255) DEFAULT NULL,
  `commit` tinyint(1) DEFAULT NULL,
  `expired_time` bigint(13) DEFAULT NULL,
  `publish_time` bigint(13) DEFAULT NULL,
  `deliver_count` int(11) DEFAULT NULL,
  `deliver_limit` int(11) DEFAULT NULL,
  `kite_server` varchar(255) DEFAULT NULL,
  `fail_groups` varchar(255) DEFAULT NULL,
  `succ_groups` varchar(255) DEFAULT NULL,
  `next_deliver_time` bigint(13) DEFAULT NULL,
  PRIMARY KEY (`message_id`),
  KEY `idx_commit` (`commit`),
  KEY `idx_kite_server` (`kite_server`),
  KEY `idx_expired_time` (`expired_time`),
  KEY `idx_recover_a` (`next_deliver_time`,`kite_server`,`expired_time`,`deliver_count`,`deliver_limit`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `kite_msg_10`
--

DROP TABLE IF EXISTS `kite_msg_10`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `kite_msg_10` (
  `header` mediumblob,
  `body` mediumblob,
  `msg_type` tinyint(3) unsigned DEFAULT NULL,
  `message_id` varchar(255) NOT NULL,
  `topic` varchar(255) DEFAULT NULL,
  `message_type` varchar(255) DEFAULT NULL,
  `publish_group` varchar(255) DEFAULT NULL,
  `commit` tinyint(1) DEFAULT NULL,
  `expired_time` bigint(13) DEFAULT NULL,
  `publish_time` bigint(13) DEFAULT NULL,
  `deliver_count` int(11) DEFAULT NULL,
  `deliver_limit` int(11) DEFAULT NULL,
  `kite_server` varchar(255) DEFAULT NULL,
  `fail_groups` varchar(255) DEFAULT NULL,
  `succ_groups` varchar(255) DEFAULT NULL,
  `next_deliver_time` bigint(13) DEFAULT NULL,
  PRIMARY KEY (`message_id`),
  KEY `idx_commit` (`commit`),
  KEY `idx_kite_server` (`kite_server`),
  KEY `idx_expired_time` (`expired_time`),
  KEY `idx_recover_a` (`next_deliver_time`,`kite_server`,`expired_time`,`deliver_count`,`deliver_limit`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `kite_msg_11`
--

DROP TABLE IF EXISTS `kite_msg_11`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `kite_msg_11` (
  `header` mediumblob,
  `body` mediumblob,
  `msg_type` tinyint(3) unsigned DEFAULT NULL,
  `message_id` varchar(255) NOT NULL,
  `topic` varchar(255) DEFAULT NULL,
  `message_type` varchar(255) DEFAULT NULL,
  `publish_group` varchar(255) DEFAULT NULL,
  `commit` tinyint(1) DEFAULT NULL,
  `expired_time` bigint(13) DEFAULT NULL,
  `publish_time` bigint(13) DEFAULT NULL,
  `deliver_count` int(11) DEFAULT NULL,
  `deliver_limit` int(11) DEFAULT NULL,
  `kite_server` varchar(255) DEFAULT NULL,
  `fail_groups` varchar(255) DEFAULT NULL,
  `succ_groups` varchar(255) DEFAULT NULL,
  `next_deliver_time` bigint(13) DEFAULT NULL,
  PRIMARY KEY (`message_id`),
  KEY `idx_commit` (`commit`),
  KEY `idx_kite_server` (`kite_server`),
  KEY `idx_expired_time` (`expired_time`),
  KEY `idx_recover_a` (`next_deliver_time`,`kite_server`,`expired_time`,`deliver_count`,`deliver_limit`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `kite_msg_12`
--

DROP TABLE IF EXISTS `kite_msg_12`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `kite_msg_12` (
  `header` mediumblob,
  `body` mediumblob,
  `msg_type` tinyint(3) unsigned DEFAULT NULL,
  `message_id` varchar(255) NOT NULL,
  `topic` varchar(255) DEFAULT NULL,
  `message_type` varchar(255) DEFAULT NULL,
  `publish_group` varchar(255) DEFAULT NULL,
  `commit` tinyint(1) DEFAULT NULL,
  `expired_time` bigint(13) DEFAULT NULL,
  `publish_time` bigint(13) DEFAULT NULL,
  `deliver_count` int(11) DEFAULT NULL,
  `deliver_limit` int(11) DEFAULT NULL,
  `kite_server` varchar(255) DEFAULT NULL,
  `fail_groups` varchar(255) DEFAULT NULL,
  `succ_groups` varchar(255) DEFAULT NULL,
  `next_deliver_time` bigint(13) DEFAULT NULL,
  PRIMARY KEY (`message_id`),
  KEY `idx_commit` (`commit`),
  KEY `idx_kite_server` (`kite_server`),
  KEY `idx_expired_time` (`expired_time`),
  KEY `idx_recover_a` (`next_deliver_time`,`kite_server`,`expired_time`,`deliver_count`,`deliver_limit`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `kite_msg_13`
--

DROP TABLE IF EXISTS `kite_msg_13`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `kite_msg_13` (
  `header` mediumblob,
  `body` mediumblob,
  `msg_type` tinyint(3) unsigned DEFAULT NULL,
  `message_id` varchar(255) NOT NULL,
  `topic` varchar(255) DEFAULT NULL,
  `message_type` varchar(255) DEFAULT NULL,
  `publish_group` varchar(255) DEFAULT NULL,
  `commit` tinyint(1) DEFAULT NULL,
  `expired_time` bigint(13) DEFAULT NULL,
  `publish_time` bigint(13) DEFAULT NULL,
  `deliver_count` int(11) DEFAULT NULL,
  `deliver_limit` int(11) DEFAULT NULL,
  `kite_server` varchar(255) DEFAULT NULL,
  `fail_groups` varchar(255) DEFAULT NULL,
  `succ_groups` varchar(255) DEFAULT NULL,
  `next_deliver_time` bigint(13) DEFAULT NULL,
  PRIMARY KEY (`message_id`),
  KEY `idx_commit` (`commit`),
  KEY `idx_kite_server` (`kite_server`),
  KEY `idx_expired_time` (`expired_time`),
  KEY `idx_recover_a` (`next_deliver_time`,`kite_server`,`expired_time`,`deliver_count`,`deliver_limit`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `kite_msg_14`
--

DROP TABLE IF EXISTS `kite_msg_14`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `kite_msg_14` (
  `header` mediumblob,
  `body` mediumblob,
  `msg_type` tinyint(3) unsigned DEFAULT NULL,
  `message_id` varchar(255) NOT NULL,
  `topic` varchar(255) DEFAULT NULL,
  `message_type` varchar(255) DEFAULT NULL,
  `publish_group` varchar(255) DEFAULT NULL,
  `commit` tinyint(1) DEFAULT NULL,
  `expired_time` bigint(13) DEFAULT NULL,
  `publish_time` bigint(13) DEFAULT NULL,
  `deliver_count` int(11) DEFAULT NULL,
  `deliver_limit` int(11) DEFAULT NULL,
  `kite_server` varchar(255) DEFAULT NULL,
  `fail_groups` varchar(255) DEFAULT NULL,
  `succ_groups` varchar(255) DEFAULT NULL,
  `next_deliver_time` bigint(13) DEFAULT NULL,
  PRIMARY KEY (`message_id`),
  KEY `idx_commit` (`commit`),
  KEY `idx_kite_server` (`kite_server`),
  KEY `idx_expired_time` (`expired_time`),
  KEY `idx_recover_a` (`next_deliver_time`,`kite_server`,`expired_time`,`deliver_count`,`deliver_limit`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `kite_msg_15`
--

DROP TABLE IF EXISTS `kite_msg_15`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `kite_msg_15` (
  `header` mediumblob,
  `body` mediumblob,
  `msg_type` tinyint(3) unsigned DEFAULT NULL,
  `message_id` varchar(255) NOT NULL,
  `topic` varchar(255) DEFAULT NULL,
  `message_type` varchar(255) DEFAULT NULL,
  `publish_group` varchar(255) DEFAULT NULL,
  `commit` tinyint(1) DEFAULT NULL,
  `expired_time` bigint(13) DEFAULT NULL,
  `publish_time` bigint(13) DEFAULT NULL,
  `deliver_count` int(11) DEFAULT NULL,
  `deliver_limit` int(11) DEFAULT NULL,
  `kite_server` varchar(255) DEFAULT NULL,
  `fail_groups` varchar(255) DEFAULT NULL,
  `succ_groups` varchar(255) DEFAULT NULL,
  `next_deliver_time` bigint(13) DEFAULT NULL,
  PRIMARY KEY (`message_id`),
  KEY `idx_commit` (`commit`),
  KEY `idx_kite_server` (`kite_server`),
  KEY `idx_expired_time` (`expired_time`),
  KEY `idx_recover_a` (`next_deliver_time`,`kite_server`,`expired_time`,`deliver_count`,`deliver_limit`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `kite_msg_2`
--

DROP TABLE IF EXISTS `kite_msg_2`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `kite_msg_2` (
  `header` mediumblob,
  `body` mediumblob,
  `msg_type` tinyint(3) unsigned DEFAULT NULL,
  `message_id` varchar(255) NOT NULL,
  `topic` varchar(255) DEFAULT NULL,
  `message_type` varchar(255) DEFAULT NULL,
  `publish_group` varchar(255) DEFAULT NULL,
  `commit` tinyint(1) DEFAULT NULL,
  `expired_time` bigint(13) DEFAULT NULL,
  `publish_time` bigint(13) DEFAULT NULL,
  `deliver_count` int(11) DEFAULT NULL,
  `deliver_limit` int(11) DEFAULT NULL,
  `kite_server` varchar(255) DEFAULT NULL,
  `fail_groups` varchar(255) DEFAULT NULL,
  `succ_groups` varchar(255) DEFAULT NULL,
  `next_deliver_time` bigint(13) DEFAULT NULL,
  PRIMARY KEY (`message_id`),
  KEY `idx_commit` (`commit`),
  KEY `idx_kite_server` (`kite_server`),
  KEY `idx_expired_time` (`expired_time`),
  KEY `idx_recover_a` (`next_deliver_time`,`kite_server`,`expired_time`,`deliver_count`,`deliver_limit`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `kite_msg_3`
--

DROP TABLE IF EXISTS `kite_msg_3`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `kite_msg_3` (
  `header` mediumblob,
  `body` mediumblob,
  `msg_type` tinyint(3) unsigned DEFAULT NULL,
  `message_id` varchar(255) NOT NULL,
  `topic` varchar(255) DEFAULT NULL,
  `message_type` varchar(255) DEFAULT NULL,
  `publish_group` varchar(255) DEFAULT NULL,
  `commit` tinyint(1) DEFAULT NULL,
  `expired_time` bigint(13) DEFAULT NULL,
  `publish_time` bigint(13) DEFAULT NULL,
  `deliver_count` int(11) DEFAULT NULL,
  `deliver_limit` int(11) DEFAULT NULL,
  `kite_server` varchar(255) DEFAULT NULL,
  `fail_groups` varchar(255) DEFAULT NULL,
  `succ_groups` varchar(255) DEFAULT NULL,
  `next_deliver_time` bigint(13) DEFAULT NULL,
  PRIMARY KEY (`message_id`),
  KEY `idx_commit` (`commit`),
  KEY `idx_kite_server` (`kite_server`),
  KEY `idx_expired_time` (`expired_time`),
  KEY `idx_recover_a` (`next_deliver_time`,`kite_server`,`expired_time`,`deliver_count`,`deliver_limit`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `kite_msg_4`
--

DROP TABLE IF EXISTS `kite_msg_4`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `kite_msg_4` (
  `header` mediumblob,
  `body` mediumblob,
  `msg_type` tinyint(3) unsigned DEFAULT NULL,
  `message_id` varchar(255) NOT NULL,
  `topic` varchar(255) DEFAULT NULL,
  `message_type` varchar(255) DEFAULT NULL,
  `publish_group` varchar(255) DEFAULT NULL,
  `commit` tinyint(1) DEFAULT NULL,
  `expired_time` bigint(13) DEFAULT NULL,
  `publish_time` bigint(13) DEFAULT NULL,
  `deliver_count` int(11) DEFAULT NULL,
  `deliver_limit` int(11) DEFAULT NULL,
  `kite_server` varchar(255) DEFAULT NULL,
  `fail_groups` varchar(255) DEFAULT NULL,
  `succ_groups` varchar(255) DEFAULT NULL,
  `next_deliver_time` bigint(13) DEFAULT NULL,
  PRIMARY KEY (`message_id`),
  KEY `idx_commit` (`commit`),
  KEY `idx_kite_server` (`kite_server`),
  KEY `idx_expired_time` (`expired_time`),
  KEY `idx_recover_a` (`next_deliver_time`,`kite_server`,`expired_time`,`deliver_count`,`deliver_limit`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `kite_msg_5`
--

DROP TABLE IF EXISTS `kite_msg_5`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `kite_msg_5` (
  `header` mediumblob,
  `body` mediumblob,
  `msg_type` tinyint(3) unsigned DEFAULT NULL,
  `message_id` varchar(255) NOT NULL,
  `topic` varchar(255) DEFAULT NULL,
  `message_type` varchar(255) DEFAULT NULL,
  `publish_group` varchar(255) DEFAULT NULL,
  `commit` tinyint(1) DEFAULT NULL,
  `expired_time` bigint(13) DEFAULT NULL,
  `publish_time` bigint(13) DEFAULT NULL,
  `deliver_count` int(11) DEFAULT NULL,
  `deliver_limit` int(11) DEFAULT NULL,
  `kite_server` varchar(255) DEFAULT NULL,
  `fail_groups` varchar(255) DEFAULT NULL,
  `succ_groups` varchar(255) DEFAULT NULL,
  `next_deliver_time` bigint(13) DEFAULT NULL,
  PRIMARY KEY (`message_id`),
  KEY `idx_commit` (`commit`),
  KEY `idx_kite_server` (`kite_server`),
  KEY `idx_expired_time` (`expired_time`),
  KEY `idx_recover_a` (`next_deliver_time`,`kite_server`,`expired_time`,`deliver_count`,`deliver_limit`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `kite_msg_6`
--

DROP TABLE IF EXISTS `kite_msg_6`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `kite_msg_6` (
  `header` mediumblob,
  `body` mediumblob,
  `msg_type` tinyint(3) unsigned DEFAULT NULL,
  `message_id` varchar(255) NOT NULL,
  `topic` varchar(255) DEFAULT NULL,
  `message_type` varchar(255) DEFAULT NULL,
  `publish_group` varchar(255) DEFAULT NULL,
  `commit` tinyint(1) DEFAULT NULL,
  `expired_time` bigint(13) DEFAULT NULL,
  `publish_time` bigint(13) DEFAULT NULL,
  `deliver_count` int(11) DEFAULT NULL,
  `deliver_limit` int(11) DEFAULT NULL,
  `kite_server` varchar(255) DEFAULT NULL,
  `fail_groups` varchar(255) DEFAULT NULL,
  `succ_groups` varchar(255) DEFAULT NULL,
  `next_deliver_time` bigint(13) DEFAULT NULL,
  PRIMARY KEY (`message_id`),
  KEY `idx_commit` (`commit`),
  KEY `idx_kite_server` (`kite_server`),
  KEY `idx_expired_time` (`expired_time`),
  KEY `idx_recover_a` (`next_deliver_time`,`kite_server`,`expired_time`,`deliver_count`,`deliver_limit`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `kite_msg_7`
--

DROP TABLE IF EXISTS `kite_msg_7`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `kite_msg_7` (
  `header` mediumblob,
  `body` mediumblob,
  `msg_type` tinyint(3) unsigned DEFAULT NULL,
  `message_id` varchar(255) NOT NULL,
  `topic` varchar(255) DEFAULT NULL,
  `message_type` varchar(255) DEFAULT NULL,
  `publish_group` varchar(255) DEFAULT NULL,
  `commit` tinyint(1) DEFAULT NULL,
  `expired_time` bigint(13) DEFAULT NULL,
  `publish_time` bigint(13) DEFAULT NULL,
  `deliver_count` int(11) DEFAULT NULL,
  `deliver_limit` int(11) DEFAULT NULL,
  `kite_server` varchar(255) DEFAULT NULL,
  `fail_groups` varchar(255) DEFAULT NULL,
  `succ_groups` varchar(255) DEFAULT NULL,
  `next_deliver_time` bigint(13) DEFAULT NULL,
  PRIMARY KEY (`message_id`),
  KEY `idx_commit` (`commit`),
  KEY `idx_kite_server` (`kite_server`),
  KEY `idx_expired_time` (`expired_time`),
  KEY `idx_recover_a` (`next_deliver_time`,`kite_server`,`expired_time`,`deliver_count`,`deliver_limit`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `kite_msg_8`
--

DROP TABLE IF EXISTS `kite_msg_8`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `kite_msg_8` (
  `header` mediumblob,
  `body` mediumblob,
  `msg_type` tinyint(3) unsigned DEFAULT NULL,
  `message_id` varchar(255) NOT NULL,
  `topic` varchar(255) DEFAULT NULL,
  `message_type` varchar(255) DEFAULT NULL,
  `publish_group` varchar(255) DEFAULT NULL,
  `commit` tinyint(1) DEFAULT NULL,
  `expired_time` bigint(13) DEFAULT NULL,
  `publish_time` bigint(13) DEFAULT NULL,
  `deliver_count` int(11) DEFAULT NULL,
  `deliver_limit` int(11) DEFAULT NULL,
  `kite_server` varchar(255) DEFAULT NULL,
  `fail_groups` varchar(255) DEFAULT NULL,
  `succ_groups` varchar(255) DEFAULT NULL,
  `next_deliver_time` bigint(13) DEFAULT NULL,
  PRIMARY KEY (`message_id`),
  KEY `idx_commit` (`commit`),
  KEY `idx_kite_server` (`kite_server`),
  KEY `idx_expired_time` (`expired_time`),
  KEY `idx_recover_a` (`next_deliver_time`,`kite_server`,`expired_time`,`deliver_count`,`deliver_limit`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `kite_msg_9`
--

DROP TABLE IF EXISTS `kite_msg_9`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `kite_msg_9` (
  `header` mediumblob,
  `body` mediumblob,
  `msg_type` tinyint(3) unsigned DEFAULT NULL,
  `message_id` varchar(255) NOT NULL,
  `topic` varchar(255) DEFAULT NULL,
  `message_type` varchar(255) DEFAULT NULL,
  `publish_group` varchar(255) DEFAULT NULL,
  `commit` tinyint(1) DEFAULT NULL,
  `expired_time` bigint(13) DEFAULT NULL,
  `publish_time` bigint(13) DEFAULT NULL,
  `deliver_count` int(11) DEFAULT NULL,
  `deliver_limit` int(11) DEFAULT NULL,
  `kite_server` varchar(255) DEFAULT NULL,
  `fail_groups` varchar(255) DEFAULT NULL,
  `succ_groups` varchar(255) DEFAULT NULL,
  `next_deliver_time` bigint(13) DEFAULT NULL,
  PRIMARY KEY (`message_id`),
  KEY `idx_commit` (`commit`),
  KEY `idx_kite_server` (`kite_server`),
  KEY `idx_expired_time` (`expired_time`),
  KEY `idx_recover_a` (`next_deliver_time`,`kite_server`,`expired_time`,`deliver_count`,`deliver_limit`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2015-03-26 16:35:36
