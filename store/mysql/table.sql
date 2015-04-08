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

CREATE Database  IF NOT EXISTS kite_0;
CREATE Database  IF NOT EXISTS kite_1;
CREATE Database  IF NOT EXISTS kite_2;
CREATE Database  IF NOT EXISTS kite_3;
CREATE Database  IF NOT EXISTS kite_4;
CREATE Database  IF NOT EXISTS kite_5;
CREATE Database  IF NOT EXISTS kite_6;
CREATE Database  IF NOT EXISTS kite_7;



DROP TABLE IF EXISTS `kite_msg_0`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `kite_msg_0` (
  `header` mediumblob,
  `body` mediumblob,
  `msg_type` tinyint(3) unsigned DEFAULT NULL,
  `message_id` varchar(32) NOT NULL,
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
  `message_id` varchar(32) NOT NULL,
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


DROP TABLE IF EXISTS `kite_msg_2`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `kite_msg_2` (
  `header` mediumblob,
  `body` mediumblob,
  `msg_type` tinyint(3) unsigned DEFAULT NULL,
  `message_id` varchar(32) NOT NULL,
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
  `message_id` varchar(32) NOT NULL,
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

