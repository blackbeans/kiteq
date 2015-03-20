
drop table if exists `kite_msg_0`;
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
  PRIMARY KEY (`message_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


 drop table if exists `kite_msg_1`;
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
  PRIMARY KEY (`message_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

  drop table if exists `kite_msg_2`;
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
  PRIMARY KEY (`message_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


 drop table if exists `kite_msg_3`;
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
  PRIMARY KEY (`message_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

  drop table if exists `kite_msg_4`;
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
  PRIMARY KEY (`message_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


 drop table if exists `kite_msg_5`;
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
  PRIMARY KEY (`message_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


drop table if exists `kite_msg_6`;
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
  PRIMARY KEY (`message_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


  drop table if exists `kite_msg_7`;
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
  PRIMARY KEY (`message_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

  drop table if exists `kite_msg_8`;
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
  PRIMARY KEY (`message_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


 drop table if exists `kite_msg_9`;
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
  PRIMARY KEY (`message_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

  drop table if exists `kite_msg_10`;
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
  PRIMARY KEY (`message_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

  drop table if exists `kite_msg_11`;
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
  PRIMARY KEY (`message_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


  drop table if exists `kite_msg_12`;
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
  PRIMARY KEY (`message_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


  drop table if exists `kite_msg_13`;
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
  PRIMARY KEY (`message_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

  drop table if exists `kite_msg_14`;
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
  PRIMARY KEY (`message_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


 drop table if exists `kite_msg_15`;
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
  PRIMARY KEY (`message_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



alter table kite_msg_0 add index idx_msg_type(msg_type);
alter table kite_msg_1 add index idx_msg_type(msg_type);
alter table kite_msg_2 add index idx_msg_type(msg_type);
alter table kite_msg_3 add index idx_msg_type(msg_type);
alter table kite_msg_4 add index idx_msg_type(msg_type);
alter table kite_msg_5 add index idx_msg_type(msg_type);
alter table kite_msg_6 add index idx_msg_type(msg_type);
alter table kite_msg_7 add index idx_msg_type(msg_type);
alter table kite_msg_8 add index idx_msg_type(msg_type);
alter table kite_msg_9 add index idx_msg_type(msg_type);
alter table kite_msg_9 add index idx_msg_type(msg_type);
alter table kite_msg_10 add index idx_msg_type(msg_type);
alter table kite_msg_11 add index idx_msg_type(msg_type);
alter table kite_msg_12 add index idx_msg_type(msg_type);
alter table kite_msg_13 add index idx_msg_type(msg_type);
alter table kite_msg_14 add index idx_msg_type(msg_type);
alter table kite_msg_15 add index idx_msg_type(msg_type);



alter table kite_msg_0 add index idx_commit(commit);
alter table kite_msg_1 add index idx_commit(commit);
alter table kite_msg_2 add index idx_commit(commit);
alter table kite_msg_3 add index idx_commit(commit);
alter table kite_msg_4 add index idx_commit(commit);
alter table kite_msg_5 add index idx_commit(commit);
alter table kite_msg_6 add index idx_commit(commit);
alter table kite_msg_7 add index idx_commit(commit);
alter table kite_msg_8 add index idx_commit(commit);
alter table kite_msg_9 add index idx_commit(commit);
alter table kite_msg_10 add index idx_commit(commit);
alter table kite_msg_11 add index idx_commit(commit);
alter table kite_msg_12 add index idx_commit(commit);
alter table kite_msg_13 add index idx_commit(commit);
alter table kite_msg_14 add index idx_commit(commit);
alter table kite_msg_15 add index idx_commit(commit);



drop index idx_recover on kite_msg_0;
drop index idx_recover on kite_msg_1;
drop index idx_recover on kite_msg_2;
drop index idx_recover on kite_msg_3;
drop index idx_recover on kite_msg_4;
drop index idx_recover on kite_msg_5;
drop index idx_recover on kite_msg_6;
drop index idx_recover on kite_msg_7;
drop index idx_recover on kite_msg_8;
drop index idx_recover on kite_msg_9;
drop index idx_recover on kite_msg_10;
drop index idx_recover on kite_msg_11;
drop index idx_recover on kite_msg_12;
drop index idx_recover on kite_msg_13;
drop index idx_recover on kite_msg_14;
drop index idx_recover on kite_msg_15;

alter table kite_msg_0 add index idx_recover(kite_server,expired_time,next_deliver_time);
alter table kite_msg_1 add index idx_recover(kite_server,expired_time,next_deliver_time);
alter table kite_msg_2 add index idx_recover(kite_server,expired_time,next_deliver_time);
alter table kite_msg_3 add index idx_recover(kite_server,expired_time,next_deliver_time);
alter table kite_msg_4 add index idx_recover(kite_server,expired_time,next_deliver_time);
alter table kite_msg_5 add index idx_recover(kite_server,expired_time,next_deliver_time);
alter table kite_msg_6 add index idx_recover(kite_server,expired_time,next_deliver_time);
alter table kite_msg_7 add index idx_recover(kite_server,expired_time,next_deliver_time);
alter table kite_msg_8 add index idx_recover(kite_server,expired_time,next_deliver_time);
alter table kite_msg_9 add index idx_recover(kite_server,expired_time,next_deliver_time);
alter table kite_msg_10 add index idx_recover(kite_server,expired_time,next_deliver_time);
alter table kite_msg_11 add index idx_recover(kite_server,expired_time,next_deliver_time);
alter table kite_msg_12 add index idx_recover(kite_server,expired_time,next_deliver_time);
alter table kite_msg_13 add index idx_recover(kite_server,expired_time,next_deliver_time);
alter table kite_msg_14 add index idx_recover(kite_server,expired_time,next_deliver_time);
alter table kite_msg_15 add index idx_recover(kite_server,expired_time,next_deliver_time);