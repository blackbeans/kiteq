#!/usr/bin/env bash

# "db 初始化kiteq脚本 单库多表"




shardNum=4

hashNum=`expr 16 / 4`


echo "------------分库："$shardNum"\t每个表"$hashNum"张表------------------"

DB_NAME="kite"
TB_NAME="kite_msg"
for db_index in {0..3}; do
    mysql -u root -e "
        drop database if exists ${DB_NAME}_${db_index};
        create database ${DB_NAME}_${db_index};"

        for tb_index in {0..3}; do
            mysql -u root -e "
            use ${DB_NAME}_${db_index};
            DROP TABLE IF EXISTS ${TB_NAME}_${tb_index};
            create table if not exists ${TB_NAME}_${tb_index} (
                  id int NOT NULL AUTO_INCREMENT,
                  header mediumblob,
                  body mediumblob,
                  msg_type tinyint(3) unsigned DEFAULT NULL,
                  message_id varchar(32) NOT NULL,
                  topic varchar(255) DEFAULT NULL,
                  message_type varchar(255) DEFAULT NULL,
                  publish_group varchar(255) DEFAULT NULL,
                  commit tinyint(1) DEFAULT NULL,
                  expired_time bigint(13) DEFAULT NULL,
                  publish_time bigint(13) DEFAULT NULL,
                  deliver_count int(11) DEFAULT NULL,
                  deliver_limit int(11) DEFAULT NULL,
                  kite_server varchar(255) DEFAULT NULL,
                  fail_groups varchar(255) DEFAULT NULL,
                  succ_groups varchar(255) DEFAULT NULL,
                  next_deliver_time bigint(13) DEFAULT NULL,
                  PRIMARY KEY (message_id),
                  KEY idx_id (id),
                  KEY idx_commit (commit),
                  KEY idx_kite_server (kite_server),
                  KEY idx_expired_time (expired_time),
                  KEY idx_recover_a (next_deliver_time,kite_server,expired_time,deliver_count,deliver_limit)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
                "
                echo "create table "${TB_NAME}_${tb_index}" succ!"
        done;
        #创建一下DLQ的表
         mysql -u root -e "
            use ${DB_NAME}_${db_index};
            DROP TABLE IF EXISTS ${TB_NAME}_dlq;
            create table if not exists ${TB_NAME}_dlq (
                  id int NOT NULL AUTO_INCREMENT,
                  header mediumblob,
                  body mediumblob,
                  msg_type tinyint(3) unsigned DEFAULT NULL,
                  message_id varchar(32) NOT NULL,
                  topic varchar(255) DEFAULT NULL,
                  message_type varchar(255) DEFAULT NULL,
                  publish_group varchar(255) DEFAULT NULL,
                  commit tinyint(1) DEFAULT NULL,
                  expired_time bigint(13) DEFAULT NULL,
                  publish_time bigint(13) DEFAULT NULL,
                  deliver_count int(11) DEFAULT NULL,
                  deliver_limit int(11) DEFAULT NULL,
                  kite_server varchar(255) DEFAULT NULL,
                  fail_groups varchar(255) DEFAULT NULL,
                  succ_groups varchar(255) DEFAULT NULL,
                  next_deliver_time bigint(13) DEFAULT NULL,
                  PRIMARY KEY (message_id),
                  KEY idx_id (id),
                  KEY idx_commit (commit),
                  KEY idx_kite_server (kite_server),
                  KEY idx_expired_time (expired_time),
                  KEY idx_recover_a (next_deliver_time,kite_server,expired_time,deliver_count,deliver_limit)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
                "
                echo "create table "${TB_NAME}_dlq" succ!"

done;