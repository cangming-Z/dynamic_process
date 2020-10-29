# -*- coding:utf-8 -*-
# _author_ = 'zhangdeqiang

import time
import datetime
from proto import tag_pb2
from kafka import KafkaProducer
from until.getMesInfo import MesData
from until.getdb import get_postgresql_conn


kafkaUrl = '192.168.0.130:9092'
topicList = ['kafka-test-001']

host = '192.168.0.105'
port = '5432'
user = 'gpadmin'
passwd = 'gpadmin'
db = 'postgres'

sleepTime = 3

conn = get_postgresql_conn(host, port, user, passwd, db)
gp = MesData(conn)

baseStartTime = datetime.datetime(2020, 7, 2, 0, 0, 0)
baseEndTime = datetime.datetime(2020, 7, 2, 00, 25, 59)
startTime = datetime.datetime(2020, 7, 2, 0, 0, 0)
endTime = datetime.datetime(2020, 7, 2, 00, 25, 59)
baseTable = 'ods_hbase_data_kknf_1_prt_7'
mergeTable = 'ods_hbase_data_kknf_1_prt_7'

sql = 'SELECT DISTINCT tag FROM %s' % baseTable
data = gp.db_rw(sql)
tags = []

kafkaProducer = KafkaProducer(bootstrap_servers=kafkaUrl)

while startTime < endTime:
    timeStandard = datetime.datetime.now().strftime("%Y%m%d%H%M%S").encode()
    for topicName in topicList:
        proto = tag_pb2.interface_param()
        sqlGetValues = "select tag,value from %s where time between '%s' and '%s'" \
                       % (mergeTable, startTime,
                          (startTime + datetime.timedelta(minutes=sleepTime - 1)))
        values = gp.db_rw(sqlGetValues)

        for value in values:
            param = proto.Param.add()
            param.name = value[0]
            param.time = timeStandard
            param.value = str(value[1])

        kafkaProducer.send(topicName, proto.SerializeToString(), timeStandard)
        print("topic：%s；-time：%s" % (topicName, timeStandard))
    time.sleep(sleepTime)
    startTime = startTime + datetime.timedelta(minutes=5)
    if startTime >= endTime:
        startTime = baseStartTime
        endTime = baseEndTime
