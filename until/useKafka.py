# -*- coding:utf-8 -*-
# _author_ = 'zhangdeqiang'


import time
from proto import tag_pb2
from datetime import datetime
from kafka import KafkaProducer
from kafka import KafkaConsumer


# 处理kafka消费到的数据：protobuf反序列化，并处理成指定格式
def deal_with_consumer_data(msg):
    target = tag_pb2.interface_param()
    target.ParseFromString(msg)
    dict_result = {}
    time = target.Param[0].time
    time = time[:4] + "-" + time[4:6] + "-" + time[6:8] + " " + time[8:10] \
           + ":" + time[10:12] + ":" + time[12:14]
    dict_result['create_time'] = time
    for tar in target.Param:
        tag = str(tar.name).replace('.', '')
        if tag not in dict_result.keys():
            dict_result[tag] = tar.value
    return dict_result


class Kafka:
    def __init__(self, hosts, topicName):
        self.hosts = hosts
        self.topic = topicName
        self.kafkaProducer = KafkaProducer(bootstrap_servers=self.hosts)

    def producer(self, value, key=None, sleepTime=1):
        if key is None:
            key = str(datetime.now()).encode()
        self.kafkaProducer.send(self.topic, value, key)
        print("%s：插入kafka成功" % datetime.now())
        time.sleep(sleepTime)

    # 生产者关闭
    def producerClose(self):
        self.kafkaProducer.close()


def myKafkaConsumer(host, topic, format_value, conn):
    try:
        consumer = KafkaConsumer(topic, group_id='consumer-', bootstrap_servers=host,
                                 auto_offset_reset='smallest', enable_auto_commit=False)
        print("消费开始")
        for message in consumer:
            if message is not None:
                value = message.value
                format_value(value, conn)
    except Exception as e:
        print(e)
