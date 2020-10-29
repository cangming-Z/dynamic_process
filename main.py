# -*- coding:utf-8 -*-
# _author_ = 'zhangdeqiang


import os
import sys
import time
import pymysql
import datetime
import configparser
from until import path
from proto import tag_pb2
from kafka import KafkaProducer
from multiprocessing import Process
from until.getMesInfo import MesData
from until.getdb import get_postgresql_conn


# 获取项目根目录
filepath = path.get_obj_path()
config = configparser.ConfigParser()
filePath = path.sep.join([filepath, 'config', 'detail.ini'])
treadNames = None
currentChildProcess = {}


# 关闭指定进程
def killProcess(processDetail):
    for key in processDetail:
        # Windows系统
        cmd = 'taskkill /pid ' + str(processDetail[key]) + ' /f'
        print("cmd指令%s" % cmd)
        try:
            print("子进程：%s(pid：%s)关闭中..." % (key, processDetail[key]))
            p = os.popen(cmd)
        except Exception as e:
            print("子进程：%s(pid：%s)关闭错误：\n%s" % (key, processDetail[key], e))
        print()


# 读取配置文件，获取配置的进程列表
def readConfig(sleepTime, firstTreadNames):
    try:
        global treadNames  # 全局变量（进程名称）
        startTime = datetime.datetime.now()
        lastTreadNames = firstTreadNames  # 上一次

        while True:
            if datetime.datetime.now().strftime('%Y-%m-%d %H-%M-%S') \
                    == startTime.strftime('%Y-%m-%d %H-%M-%S'):
                # 从配置文件中读取当前项目信息
                config.read(filePath, encoding='utf-8')
                treadNames = eval(config["work"]["runProject"])
                print("*---------\n%s：\n读取配置文件" % startTime)
                for treadName in treadNames:
                    if treadName not in lastTreadNames:
                        # 新增进程
                        print("配置文件更改-新增进程：%s" % treadName)
                        childProcess = Process(target=work, args=(treadName,))
                        childProcess.start()
                        currentChildProcess[treadName] = childProcess.pid
                for lastTreadName in lastTreadNames:
                    if lastTreadName not in treadNames:
                        # 删除进程
                        killDetail = {lastTreadName: currentChildProcess[lastTreadName]}
                        killProcess(killDetail)
                        print("配置文件更改-删除进程：%s" % lastTreadName)
                print("配置文件读取结束，进程更新结束，当前进程：%s\n*---------"
                      % str(treadNames))
                lastTreadNames = treadNames
                startTime += datetime.timedelta(seconds=sleepTime)
    except Exception as e:
        print("读取配置文件，配置进程列表出错：%s" % e)
        sys.exit()


# kafka生产者进程（槐坎南方、大冶尖峰、泉头水泥）
def work(project):
    try:
        print("进程%s开始" % project)
        # 从配置文件中读取当前项目信息
        config.read(filePath, encoding='utf-8')

        kafkaUrl = config[project]['kafkaUrl']
        topicName = config[project]["topicName"]
        host = config[project]["host"]
        port = eval(config[project]["port"])
        user = config[project]["user"]
        passwd = config[project]["passwd"]
        db = config[project]["db"]
        sleepTime = eval(config[project]["sleepTime"])

        # 槐坎南方有真实数据
        if project == "hknf-real":
            mergeTable = config[project]["mergeTable"]
            st = config[project]["startTime"]
            startTime = datetime.datetime.strptime(st, "%Y-%m-%d %H:%M:%S")
            realHisData(project, host, port, user, passwd, db,
                        kafkaUrl, topicName, sleepTime, mergeTable, startTime)
        # 泉头水泥、大冶尖峰等用假数据，递增1
        else:
            simulateData(project, host, port, user, passwd, db,
                         kafkaUrl, topicName, sleepTime)

    except Exception as e:
        print("%s" % e)
        sys.exit()


# 泉头水泥、大冶尖峰用假数据，递增1
def simulateData(project, host, port, user, passwd, db, kafkaUrl, topicName, sleepTime):
    try:
        conn = pymysql.Connect(host=host, port=port, user=user,
                               passwd=passwd, db=db)
        db = MesData(conn)

        sql = 'select distinct tag from tag_info where del_flag=0 and tag is not NULL'
        data = db.db_rw(sql)
        if data is not None and data[0][0] is not None:
            tags = data
        else:
            print('数据库：byt_mes_bigdata；表：tag_info无数据')
            sys.exit()
        kafkaProducer = KafkaProducer(bootstrap_servers=kafkaUrl)
        number = 0
        startTime = datetime.datetime.now()

        while True:
            if datetime.datetime.now().strftime('%Y-%m-%d %H-%M-%S') \
                    == startTime.strftime('%Y-%m-%d %H-%M-%S'):
                timeStandard = datetime.datetime.now().strftime("%Y%m%d%H%M%S").encode()
                proto = tag_pb2.interface_param()
                for value in tags:
                    param = proto.Param.add()
                    param.name = value[0]
                    param.time = timeStandard
                    param.value = str(number)
                kafkaProducer.send(topicName, proto.SerializeToString(), timeStandard)
                number += 1
                startTime += datetime.timedelta(seconds=sleepTime)
                print("*-*project：%s；topic：%s；-time：%s*-*" % (project, topicName, timeStandard))
    except Exception as e:
        print("%s" % e)
        sys.exit()


# 槐坎南方有真实数据
def realHisData(project, host, port, user, passwd, db, kafkaUrl,
                topicName, sleepTime, mergeTable, startTime):
    try:
        conn = get_postgresql_conn(host, port, user, passwd, db)
        gp = MesData(conn)
        kafkaProducer = KafkaProducer(bootstrap_servers=kafkaUrl)

        # 预先准备的真实数据开始结束时间
        sqlTimeLimit = 'select min(time),max(time) from "%s"' % mergeTable
        a = gp.db_rw(sqlTimeLimit)
        minTime = a[0][0]
        maxTime = a[0][1]

        timerStart = datetime.datetime.now()  # 计时器开始时间

        print("%s真实数据开始时间：%s" % (project, startTime))
        while True:
            if datetime.datetime.now().strftime('%Y-%m-%d %H-%M-%S') \
                    == timerStart.strftime('%Y-%m-%d %H-%M-%S'):
                timeStandard = timerStart.strftime("%Y%m%d%H%M%S").encode()

                proto = tag_pb2.interface_param()
                sqlGetValues = "select tag,value from %s where time between '%s' and '%s'" \
                               % (mergeTable, startTime,
                                  (startTime + datetime.timedelta(minutes=4)))
                values = gp.db_rw(sqlGetValues)

                for value in values:
                    param = proto.Param.add()
                    param.name = value[0]
                    param.time = timeStandard
                    param.value = str(value[1])
                kafkaProducer.send(topicName, proto.SerializeToString(), timeStandard)
                startTime += datetime.timedelta(seconds=300)
                timerStart += datetime.timedelta(seconds=sleepTime)

                if startTime > maxTime:
                    startTime = minTime

                print("#-#project：%s；topic：%s；-time：%s#-#" % (project, topicName, timeStandard))
    except Exception as e:
        print("%s出错：%s" % (project, e))
        sys.exit()


if __name__ == '__main__':
    t1 = Process(target=readConfig, args=(10, []))
    t1.start()
    print("This is main function at：%s" % time.time())
