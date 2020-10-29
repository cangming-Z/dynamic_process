# -*- coding:utf-8 -*-
# _author_ = 'zhangdeqiang'


import os
sep = os.path.sep  # 当前系统分隔符


def get_obj_path():
    #  获取工程根路径
    filePath = os.path.dirname(__file__)  # 获取当前文件路径
    filePathList = filePath.replace('/', sep).split(sep)   # 更换文件路径分隔符，并按分隔符分割
    obj_path = sep.join(filePathList[:len(filePathList)-1])   # 获取工程根路径
    return obj_path


if __name__ == '__main__':
    print(get_obj_path())
