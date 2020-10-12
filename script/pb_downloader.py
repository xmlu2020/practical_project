#!/usr/bin/python3
# coding: utf-8

from aibee_hdfs import hdfscli
import os
import subprocess
from tqdm import tqdm
import requests
import json
import time
from multiprocessing import Pool, cpu_count, Process

def init():
    keytab = './sjyw.keytab'
    username = 'sjyw'
    print("{}, {}".format(username, keytab))
    hdfscli.initKerberos(keytab, username)
    client = hdfscli.HdfsClient(user=username)
    return client


def get_undownloaded_file(hdfs_addr, local_dir, work_num=40):
    client = init()
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)
    total_pb_list = client.list(hdfs_addr)
    print("总共pb文件数：{}".format(len(total_pb_list)))

    local_pb_list = os.listdir(local_dir)
    print("已下载pb文件数：{}".format(len(local_pb_list)))

    undownload_pbs = set(total_pb_list) - set(local_pb_list)
    print("下载未完成的文件 {} 个 ...".format(len(total_pb_list) - len(local_pb_list)))
    # for pb in tqdm(undownload_pbs):
    #     client.download(os.path.join(hdfs_addr, pb), local_dir)

    args = []
    for pb in undownload_pbs:
        args.append([os.path.join(hdfs_addr, pb), local_dir])
    new_args = [args[i:i+work_num] for i in range(0, len(args), work_num)]

    for i in range(len(new_args)):
        process_list = []
        for j in range(len(new_args[i])):
            process_list.append(Process(target=client.download, args=new_args[i][j]))
        for process in process_list:
            process.start()
        for process in process_list:
            process.join()


def call_robot(hdfs_addr, host_name):
    url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=72e4830c-b5fd-442c-83bf-98af61997eea"
    parms = {
        "msgtype": "text",
        "text": {
            "content":
            """pb文件在重试下载10次后失败\n主机名：{},\n下载失败地址：{}
            """.format(host_name, hdfs_addr),
            "mentioned_list": [18801061291]
        }
    }
    requests.post(url, data=json.dumps(parms))


def download_pb_file(hdfs_path, local_path):
    host_name = os.system("hostname")
    # hdfs_addr = "/gz/prod/customer/GZCI/guangzhou/mowgz/on-premise/tracking/svonline/20200530/pb"
    # local_dir = "/ssd/ysqin/purity_merge/direct/GZCI_guangzhou_mowgz_20200530/pb"
    get_undownloaded_file(hdfs_path, local_path)

    count = 0
    try:
        print("hdfs_path:{}, local_path:{}".format(hdfs_path, local_path))
        get_undownloaded_file(hdfs_path, local_path)
    except:
        count += 1
        time.sleep(5)
        if count <= 10:
            get_undownloaded_file(hdfs_path, local_path)
        else:
            call_robot(hdfs_path, host_name)


if __name__ == '__main__':
    hdfs_addr = "hdfs地址"
    local_dir = "/mnt/data/pb"
    download_pb_file(hdfs_addr, local_dir)
