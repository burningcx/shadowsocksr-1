#!/usr/bin/env python
# -*- coding:utf-8 -*-
'''
@author: alishtory
@site: https://github.com/alishtory
@file: xsadmin_server.py
@time: 2017/3/10 16:33
@description: 
'''

import config_xsadmin as config
import requests, hashlib
import time, random, logging
from ServerManager import AbstractServerManager


class XsadminServerManager(AbstractServerManager):
    def update_transfer_fetch_users(self, curr_transfers):
        response = post_api_request(json=curr_transfers)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            #不是200，出错了
            logging.info('api request code is%d\n%s'%(response.status_code, response.text))
            raise Exception('api return error')

def post_api_request(data= None,json= None,verify=False):
    headers = {'content-type': 'application/json'}
    headers['AUTHORIZATION']= signature_header()
    r = requests.post(config.API_URL, data=data, json=json, headers=headers,verify=verify)
    return r

def signature_header():
    m = hashlib.md5()
    timestamp = time.time()
    nonce_str = get_nonce_str()
    sign_str = '%s|%s|%s|%d' % (config.API_KEY, nonce_str, config.API_SECRET, timestamp)
    m.update(sign_str.encode())
    signature = m.hexdigest()
    return '%s|%s|%s|%d'%(config.API_KEY, nonce_str, signature, timestamp)

def get_nonce_str():
    return ''.join(random.sample('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789',10))

if __name__ == '__main__':
    manager = XsadminServerManager()
    while(True):
        try:
            manager.loop_server()
        except Exception as e:
            logging.error('loop happens error:%s'% e)
        time.sleep(config.API_INTERVAL)
