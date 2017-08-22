#!/usr/bin/env python
# -*- coding:utf-8 -*-
'''
@author: alishtory
@site: https://github.com/alishtory
@file: ServerManager.py
@time: 2017/3/7 15:28
@description: 多用户版本中，启动、关闭、更新流量的管理模块
'''

import logging, threading
from abc import ABCMeta, abstractmethod
from server_pool import ServerPool
from shadowsocks import eventloop


class AbstractServerManager():

    __metaclass__ = ABCMeta
    last_enable_port_set = set() #上一次已经在服务的端口集合
    last_user_cfgs = {}  # 上一次用户服务配置
    spool = ServerPool.get_instance()
    last_transfer_stamps = {} #上次流量更新到的戳、标记
    event = threading.Event()

    @abstractmethod
    def update_transfer_fetch_users(self, curr_transfers):
     '''
     抽象方法，子类必须实现
     1. 更新流量的回调
        curr_transfers 格式如：
        {23123:(2398417,12094733),23127:(2873419,47231093),}  表示{端口:(上传,下载)}
     2. 要求：最新的提供服务的用户 配置信息（包括端口、密码、加密方式等等）
        返回值 user_cfgs 格式要求如：
        [{'port':23123, 'method':'rc4-md5', 'password':'I7S3dh2'}, {'port':23127, 'method':'chacha20', 'password':'2Di3h9'}]
     :return :
     '''
     pass

    def load_user_cfgs(self, user_cfg):
        default_cfgs = self.spool.config.copy()  # copy bug
        default_cfgs.update(user_cfg)
        return default_cfgs

    def loop_server(self):
        '''
        循环遍历，更新流量，加入最新的要服务的端口，如果配置更改，则更新；删除已移除的端口
        1. 如果端口已经在服务器中，则：
          1.1 如果这个端口的配置改变（比如，这个端口的密码改了），那么更新
          1.2 如果这个端口的配置没改变，则不做任何操作
        2. 如果这个端口不在服务中，则加入服务；
        3. 把以前在服务中，现在不在列表中的端口移出服务
        总流程：你向我汇报上一次使用的流量，我把接下来要服务的端口及配置给你。
        :return:
        '''
        curr_transfers = {}  # 当前流量使用情况
        last_transfer_stamps_ports = self.last_transfer_stamps.keys()
        #获取上一次更新流量到现在，所有节点使用的流量
        for port in self.last_enable_port_set:
            trans = self.spool.get_server_transfer(port) #端口使用的流量总计
            #1. 拿到上一次使用的戳
            if port in last_transfer_stamps_ports:
                last_trans_stamp = self.last_transfer_stamps[port]
            else:
                last_trans_stamp = (0, 0)
            #2. 本次流量到达点减去戳点，得到上次到本次之间，这个端口使用的流量
            trans_tuple = (trans[0]-last_trans_stamp[0], trans[1]-last_trans_stamp[1])
            if trans_tuple != (0, 0):
                curr_transfers[port] = trans_tuple

        logging.debug('curr_transfers is: %s' % curr_transfers)
        ucfgs = self.update_transfer_fetch_users(curr_transfers)
        enable_port_set = set()
        user_cfgs = dict()
        for ucfg in ucfgs:
            pt = ucfg.pop('port')
            enable_port_set.add(pt)
            user_cfgs[pt] = ucfg


        #更新流量成功之后，我们把流量戳改一下@@@@

        #1. 删除已经不在服务的端口
        disable_port_set = self.last_enable_port_set - enable_port_set
        for port in disable_port_set:
            logging.info("manager stop server at port [%s],because it's disable now" % port)
            self.spool.cb_del_server(port)
            #@@@@端口干掉后，流量戳也一起干掉
            del self.last_transfer_stamps[port]
        #2.1 更新修改了配置的端口
        in_service_port_set = enable_port_set & self.last_enable_port_set
        in_service_changedcfg_port_set = set()  #改变配置的端口集合
        for port in in_service_port_set:
            user_cfg = user_cfgs[port]
            if user_cfg != self.last_user_cfgs[port]:
                logging.info('manager stop server at port [%s] because config changed,then it will auto restart' % (port))
                self.spool.cb_del_server(port)
                in_service_changedcfg_port_set.add(port)
                # @@@@端口更新后，流量戳置零
                self.last_transfer_stamps[port] = (0, 0)
            else:
                # @@@@配置没有更改，那么更新流量使用戳点；告诉服务器流量更新后，把流量使用戳点改到最新的戳点
                trans = self.spool.get_server_transfer(port)
                self.last_transfer_stamps[port] = trans
        #3. 添加还不在服务的端口
        not_in_port_set = enable_port_set - self.last_enable_port_set
        for port in not_in_port_set:
            if port > 0 and port < 65536 :
                user_cfg = user_cfgs[port]
                cfg = self.load_user_cfgs(user_cfg)
                logging.info('manager start server at port [%s],config: %s' % (port, user_cfg))
                self.spool.new_server(port, cfg)
                # @@@@端口新开启后，流量戳置零
                self.last_transfer_stamps[port] = (0, 0)
            else:
                logging.error('port must between 1 and 65535')
        #2.2 要稍等一会儿，否则可能会出现无法重启，报端口占用错误
        self.event.wait(eventloop.TIMEOUT_PRECISION + eventloop.TIMEOUT_PRECISION / 2)
        for port in in_service_changedcfg_port_set:
            user_cfg = user_cfgs[port]
            logging.info('manager start server at port [%s] because config changed: %s' % (port, user_cfg))
            #self.spool.cb_del_server(port)
            cfg = self.load_user_cfgs(user_cfg)
            self.spool.new_server(port, cfg)

        #更新上次数据缓存
        self.last_enable_port_set = enable_port_set
        self.last_user_cfgs = user_cfgs

