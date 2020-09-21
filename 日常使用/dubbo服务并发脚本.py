# -*- coding: utf-8 -*-
# @Time    : 2020/9/21 13:51
# @Author  : grassroadsZ
# @File    : dubbo服务并发脚本.py


import concurrent.futures
import json
import random
import telnetlib
import threading
import urllib

from kazoo.client import KazooClient
from loguru import logger


class DubboStressTest(object):
    """
    基于telnetlib.Telnet 对dubbo服务进行单个请求测试或者并发测试

    通过连接zk 根据接口名
    """
    _services_info = {}

    _lock_1 = threading.Lock()
    _lock_2 = threading.Lock()
    _instance = None
    __init_flag = False

    def __new__(cls, *args, **kwargs):
        if cls._instance:
            return cls._instance
        with cls._lock_1:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
            return cls._instance

    def __init__(self, zk_service, interface_name):
        """
        :param zk_service:
        :param interface_name:
        """
        self._zk_service = zk_service
        self._interface = interface_name
        if self.__init_flag:
            return

        with self._lock_2:
            if self.__init_flag:
                return

            self._services_info = self._get_dubbo_services_info()
            self.__init_flag = True

    def _get_dubbo_services_info(self):
        zk = KazooClient(hosts="{}".format(self._zk_service))
        zk.start()
        urls = []
        service_list = zk.get_children("dubbo")
        for i in service_list:
            if self._interface in i:
                try:
                    # 获取服务发布方
                    providers = zk.get_children("/dubbo/{}/providers".format(i))
                    if providers:
                        for provider in providers:
                            url = urllib.parse.unquote(provider)
                            if url.startswith('dubbo:'):
                                urls.append(url.split('dubbo://')[1])
                except Exception as error:
                    print(f"从{self._zk_service} 获取 {self._interface} 服务的提供者失败, {error}")
        paths = []
        for i in urls:
            try:
                path, temp = i.split('/')
                service = temp.split('?')[0]
                method = temp.split('methods=')[1].split('&')[0].split(',')
                paths.append(path)
            except Exception as e:
                print(f"获取接口 {self._interface} 的信息连接信息失败，{e}")
        services = {"service": service, "paths": paths, "method": method}
        print(f"接口{self._interface}对应的服务信息为{services}")
        return services

    @logger.catch
    def requests_dubbo(self, method, param):
        """
        请求dubbo接口
        :param method: dubbo接口的方法
        :param param: 请求参数
        :return:
        """
        res = self._services_info
        methods = res.get("method")
        if method not in methods:
            raise NameError(f"{method} not in {methods}")

        paths = res.get("paths")

        if len(paths) > 1:
            paths = paths[random.randint(0, len(paths) - 1)]
            # paths = paths[-1]
        else:
            paths = paths[0]

        ip, port = paths.split(":")

        con = Dubbo(ip, port)
        result = con.invoke(self._interface, method, param)
        print(f"{threading.Thread().getName()} -- 向ip:{ip},port:{port}的服务发起invoke 命令请求结果为：{result}")
        return result

    def run(self, method, data_list, num=5):
        # Todo: num 好像没有控制线程数，线程数由data_list的长度控制的
        with concurrent.futures.ThreadPoolExecutor(max_workers=num) as executor:
            to_do = []
            for data in data_list:
                future = executor.submit(self.requests_dubbo, method, data)
                to_do.append(future)
            for future in concurrent.futures.as_completed(to_do):
                future.result()


class Dubbo(telnetlib.Telnet):
    prompt = 'dubbo>'
    coding = 'utf-8'

    def __init__(self, host=None, port=0, timeout=10):
        super().__init__(host, port, timeout)
        self.write(b'\n')

    def command(self, flag, str_=""):
        data = self.read_until(flag.encode())
        self.write(str_.encode() + b"\n")
        return data

    def invoke(self, service_name, method_name, arg):
        arg_str = None

        if isinstance(arg, dict):
            arg_str = json.dumps(arg)
        if isinstance(arg, tuple):
            arg_str = str(arg).replace("(", "").replace(")", "")

        command_str = "invoke {0}.{1}({2})".format(service_name, method_name, arg_str)

        self.command(Dubbo.prompt, command_str)
        data = self.command(Dubbo.prompt, "")
        data = data.decode("utf-8", errors='ignore').split('\n')[1].strip()

        return data


if __name__ == '__main__':

    t = {"outerAccountType": "01", "fitmentContratNo": "C20092100001", "orgNo": "100002", "isStaff": "",
         "outerAccountName": "店铺", "outerAccountId": "D200918C6503", "registerFlow": ""}
    data = [t for i in range(30)
            ]
    dubbo = DubboStressTest(zk_service='zk-test.',
                            interface_name='com.bnq.accm.client.service.OuterAccountService')
    dubbo.run(data_list=data, method='outerAccountRegister')
