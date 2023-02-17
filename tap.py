#!/usr/bin/env python3
import argparse
import ipaddress
import json
import netifaces as ni
import random
import re
import socket
import string
import struct
import subprocess as sp
import time
import threading
from queue import Queue

SERVER_PORT = 11112
IPC_PORT    = 52525

GEN_TID = lambda _: ''.join([random.choice(string.ascii_letters) for _ in range(8)])
SHELL_POPEN = lambda x: sp.Popen(x, stdout=sp.PIPE, stderr=sp.PIPE, shell=True)
SHELL_RUN = lambda x: sp.run(x, stdout=sp.PIPE, stderr=sp.PIPE, check=True, shell=True)

class SlaveDaemon:
    def __init__(self, manifest, s_port, s_ip=None):
        self.manifest = manifest
        self.s_port = s_port
        self.s_ip = s_ip if s_ip else self.discover()
        self.tasks = dict()
        pass
    
    def discover(self):
        default_gateway = next(iter( ni.gateways()['default'] ))
        _, iface_name = ni.gateways()['default'][default_gateway]
        iface = ni.ifaddresses(iface_name)[default_gateway][0]
        all_hosts = ipaddress.IPv4Network((iface['addr'], iface['netmask']), strict=False).hosts()
        ##
        print(f'Auto-detect master over {iface_name} ...')
        for _,host in enumerate(all_hosts):
            s_ip = str(host)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(0.05)
            if sock.connect_ex((s_ip, self.s_port)) == 0:
                self.s_ip = s_ip
                print(f'Find master on {s_ip}')
                break
        raise Exception('No master found.')
        pass

    def execute(self, tid, fn, params, timeout):
        try:
            config = self.manifest[fn]
            params = config['parameters'].update(params)
            commands = config['client-commands']
            for i in range(len(commands)):
                for k,v in params.items():
                    commands[i].replace(f'${k}', v)
            ##
            _now = time.time()
            processes = [ SHELL_POPEN(cmd) for cmd in commands ]
            returns = [None]
            while None in returns and time.time() - _now < timeout:
                returns = [ proc.poll() for proc in processes ]
                time.sleep(0.1)
            ##
            for i,ret in enumerate(returns):
                if ret < 0:
                    raise Exception( processes[i].stderr.decode() )
            outputs = { f'$client_output_{i}':o.decode() for i,o in enumerate(processes.stdout) }
            ##
            results = dict()
            for key,value in self.manifest['output'].items():
                if value['exec']=='client':
                    cmd = value['cmd']
                    [ cmd.replace(k,o) for k,o in outputs.items() ]
                ret = SHELL_RUN(cmd).stdout.decode()
                ret = re.findall(value['format'], ret)[0]
                results[key] = ret
        except Exception as e:
            self.tasks[tid]['results'] = str(e)
        else:
            self.tasks[tid]['results'] = results
        pass

    def handle_request(self, request, args):
        result = dict()
        try:
            if request=='describe':
                abstract = { k:v['description'] for k,v in self.manifest.functions.items() }
                result = { 'name': self.manifest['name'], 'functions': abstract }
            elif request=='info' and 'function' in args:
                fn = args['function']
                result = self.manifest['functions'][fn]
            elif request=='execute' and 'function' in args:
                params  = args['parameters'] if 'parameters' in args else {}
                timeout = args['timeout'] if 'timeout' in args else -1
                tid = GEN_TID()
                _thread = threading.Thread(target=self.execute, args=(tid, args['function'], params, timeout)); _thread.start()
                self.tasks.update({ tid : {'handle':_thread, 'results':''} })
                result = { 'tid': tid }
            elif request=='fetch' and 'tid' in args:
                results = self.tasks[ args['tid'] ]['results']
                if not results:
                    raise Exception('Empty response.')
                self.tasks[ args['tid'] ]['handle'].join()
            else:
                raise Exception('Invalid Request.')
        except Exception as e:
            return { 'err': str(e) }
        else:
            return result.update({'err':''})
        pass

    def start(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.s_ip, self.s_port))
        ##
        while True:
            _len = struct.unpack('I', sock.recv(4))
            _msg = sock.recv(_len).decode()
            _msg = json.loads(_msg)
            ##
            result = self.handle_request(_msg['request'], _msg['args'])
            _msg = json.dumps(result).encode()
            ##
            _len = struct.pack('I', len(_msg))
            _msg = _len + _msg
            sock.send(_msg)
        pass

    pass

class MasterDaemon:
    def __init__(self, port, tx, rx):
        self.port = port
        self.tx, self.rx = tx, rx
        pass

    def start(self):
        pass
    pass

class IPCDaemon:
    def __init__(self, port, tx, rx):
        self.port = port
        self.tx, self.rx = tx, rx
        pass

    def start(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(('', self.port))
        while True:
            cmd, args = sock.recv(1024).decode().split(maxsplit=1)
            params = dict([ params.split('=') for param in args.split('') ])
    pass

class Connector:
    def __init__(self, client:str='', port=None):
        self.client = client
        self.port = port if port else IPC_PORT
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect(('', self.port))
        pass

    def describe(self):
        return self.__send('describe')
    
    def info(self, function:str):
        return self.__send('info', {'function':function})
    
    def execute(self, function, parameters:dict=None, timeout:float=None):
        msg = {'function':function}
        if parameters: msg['parameters'] = parameters
        if timeout: msg['timeout'] = timeout
        return self.__send('execute', msg)
    
    def fetch(self, tid):
        return self.__send('fetch', {'tid':tid})

    def __send(self, cmd, args): #FIXME: need also send client name
        msg = {'request':cmd, 'args':args}
        msg = json.dumps(msg).encode()
        ##
        _len = struct.pack('I', len(msg))
        msg = _len+msg
        self.sock.send(msg)
        ##
        _len = struct.unpack('I', self.sock.recv(4))
        msg = self.sock.recv(_len).decode()
        msg = json.loads(msg)
        pass
    
    def __connect(self, client):
        pass

    pass


    
    pass

def master_main(args):
    req_q, res_q = Queue(), Queue()
    master = MasterDaemon(args.port, res_q, req_q)
    daemon = IPCDaemon(args.ipc_port, req_q, res_q)
    master.start(); daemon.start()
    pass

def slave_main(args):
    try:
        manifest = open('./manifest.json')
        manifest = json.load( manifest )
    except:
        raise Exception('Client manifest file load failure.')
    ##
    slave = SlaveDaemon(manifest, args.port, args.client)
    slave.start()
    pass

def main():
    parser = argparse.ArgumentParser(description='All-in-one cluster control tap.')
    parser.add_argument('-p', '--port', type=int, nargs='?', default=SERVER_PORT, help='(Optional) server port.')
    ##
    s_group = parser.add_argument_group('Server specific')
    s_group.add_argument('-s', '--server', action='store_true', help='run in server mode.')
    s_group.add_argument('--ipc-port', type=int, nargs='?', default=IPC_PORT, help='(Optional) external IPC port.')
    ##
    c_group = parser.add_argument_group('Client specific')
    c_group.add_argument('-c', '--client', type=str, default='', nargs='?', help='run in client mode.')
    ##
    args = parser.parse_args()
    if args.client or args.client==None:
        slave_main(args)
    elif args.server:
        master_main(args)
    else:
        print('Please specify client mode or server mode.')
    pass

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        raise e #pass
    finally:
        pass #exit()
