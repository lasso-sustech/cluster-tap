#!/usr/bin/env python3
import argparse
import ipaddress
import json
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

def _recv(sock:socket.socket):
    _len = struct.unpack('I', sock.recv(4))
    _msg = sock.recv(_len).decode()
    return json.loads(_msg)

def _send(sock:socket.socket, msg):
    _msg = json.dumps(msg).encode()
    _len = struct.pack('I', len(_msg))
    sock.send( _len + _msg )

def _sync(sock:socket.socket, msg):
    _send(sock, msg)
    return _recv(sock)

class SlaveDaemon:
    def __init__(self, manifest, s_port, s_ip=None):
        self.manifest = manifest
        self.s_port = s_port
        self.s_ip = s_ip if s_ip else self.discover()
        self.tasks = dict()
        pass
    
    def discover(self):
        o = SHELL_RUN('ip route | grep default').stdout.decode()
        iface_name = re.findall('default via (\S+) dev (\S+) .*', o)[0][1]
        o = SHELL_RUN('ip addr').stdout.decode()
        iface_network = re.findall(f'.+inet (\S+).+{iface_name}', o)[0]
        all_hosts = ipaddress.IPv4Network(iface_network, strict=False).hosts()
        ##
        print(f'Auto-detect master over {iface_name} ...')
        for _,host in enumerate(all_hosts):
            s_ip = str(host)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(0.1)
            if sock.connect_ex((s_ip, self.s_port)) == 0:
                self.s_ip = s_ip
                print(f'Find master on {s_ip}')
                break
        raise Exception('No master found.')
        pass

    def client_execute(self, tid, fn, params, timeout):
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

    def handle(self, request, args):
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
                _thread = threading.Thread(target=self.client_execute, args=(tid, args['function'], params, timeout)); _thread.start()
                self.tasks.update({ tid : {'handle':_thread, 'results':''} })
                result = { 'tid': tid }
            elif request=='fetch' and 'tid' in args:
                results = self.tasks[ args['tid'] ]['results']
                if not results:
                    raise Exception('Client: empty response.')
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
        ## initial register
        msg = self.manifest['name']
        _send(sock, msg)
        print( f'Client "{msg}" is now on.' )
        ##
        while True:
            msg = _recv(sock)
            res = self.handle(msg['request'], msg['args'])
            _send(sock, res)
        pass

    pass

class MasterDaemon:
    def __init__(self, port, ipc_port):
        self.port = port
        self.ipc_port = ipc_port
        self.clients = dict()
        pass

    def server_execute(self, tid, manifest, fn, params, timeout):
        try:
            config = manifest[fn]
            params = config['parameters'].update(params)
            commands = config['server-commands']
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
            outputs = { f'$server_output_{i}':o.decode() for i,o in enumerate(processes.stdout) }
            ##
            results = dict()
            for key,value in manifest['output'].items():
                if value['exec']=='server':
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

    def handle(self, name, conn, tx:Queue, rx:Queue):
        tasks = dict()
        try:
            while True:
                cmd, args = rx.get()
                if cmd=='execute':
                    _args = json.load(args)['args']
                    params = args['parameters'] if 'parameters' in args else {}
                    _request = { 'request':'info', 'args':{'function':args['function']} }
                    manifest = _sync(conn, _request)
                    ##
                    s_tid = GEN_TID()
                    _thread = threading.Thread(target=self.server_execute, args=(s_tid, manifest, args['function'], params)); _thread.start()
                    self.tasks.update({ s_tid : {'handle':_thread, 'results':''} })
                    ##
                    res = _sync(conn, args); tid = res['tid']
                    self.tasks.update({ tid : s_tid })
                    tx.put( res )
                    pass
                elif cmd=='fetch':
                    _args = json.load(args); tid = _args['args']['tid']
                    client_results = _sync(conn, args)
                    ##
                    s_tid = self.task[tid]
                    server_results = self.tasks[s_tid]['results']
                    if not server_results:
                        raise Exception('Server: empty response.')
                    self.tasks[tid]['handle'].join()
                    ##
                    tx.put({ **client_results, **server_results })
                else:
                    tx.put( _sync(conn, args) )
        except Exception: #FIXME: maybe broken connection, or not
            print(e)
            print(f'Connection loss: {name}.')
            self.clients.pop(name)
        pass

    def daemon(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(('', self.ipc_port))
        ##
        while True:
            msg, addr = sock.recvfrom(1024)
            cmd, args = msg.decode().split(maxsplit=1)
            cmd, client = cmd.split('@')
            ## blocking-mode
            if client=='' or cmd=='list_all':
                res = { k:v['addr'] for k,v in self.clients.items() }
            else:
                self.clients[client]['tx'].put((cmd, args))
                res = self.clients[client]['rx'].get()
            res = json.dumps(res).encode()
            sock.sendto(res, addr)
            pass
        pass

    def serve(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('0.0.0.0', self.port))
        sock.listen()
        ##
        while True:
            ## initial register
            conn, addr = sock.accept()
            name = _recv(sock)
            ##
            tx, rx = Queue(), Queue()
            handler = threading.Thread(target=self.handle, args=(name, conn, rx, tx))
            self.client.update({
                name:{'handler':handler,'conn':conn,'addr':addr,'tx':tx,'rx':rx} })
            handler.start()
        pass

    def start(self):
        self.server_thread = threading.Thread(target=self.serve)
        self.server_thread.start()
        self.daemon()
        pass

    pass

class Connector:
    def __init__(self, client:str='', port=None):
        self.client = client
        self.port = port if port else IPC_PORT
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.connect(('', self.port))
        pass

    def list_all(self):
        return self.__send('list_all', '')

    def describe(self):
        return self.__send('describe', '')
    
    def info(self, function:str):
        return self.__send('info', {'function':function})
    
    def execute(self, function, parameters:dict=None, timeout:float=None):
        args = {'function':function}
        if parameters: args['parameters'] = parameters
        if timeout: args['timeout'] = timeout
        return self.__send('execute', args)
    
    def fetch(self, tid):
        return self.__send('fetch', {'tid':tid})

    def __send(self, cmd, args:str):
        ## format: '<cmd>@<client> <args>'
        args = {'request':cmd, 'args':args}
        msg = ' '.join( f'{cmd}@{self.client}', json.dumps(args) )
        self.sock.send(msg)
        ##
        res = self.sock.recv().decode()
        res = json.loads(res)
        if 'err' in res:
            raise Exception(res['err'])
        return res

    pass

def master_main(args):
    master = MasterDaemon(args.port, args.ipc_port)
    master.start()
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
