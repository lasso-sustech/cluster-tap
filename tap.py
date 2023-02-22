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
from subprocess import CalledProcessError
import threading
import time
import traceback
from queue import Queue

SERVER_PORT = 11112
IPC_PORT    = 52525

GEN_TID = lambda: ''.join([random.choice(string.ascii_letters) for _ in range(8)])
SHELL_POPEN = lambda x: sp.Popen(x, stdout=sp.PIPE, stderr=sp.PIPE, shell=True)
SHELL_RUN = lambda x: sp.run(x, stdout=sp.PIPE, stderr=sp.PIPE, check=True, shell=True)

def _recv(sock:socket.socket, decoding:bool=True):
    _len = struct.unpack('I', sock.recv(4))[0]
    _msg = sock.recv(_len).decode()
    if decoding:
        _msg = json.loads(_msg)
        if 'err' in _msg: UntangledException(_msg['err'])
    return _msg

def _send(sock:socket.socket, msg, encoding:bool=True):
    _msg = json.dumps(msg).encode() if encoding else msg.encode()
    _len = struct.pack('I', len(_msg))
    sock.send( _len + _msg )

def _sync(sock:socket.socket, msg, encoding=True, decoding=True):
    _send(sock, msg, encoding)
    return _recv(sock, decoding)

def _execute(role, tasks, tid, config, params, timeout) -> None:
    try:
        exec_params = config['parameters'] if 'parameters' in config else {}
        exec_params.update(params)
        ##
        commands = config[f'{role}-commands'].copy()
        for i in range(len(commands)):
            for k,v in exec_params.items():
                commands[i] = commands[i].replace(f'${k}', str(v))
        ##
        processes = [ SHELL_POPEN(cmd) for cmd in commands ]
        returns = [None]
        _now = time.time()
        while None in returns and time.time() - _now < timeout+0.5:
            returns = [ proc.poll() for proc in processes ]
            time.sleep(0.1)
        ##
        for i,ret in enumerate(returns):
            if ret is None:
                processes[i].terminate()
                if role=='client':
                    raise ClientTimeoutException( f'[{i}]-th command failed to complete.' )
                elif role=='server':
                    raise ServerTimeoutException( f'[{i}]-th command failed to complete.' )
            if ret < 0:
                raise StdErrException( processes[i].stderr.read().decode() )
        outputs = { f'${role}_output_{i}' : repr(p.stdout.read().decode())
                        for i,p in enumerate(processes) }
        ##
        results = dict()
        for key,value in config['output'].items():
            if value['exec']==role:
                cmd = value['cmd']
                for k,o in outputs.items():
                    cmd = cmd.replace(k,o)
                ret = SHELL_RUN(cmd).stdout.decode()
                ret = re.findall(value['format'], ret)[0]
                results[key] = ret
    except Exception as e:
        tasks[tid]['results'] = { 'err': UntangledException.format(e) }
    else:
        tasks[tid]['results'] = results
    pass

class StdErrException(Exception): pass

class ClientTimeoutException(Exception): pass

class ClientNoResponseException(Exception): pass

class InvalidRequestException(Exception): pass

class AutoDetectFailureException(Exception): pass

class ClientConnectionLossException(Exception): pass

class ClientNotFoundException(Exception): pass

class ServerTimeoutException(Exception): pass

class ServerNoResponseException(Exception): pass

class UntangledException(Exception):
    def __init__(self, args):
        err_cls, err_msg = args
        raise eval(err_cls)(err_msg)
    
    @staticmethod
    def format(e:Exception):
        err_cls = type(e).__name__
        err_msg = str(e) + '\n' + traceback.format_exc()
        return (err_cls, err_msg)
    pass

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
        raise AutoDetectFailureException('No master found.')
        pass

    def handle(self, request, args):
        result = dict()
        try:
            if request=='describe':
                abstract = { k:v['description'] for k,v in self.manifest['functions'].items() }
                result = { 'name': self.manifest['name'], 'functions': abstract }
            elif request=='info' and 'function' in args:
                fn = args['function']
                result = self.manifest['functions'][fn]
            elif request=='execute' and 'function' in args:
                fn = args['function']
                config = self.manifest['functions'][fn]
                params  = args['parameters'] if 'parameters' in args else {}
                timeout = args['timeout'] if 'timeout' in args else 999
                tid = GEN_TID()
                _thread = threading.Thread(target=_execute, args=('client', self.tasks, tid, config, params, timeout))
                self.tasks[tid] = {'handle':_thread, 'results':''}
                _thread.start()
                result = { 'tid': tid }
            elif request=='fetch' and 'tid' in args:
                result = self.tasks[ args['tid'] ]['results']
                if not result:
                    raise ClientNoResponseException('Empty response.')
                self.tasks[ args['tid'] ]['handle'].join()
            else:
                raise InvalidRequestException(f'Request "{request}" is invalid.')
        except Exception as e:
            return { 'err': UntangledException.format(e) }
        else:
            return result
        pass

    def start(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.s_ip, self.s_port))
        ## initial register
        name = self.manifest['name']
        _send(sock, name)
        print( f'Client "{name}" is now on.' )
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

    def handle(self, name, conn, tx:Queue, rx:Queue):
        pair_tasks = dict()
        while True:
            try:
                cmd, args = rx.get()
                if cmd=='execute':
                    p_args = json.loads(args)['args']
                    params = p_args['parameters'] if 'parameters' in p_args else {}
                    timeout = p_args['timeout'] if 'timeout' in p_args else 999
                    _request = { 'request':'info', 'args':{'function':p_args['function']} }
                    config = _sync(conn, _request)
                    ##
                    s_tid = GEN_TID()
                    _thread = threading.Thread(target=_execute, args=('server', pair_tasks, s_tid, config, params, timeout))
                    pair_tasks.update({ s_tid : {'handle':_thread, 'results':''} })
                    _thread.start()
                    ##
                    res = _sync(conn, args, encoding=False)
                    tid = res['tid']
                    pair_tasks.update({ tid : s_tid })
                    tx.put( res )
                    pass
                elif cmd=='fetch':
                    _args = json.loads(args)
                    tid = _args['args']['tid']
                    client_results = _sync(conn, args, encoding=False)
                    ##
                    s_tid = pair_tasks[tid]
                    server_results = pair_tasks[s_tid]['results']
                    if not server_results:
                        raise ServerNoResponseException('Empty response.')
                    pair_tasks[s_tid]['handle'].join()
                    ##
                    tx.put({ **client_results, **server_results })
                else:
                    tx.put( _sync(conn, args, encoding=False) )
            except struct.error:
                e = ClientConnectionLossException(f'{name} disconnected.')
                tx.put({ 'err': UntangledException.format(e) })
                self.clients.pop(name)
                break
            except Exception as e:
                tx.put({ 'err': UntangledException.format(e) })
        pass

    def daemon(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(('', self.ipc_port))
        ##
        print('IPC Daemon is now on.')
        while True:
            msg, addr = sock.recvfrom(1024)
            cmd, args = msg.decode().split(maxsplit=1)
            cmd, client = cmd.split('@')
            ## blocking-mode
            if client=='' or cmd=='list_all':
                res = { k:v['addr'] for k,v in self.clients.items() }
            elif client not in self.clients:
                e = ClientNotFoundException(f'Client "{client}" not exists.')
                res = { 'err': UntangledException.format(e) }
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
        print('Server is now on.')
        while True:
            ## initial register
            conn, addr = sock.accept()
            name = _recv(conn)
            print(f'Client "{name}" connected.')
            ##
            tx, rx = Queue(), Queue()
            handler = threading.Thread(target=self.handle, args=(name, conn, rx, tx))
            self.clients.update({
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
    """The IPC broker used to communicate with the tap server, via UDP.
    
    Args:
        client (str): The client name. Leave empty to only query from server.
        addr (str): (Optional) Specify the IP address of the server, default as ''.
        port (int): (Optional) Specify the port of the server, default as 52525.
    """
    def __init__(self, client:str='', addr=None, port=None):
        self.client = client
        addr = addr if addr else ''
        port = port if port else IPC_PORT
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.connect((addr, port))
        pass

    def list_all(self) -> dict:
        """List all online clients."""
        return self.__send('list_all', '')

    def describe(self) -> dict:
        """Return the available functions on the connected client."""
        return self.__send('describe', '')
    
    def info(self, function:str) -> dict:
        """Return the details of the function on the connected client.
        
        Args:
            function (str): The function name.
        
        Returns:
            dict: The dictionary object contains full manifest of the function.
        """
        return self.__send('info', {'function':function})
    
    def execute(self, function:str, parameters:dict=None, timeout:float=None) -> str:
        """Execute the function asynchronously, return instantly with task id.
        
        Args:
            function (str): The function name.
            parameters (dict): The parameters provided for the function. The absent values will use the default values in the manifest.
            timeout (float): The longest time in seconds waiting for the outputs from function execution.
        
        Returns:
            str: The task ID.
        """
        args = {'function':function}
        if parameters: args['parameters'] = parameters
        if timeout: args['timeout'] = timeout
        res = self.__send('execute', args)
        return res['tid']
    
    def fetch(self, tid:str) -> dict:
        """Fetch the previous function execution results with task id.
        
        Args:
            tid (std): Task ID obtained from `Connector.execute`.
        
        Returns:
            dict: the output collected in dictionary struct.
        """
        return self.__send('fetch', {'tid':tid})

    def __send(self, cmd, args:str):
        """For internal usage, error handling."""
        ## format: '<cmd>@<client> <args>'
        args = {'request':cmd, 'args':args}
        msg = ' '.join([ f'{cmd}@{self.client}', json.dumps(args) ]).encode()
        self.sock.send(msg)
        ##
        res = self.sock.recv(4096).decode()
        res = json.loads(res)
        if 'err' in res: UntangledException(res['err'])
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
        raise Exception('Client manifest file loading failed.')
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
