#!/usr/bin/env python3
from abc import abstractmethod
import argparse
import ipaddress
import json
import random
import re
import socket
import string
import struct
import subprocess as sp
import threading
import time
import traceback
from queue import Queue

SERVER_PORT = 11112
IPC_PORT    = 52525

GEN_TID = lambda: ''.join([random.choice(string.ascii_letters) for _ in range(8)])
SHELL_POPEN = lambda x: sp.Popen(x, stdout=sp.PIPE, stderr=sp.PIPE, shell=True)
SHELL_RUN = lambda x: sp.run(x, stdout=sp.PIPE, stderr=sp.PIPE, check=True, shell=True)

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

def _recv(sock:socket.socket):
    _len = struct.unpack('I', sock.recv(4))[0]
    _msg = sock.recv(_len).decode()
    _msg = json.loads(_msg)
    # if 'err' in _msg: UntangledException(_msg['err'])
    return _msg

def _send(sock:socket.socket, msg, encoding:bool=True):
    _msg = json.dumps(msg).encode() if encoding else msg.encode()
    _len = struct.pack('I', len(_msg))
    sock.send( _len + _msg )

def _sync(sock:socket.socket, msg, encoding=True, decoding=True):
    _send(sock, msg, encoding)
    return _recv(sock)

def _extract(cmd:str, format:str):
    try:
        ret = SHELL_RUN(cmd).stdout.decode()
    except sp.CalledProcessError as e:
        raise StdErrException( e.stderr.decode() )
    else:
        ret = re.findall(format, ret)
        ret = [ x for x in ret if x ]
        if len(ret)==0: ret=''
        if len(ret)==1: ret=str(ret[0])
    return ret

def _execute(role, tasks, tid, config, params, timeout) -> None:
    try:
        timeout = timeout if timeout>=0 else 999
        exec_params = config['parameters'] if 'parameters' in config else {}
        exec_params.update(params)
        ##
        if f'{role}-commands' not in config:
            return
        commands = config[f'{role}-commands'].copy()
        for i in range(len(commands)):
            for k,v in exec_params.items():
                commands[i] = commands[i].replace(f'${k}', str(v))
        ##
        processes = [ SHELL_POPEN(cmd) for cmd in commands ]
        returns = [ proc.poll() for proc in processes ]
        _now = time.time()
        while None in returns and time.time() - _now < timeout:
            returns = [ proc.poll() for proc in processes ]
            time.sleep(0.1)
        ##
        for i,ret in enumerate(returns):
            if ret is None:
                processes[i].kill()
                if role=='client':
                    raise ClientTimeoutException( f'[{i}]-th {role} command.' )
                elif role=='server':
                    raise ServerTimeoutException( f'[{i}]-th {role} command.' )
            ##
            if ret != 0:
                _stderr = processes[i].stderr; assert(not _stderr==None)
                raise StdErrException( _stderr.read().decode() )
        ##
        outputs = dict()
        for i,p in enumerate(processes):
            _stdout = p.stdout; assert(not _stdout==None)
            outputs.update({ f'${role}_output_{i}' : repr(_stdout.read().decode())  })
        ##
        results = dict()
        for key,value in config['output'].items():
            if value['exec']==role:
                cmd, _format = value['cmd'], value['format']
                for k,o in outputs.items():
                    cmd = cmd.replace(k,o)
                for k,v in exec_params.items():
                    cmd = cmd.replace(f'${k}', str(v))
                results[key] = _extract(cmd, _format)
    except Exception as e:
        tasks[tid]['results'] = { 'err': UntangledException.format(e) }
    else:
        tasks[tid]['results'] = results
    pass

class Request:
    def __init__(self, handler):
        self.handler = handler
    
    def console(self, args:dict) -> dict:
        '''Default console behavior: [console] <--(bypass)--> [server].'''
        _request = type(self).__name__
        args = {'request':_request, 'args':args}
        msg = '{request} {client}@{args}'.format(
                request=_request, client=self.handler.client, args=json.dumps(args) ).encode()
        self.handler.sock.send(msg)
        ##
        res = self.handler.sock.recv(4096).decode()
        res = json.loads(res)
        if 'err' in res: UntangledException(res['err'])
        return res

    def server(self, args:str) -> dict:
        '''Default server behavior: [server] <--(bypass)--> [proxy].'''
        _request = type(self).__name__
        client, args = args.split('@', maxsplit=1)
        try:
            client = self.handler.clients[client]
        except:
            e = ClientNotFoundException(f'Client "{client}" not exists.')
            res = { 'err': UntangledException.format(e) }
        else:
            client['tx'].put((_request, args))
            res = client['rx'].get()
        return res
    
    def proxy(self, conn, _tasks:dict, args:dict) -> dict:
        '''Default proxy behavior: [proxy] <--(bypass)--> [client].'''
        return _sync(conn, args, encoding=False)
    
    @abstractmethod
    def client(self, args:dict) -> dict: pass
    pass

class Handler:
    ## console <--> server <--> proxy <--> client
    def handle(self, request:str, args) -> dict:
        try:
            handler = getattr(Handler, request)(self)
        except:
            raise InvalidRequestException(f'Request "{request}" is invalid.')
        ##
        if isinstance(self,MasterDaemon):
            return handler.server(args)
        if isinstance(self,SlaveDaemon):
            return handler.client(args)
        if isinstance(self,Connector):
            return handler.console(args)
        return dict()

    def proxy(self, client:dict, request:str, args:dict) -> dict:
        handler = getattr(self, request)(self)
        conn, tasks = client['conn'], client['tasks']
        return handler.proxy(conn, tasks, args)

    class list_all(Request):
        def server(self, _args):
            return  { k:v['addr'] for k,v in self.handler.clients.items() }
        pass

    class describe(Request):
        def client(self, _args):
            return { k:v['description'] for k,v in self.handler.manifest['functions'].items() }
        pass

    class info(Request):
        def client(self, args):
            fn = args['function']
            return self.handler.manifest['functions'][fn]
        pass

    class execute(Request):
        def server(self, args: str) -> dict:
            return super().server(args)

        def proxy(self, conn, tasks, args):
            ## load config from client
            p_args = json.loads(args)['args']
            params, timeout, fn = p_args['params'], p_args['timeout'], p_args['function']
            _request = { 'request':'info', 'args':{'function':fn} }
            config = _sync(conn, _request)
            ## execute at server-side
            if config['role']=='client':
                res = super().proxy(conn, tasks, args)
            else:
                s_tid = GEN_TID()
                _thread = threading.Thread(target=_execute, args=('server', tasks, s_tid, config, params, timeout))
                tasks.update({ s_tid : {'handle':_thread, 'results':{}} })
                _thread.start()
                ## use default bypass behavior
                res = super().proxy(conn, tasks, args)
                ## bind {tid, s_tid} map
                tid = res['tid']
                tasks.update({ tid : s_tid })
            return res

        def client(self, args):
            params, timeout, fn = args['params'], args['timeout'], args['function']
            config = self.handler.manifest['functions'][fn]
            ##
            tid = GEN_TID()
            _thread = threading.Thread(target=_execute, args=('client', self.handler.tasks, tid, config, params, timeout))
            self.handler.tasks[tid] = {'handle':_thread, 'results':{}}
            _thread.start()
            return { 'tid': tid }
        pass

    class fetch(Request):
        def proxy(self, conn, tasks, args):
            ## get s_tid with tid
            args = json.loads(args)
            tid = args['args']['tid']
            ##
            if tid in tasks:
                s_tid = tasks[tid]
                ## use default bypass behavior
                client_results = super().proxy(conn, tasks, args)
            
            ## fetch server-side results
            server_results = tasks[s_tid]['results']
            if not server_results:
                raise ServerNoResponseException('Empty response.')
            tasks[s_tid]['handle'].join()
            ##
            return { **client_results, **server_results }
        
        def client(self, args):
            res = self.handler.tasks[ args['tid'] ]['results']
            if not res:
                raise ClientNoResponseException('Empty response.')
            self.handler.tasks[ args['tid'] ]['handle'].join()
            return res
        pass

    pass

class SlaveDaemon(Handler):
    def __init__(self, manifest:dict, port:int, addr:str=''):
        self.manifest = manifest
        self.sock = None
        self.port = port
        self.addr = addr if addr else self.auto_detect()
        self.tasks = dict()
        pass
    
    def auto_detect(self) -> str:
        o = SHELL_RUN('ip route | grep default').stdout.decode()
        iface_name = re.findall('default via (\\S+) dev (\\S+) .*', o)[0][1]
        o = SHELL_RUN('ip addr').stdout.decode()
        iface_network = re.findall(f'.+inet (\\S+).+{iface_name}', o)[0]
        all_hosts = ipaddress.IPv4Network(iface_network, strict=False).hosts()
        ##
        print(f'Auto-detect master over {iface_name} ...')
        for _,host in enumerate(all_hosts):
            addr = str(host)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(0.1)
            if sock.connect_ex((addr, self.port)) == 0:
                print(f'Find master on {addr}')
                self.sock = sock
                sock.settimeout(None)
                return addr
        raise AutoDetectFailureException('No master found.')

    def start(self):
        if not self.sock:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((self.addr, self.port))
        ## initial register
        name = self.manifest['name']
        _send(self.sock, name)
        print( f'Client "{name}" is now on.' )
        ##
        while True:
            try:
                msg = _recv(self.sock)
                res = self.handle(msg['request'], msg['args'])
            except Exception as e:
                err = { 'err': UntangledException.format(e) }
                _send(self.sock, err)
            else:
                _send(self.sock, res)
        pass

    pass

class MasterDaemon(Handler):
    def __init__(self, port:int, ipc_port:int):
        self.port = port
        self.ipc_port = ipc_port
        self.clients = dict()
        pass

    def proxy_service(self, name, tx:Queue, rx:Queue):
        while True:
            try:
                res = self.proxy( self.clients[name], *rx.get() )
            except struct.error:
                e = ClientConnectionLossException(f'{name} disconnected.')
                tx.put({ 'err': UntangledException.format(e) })
                self.clients.pop(name)
                break
            except Exception as e:
                tx.put({ 'err': UntangledException.format(e) })
            else:
                tx.put( res )
        pass

    def daemon(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(('', self.ipc_port))
        ##
        print('IPC Daemon is now on.')
        while True:
            msg, addr = sock.recvfrom(1024)
            cmd, args = msg.decode().split(maxsplit=1)
            ##
            res = self.handle(cmd, args)
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
            handler = threading.Thread(target=self.proxy_service, args=(name, rx, tx))
            self.clients.update({
                name:{'handler':handler,'conn':conn,'tasks':{},'addr':addr,'tx':tx,'rx':rx} })
            handler.start()
        pass

    def start(self):
        self.server_thread = threading.Thread(target=self.serve)
        self.server_thread.start()
        self.daemon()
        pass

    pass

class Connector(Handler):
    """The IPC broker used to communicate with the tap server, via UDP.
    
    Args:
        client (str): The client name. Leave empty to only query from server.
        addr (str): (Optional) Specify the IP address of the server, default as ''.
        port (int): (Optional) Specify the port of the server, default as 52525.
    """
    def __init__(self, client:str='', addr:str='', port:int=0):
        self.client = client
        addr = addr if addr else ''
        port = port if port else IPC_PORT
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.connect((addr, port))
        pass

    def list_all(self) -> dict:
        """List all online clients."""
        return self.handle('list_all', {})

    def describe(self) -> dict:
        """Return the available functions on the connected client."""
        return self.handle('describe', {})
    
    def info(self, function:str) -> dict:
        """Return the details of the function on the connected client.
        
        Args:
            function (str): The function name.
        
        Returns:
            dict: The dictionary object contains full manifest of the function.
        """
        return self.handle('info', {'function':function})
    
    def execute(self, function:str, parameters:dict={}, timeout:float=-1) -> str:
        """Execute the function asynchronously, return instantly with task id.
        
        Args:
            function (str): The function name.
            parameters (dict): The parameters provided for the function. The absent values will use the default values in the manifest.
            timeout (float): The longest time in seconds waiting for the outputs from function execution.
        
        Returns:
            str: The task ID.
        """
        args = { 'function':function, 'parameters':parameters, 'timeout':timeout }
        res = self.handle('execute', args)
        return res['tid']
    
    def fetch(self, tid:str) -> dict:
        """Fetch the previous function execution results with task id.
        
        Args:
            tid (std): Task ID obtained from `Connector.execute`.
        
        Returns:
            dict: the output collected in dictionary struct.
        """
        return self.handle('fetch', {'tid':tid})

    pass

def master_main(args):
    master = MasterDaemon(args.port, args.ipc_port)
    master.start()
    pass

def slave_main(args):
    manifest = open('./manifest.json')
    manifest = json.load( manifest )
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
