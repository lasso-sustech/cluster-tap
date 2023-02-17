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


SERVER_PORT = 11112
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
            _len = len(_msg)
            ##
            sock.send( struct.pack('I', _len) )
            sock.send(_msg)
        pass

    pass

def main():
    parser = argparse.ArgumentParser(description='The slave program of cluster-tap.')
    parser.add_argument('-c', '--master', type=str, nargs='?', help='(Optional) master address.')
    parser.add_argument('-p', '--port', type=int, nargs='?', default=SERVER_PORT, help='(Optional) master port.')
    args = parser.parse_args()
    ##
    try:
        manifest = open('./manifest.json')
        manifest = json.load( manifest )
    except:
        raise Exception('Client manifest file load failure.')
    ##
    slave = SlaveDaemon(manifest, args.port, args.master)
    slave.start()
    pass

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        raise e #pass
    finally:
        pass #exit()
