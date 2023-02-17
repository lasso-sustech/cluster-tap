#!/usr/bin/env python3
import argparse
import json
import socket
import struct


SERVER_PORT = 11112
IPC_PORT    = 52525


class Request:
    def __init__(self, sock):
        self.sock = sock

    def describe(self):
        self.__send('describe')
    
    def info(self, function:str):
        self.__send('info', {'function':function})
    
    def execute(self, function, parameters:dict=None, timeout:float=None):
        msg = {'function':function}
        if parameters: msg['parameters'] = parameters
        if timeout: msg['timeout'] = timeout
        self.__send('execute', msg)
    
    def fetch(self, tid):
        self.__send('fetch', {'tid':tid})

    def __send(self):
        pass

    pass

class MasterDaemon:
    pass

def main():
    parser = argparse.ArgumentParser(description='The master program of cluster-tap.')
    pass

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        raise e #pass
    finally:
        pass #exit()
