#!/usr/bin/env python3
import multiprocessing as mp
import time
import unittest
from unittest import TestSuite, TestCase

import tap

MANIFEST = {
    "name":"test",
    "codebase": {},

    "functions": {
        "test_no_action": {
            "description":"test_no_action"
        },
        "test_no_parameters": {
            "description":"test_no_parameters",
            "commands": ["echo no_parameters"],
            "outputs": { "output":{"cmd":"echo $output_0","format":".*"} }
        },
        "test_no_commands": {
            "description":"test_no_commands",
            "parameters": {"param":"no_commands"},
            "outputs": { "output":{"cmd":"echo $param","format":".*"} }
        },
        "test_no_outputs": {
            "description":"test_no_outputs",
            "parameters": {"param":"dummy"},
            "commands": ["echo no_output"]
        },
        ##
        "test_command_index": {
            "description":"test_command_index",
            "parameters": {"p1":1, "p2":"2", "p3":3.3},
            "commands": ["echo $p1", "echo $p2", "echo $p3"],
            "outputs": { "output3":{"cmd":"echo $output_2","format":".*"} }
        }
    }
}

class TapTestCase(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = tap.MasterDaemon(tap.SERVER_PORT, tap.IPC_PORT, MANIFEST)
        cls.client = tap.SlaveDaemon(tap.SERVER_PORT, MANIFEST, '127.0.0.1')
        ##
        cls.proc_server = mp.Process(target=cls.server.start)
        cls.proc_client = mp.Process(target=cls.client.start)
        cls.proc_server.start()
        time.sleep(0.01)
        cls.proc_client.start()
        time.sleep(0.01)
        pass

    @classmethod
    def tearDownClass(cls):
        cls.proc_client.kill()
        time.sleep(0.01)
        cls.proc_server.kill()
        time.sleep(0.01)
        pass
    pass

class TestListAllClients(TapTestCase):
    def test_list_all_no_client_name(self):
        console = tap.Connector()
        res = console.list_all()
        self.assertIsInstance(res, dict)

    def test_list_all_wrong_client_name(self):
        console = tap.Connector('???')
        res = console.list_all()
        self.assertIsInstance(res, dict)
    pass

class TestManifestFetch(TapTestCase):
    def test_describe_correct_client(self):
        clients = tap.Connector().list_all()
        client,_ = clients.popitem()
        console = tap.Connector(client)
        res = console.describe()
        self.assertIsInstance(res, dict)
    
    def test_describe_wrong_client(self):
        client = '???'
        console = tap.Connector(client)
        try:
            console.describe()
        except tap.ClientNotFoundException:
            pass

class TestClientExecution(TapTestCase):
    def test_no_action(self):
        c = tap.Connector('test')
        tid = c.execute('test_no_action')
        time.sleep(0.01)
        c.fetch(tid)
    
    def test_no_parameters(self):
        c = tap.Connector('test')
        tid = c.execute('test_no_parameters')
        time.sleep(0.01)
        c.fetch(tid)
    
    def test_no_commands(self):
        c = tap.Connector('test')
        tid = c.execute('test_no_commands')
        time.sleep(0.01)
        c.fetch(tid)
    
    def test_no_outputs(self):
        c = tap.Connector('test')
        tid = c.execute('test_no_outputs')
        time.sleep(0.01)
        c.fetch(tid)
    
    def test_command_index(self):
        c = tap.Connector('test')
        tid = c.execute('test_command_index')
        time.sleep(0.01)
        c.fetch(tid)

class TestServerClientExecution(TapTestCase):
    def test_server_only(self):
        c = tap.Connector()
        tid = c.execute('test_command_index')
        time.sleep(0.01)
        c.fetch(tid)
    
    def test_server_and_client(self):
        c1, c2 = tap.Connector(), tap.Connector('test')
        tid1, tid2 = c1.execute('test_command_index'), c2.execute('test_command_index')
        time.sleep(0.01)
        c1.fetch(tid1)
        c2.fetch(tid2)
    pass

class TestBatchExecution(TapTestCase):
    def test_batch_on_server(self):
        cc = tap.Connector()
        res = ( cc.batch('', 'test_no_action')
                  .batch('', 'test_no_parameters')
                  .batch('', 'test_no_commands')
                  .batch('', 'test_no_outputs')
                  .batch('', 'test_command_index')
                  .wait(0.01)
                  .fetch()  ).apply()
        assert(None not in res)
    
    def test_batch_on_client(self):
        cc = tap.Connector()
        res = ( cc.batch('test', 'test_no_action')
                  .batch('test', 'test_no_parameters')
                  .batch('test', 'test_no_commands')
                  .batch('test', 'test_no_outputs')
                  .batch('test', 'test_command_index')
                  .wait(0.01)
                  .fetch()  ).apply()
        assert(None not in res)

    def test_batch_mixed(self):
        cc = tap.Connector()
        res = ( cc.batch('',     'test_command_index')
                  .batch('test', 'test_command_index')
                  .wait(0.01)
                  .fetch()  ).apply()
        assert(None not in res)

    def test_batch_all(self):
        cc = tap.Connector()
        task_list = [
            ('', 'test_command_index'),
             {'client':'test', 'function':'test_command_index'}
        ]
        res = cc.batch_all(task_list).wait(0.01).fetch().apply()
        assert(None not in res)
    pass


if __name__=='__main__':
    unittest.main()