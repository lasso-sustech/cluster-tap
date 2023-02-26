#!/usr/bin/env python3
import multiprocessing as mp
import time
import unittest
from unittest import TestSuite, TestCase

import tap

MANIFEST = {
    "name":"test",

    "functions": {
        "test_empty_content": {},
        "test_empty_description": {
            "parameters": {"dummy":""},
            #TODO:
        }
    }
}

class TapTestCase(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = tap.MasterDaemon(tap.SERVER_PORT, tap.IPC_PORT)
        cls.client = tap.SlaveDaemon(MANIFEST, tap.SERVER_PORT, '127.0.0.1')
        ##
        cls.proc_server = mp.Process(target=cls.server.start)
        cls.proc_client = mp.Process(target=cls.client.start)
        cls.proc_server.start()
        cls.proc_client.start()
        time.sleep(0.001)
        pass

    @classmethod
    def tearDownClass(cls):
        cls.proc_client.terminate()
        cls.proc_server.terminate()
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

if __name__=='__main__':
    unittest.main()