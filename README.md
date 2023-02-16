### How to Use

**Server Side**:
- copy `tap.py` to the server, next to server's function code.
- create `manifest.json` file following the format in `manifest.json.example`
  - **The manifest file is not mandatory**, if no function will be executed on server;
  - The name section is always neglected, with default value `''`.
- run `tap.py -s` as server, waiting for clients connection.

**Client Side**:
- copy `tap.py` to the client, next to client's function code.
- create `manifest.json` file in the same place following the format in `manifest.json.example`.
- run `tap.py -c` as client, trying to connect to any online server.

**Console Side**:
Compile your own scripts communicating with server using `Connector` class. For example:

```python
#!/usr/bin/env python3
import time
from tap import Connector

# use a temporary connection to fetch online clients
conn = Connector()
clients = conn.list_all()
conns = [ Connector(c) for c in clients ]

# a) Flow Mode: run tasks using python flow control, with execute/fetch.
tid_list = [ c.execute('test', {'dummy':'dummy'})
                for c in conns ]
time.sleep(12) # wait for possible completion
results = dict()
outputs = [ c.fetch(tid) for c,tid in zip(conns,tid_list) ] # fetch the results
[ results.update(o) for o in outputs ]

## b) Batch Mode: write script-style code, with batch related commands.
params = {'target_addr':'127.0.0.1', 'duration':10}
results = dict()
outputs = ( conn.batch('server', 'run-server', params, timeout=11)
                .wait(1)
                .batch('client', 'run-client', params, timeout=10)
                .wait(12)
                .fetch() ).apply()
[ results.update(o) for o in outputs ]
```
