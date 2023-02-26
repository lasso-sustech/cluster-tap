### How to Use

**Client Side**:
- copy `tap.py` to the client, next to client's function code.
- create `manifest.json` file in the same place following the format in `manifest.json.example`.
- run `tap.py -c` as client, trying to connect to any online server.

**Server Side**:
- copy `tap.py` to the server, next to server's function code.
- run `tap.py -s` as server, waiting for clients connection.
- compile your own scripts communicating with server using `Connector` class. For example:
    ```python
    #!/usr/bin/env python3
    import time
    from tap import Connector

    # use a temporary connection to fetch online clients
    conn = Connector()
    clients = conn.list_all()

    # execute tasks on clients asynchronously
    conns = [ Connector(c) for c in clients ]
    tid_list = [ c.execute('run-stream-replay', {'target_addr':'127.0.0.1', 'duration':10})
                    for c in conns ]
    
    # wait for possible completion, and fetch the results as tuple
    time.sleep(11)
    results = dict()
    outputs = [ c.fetch(tid) for c,tid in zip(conns,tid_list) ]
    [ results.update(o) for o in outputs ]
    ```

### TODO
- [ ] codebase sync, from server to clients.
- [ ] complete test suite and test pass.
