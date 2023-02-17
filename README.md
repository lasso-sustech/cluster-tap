### How to Use

**Client Side**:
- copy `client.py` to the client, next to client's function code;
- create `manifest.json` file in the same folder resembling the format in `manifest.json.example`.
- run `client.py` as slave, trying to connect to online server daemon.

**Server Side**:
- copy `server.py` to the server, next to server's function code.
- run `server.py` as master, waiting for clients connection.
- compile your own scripts communicating with server via IPC port:
    - TODO
