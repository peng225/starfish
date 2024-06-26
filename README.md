# Starfish

Starfish is a distributed lock manager.

## Quick start

1. Create a configuration file. Here is a sample config file.
   ```yaml
   # Web server address for Starfish clients.
   webServers:
   - "http://192.168.10.100:8080"
   - "http://192.168.10.101:8080"
   - "http://192.168.10.102:8080"
   # gRPC server address for other instances.
   grpcServers:
   - "192.168.10.100:10080"
   - "192.168.10.101:10080"
   - "192.168.10.102:10080"
   ```

2. Run at least three instances. Here is a command example.
   ```sh
   ./starfish -id 0 -config config.yaml -pstore-dir /path/to/persistent/store
   ```

3. Now the Web API is available for all of the Web server addresses you configured.

## Web API

| Path | Method | Description |
|-------|-------|-----------|
| `/lock` | Get | You can get the client ID who currently holds the lock. If the lock is not held by anybody, -1 is set in the response body. |
| `/lock` | Put | You can acquire the lock. You must set the client ID (>= 0) in the request body. If the lock is held by someone else, the HTTP status code 409 is returned. |
| `/unlock` | Put | You can release the lock. You should set the client ID in the request body. If the ID set in the request body is different from the current lock holder, the HTTP status code 400 is returned. |
