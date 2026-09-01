# Mini-Redis: A Python Implementation

A miniature Redis clone built from scratch in Python to learn database internals, networking, and system design.

## Features

### Phase 1: Core In-Memory Engine
- Key-value storage using Python dictionaries
- Basic operations: SET, GET, DELETE

### Phase 2: Command Parser & Dispatcher
- Text command parsing (Redis-like syntax)
- Case-insensitive command matching
- Error handling and validation

### Phase 3: TCP Socket Server
- Socket programming with Python's `socket` library
- Network communication over TCP/IP
- Client-server architecture

### Phase 4: Multi-Threaded Concurrency
- Thread-per-client model using `threading` module
- Thread-safe operations with locks
- Concurrent client support

### Phase 5: Data Persistence (AOF)
- Append-Only File (AOF) for durability
- Crash recovery and state restoration
- Write-ahead logging

## Installation

```bash
git clone https://github.com/YOUR_USERNAME/mini-redis-python.git
cd mini-redis-python
```

## Usage

Start the server:
```bash
python mini_redis.py
```

Connect using telnet or netcat:
```bash
telnet 127.0.0.1 6379
# or
nc 127.0.0.1 6379
```

## Commands

- `SET key value` - Store a key-value pair
- `GET key` - Retrieve a value
- `DEL key` - Delete a key
- `QUIT` - Disconnect from server

## Example

```bash
SET username Alice
OK

GET username
Alice

DEL username
(integer) 1

GET username
(nil)
```

## Architecture

- **MiniRedis**: Core storage engine with thread-safe operations
- **CommandParser**: Command parsing and dispatch
- **PersistenceManager**: AOF-based durability
- **RedisServer**: Multi-threaded TCP server

## Technical Highlights

- Thread-safe concurrent access using `threading.Lock()`
- AOF persistence for crash recovery
- Production-ready error handling
- Clean, beginner-friendly code with extensive comments

## Requirements

- Python 3.6+
- No external dependencies (uses only standard library)

## License

MIT License - Free to use for learning purposes

## Author

Built as a learning project to understand database internals and system design.
