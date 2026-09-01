"""
MiniRedis - A miniature Redis clone for learning database internals.
Phase 1: Core In-Memory Engine
Phase 2: Command Parser & Dispatcher
Phase 3: TCP Socket Server
Phase 4: Multi-Threaded Concurrency
Phase 5: Data Persistence (AOF - Append-Only File)
"""

import socket
import threading
import os


class MiniRedis:
    """
    A simple in-memory key-value store inspired by Redis.

    This class provides basic storage operations using an internal dictionary
    to store key-value pairs in memory.

    Phase 4: Thread Safety
    ----------------------
    In a multi-threaded server environment, multiple client threads can
    simultaneously access and modify the same shared data structure (self._data).
    Without synchronization, this leads to RACE CONDITIONS:

    Example race condition WITHOUT a lock:
    --------------------------------------
    - Thread A (Client 1): Reads self._data, sees key "counter" = 10
    - Thread B (Client 2): Reads self._data, sees key "counter" = 10
    - Thread A: Sets "counter" = 11
    - Thread B: Sets "counter" = 11
    Result: Both incremented from 10 to 11. One increment is lost!

    Another example - dictionary corruption:
    ----------------------------------------
    - Thread A: Starts adding key "user:1" (dictionary is resizing internally)
    - Thread B: Tries to read during resize (gets corrupted/partial data)
    Result: Crash or incorrect data returned!

    The threading.Lock() ensures only ONE thread can execute the critical
    section (read/write operations) at a time, preventing data corruption
    and race conditions.
    """

    def __init__(self):
        """
        Initialize the MiniRedis instance with an empty data store.

        Phase 4 Addition:
        ----------------
        - self.lock: A threading.Lock() to ensure thread-safe access to self._data

        WHY Lock is Necessary:
        - Python's dict operations are NOT atomic at the application level
        - Multiple threads reading/writing simultaneously can corrupt internal state
        - Even "simple" operations like dict[key] = value can be interrupted mid-execution
        """
        self._data = {}

        # Create a lock to protect concurrent access to self._data dictionary
        # This lock will be acquired before ANY operation that touches self._data
        self.lock = threading.Lock()

    def set(self, key, value):
        """
        Store a key-value pair in the database.

        Args:
            key: The key to store (can be any hashable type)
            value: The value to associate with the key (can be any type)

        Returns:
            str: "OK" to indicate successful storage

        Phase 4: Thread Safety
        ----------------------
        The 'with self.lock:' context manager acquires the lock BEFORE executing
        the code block and automatically releases it when done (even if an exception occurs).

        This ensures that only ONE thread can modify self._data at any given moment.
        Other threads attempting to acquire the lock will BLOCK (wait) until it's released.

        Example:
            >>> redis = MiniRedis()
            >>> redis.set("username", "alice")
            'OK'
        """
        # Acquire lock before modifying shared data structure
        # No other thread can access self._data until we release the lock
        with self.lock:
            self._data[key] = value
            return "OK"

    def get(self, key):
        """
        Retrieve the value associated with a key.

        Args:
            key: The key to look up

        Returns:
            The value associated with the key, or None if the key doesn't exist

        Phase 4: Thread Safety
        ----------------------
        Even READ operations need the lock! Why?

        Scenario without lock:
        - Thread A: Reading key "user:1"
        - Thread B: Deleting key "user:1" at the same time
        - Thread A: Gets partially deleted data or KeyError crash!

        Another reason:
        - Python's dictionary can resize itself when items are added/removed
        - Reading during a resize operation can return corrupted data
        - The lock prevents any modifications while we're reading

        Example:
            >>> redis = MiniRedis()
            >>> redis.set("username", "alice")
            'OK'
            >>> redis.get("username")
            'alice'
            >>> redis.get("nonexistent")
            None
        """
        # Acquire lock before reading from shared data structure
        with self.lock:
            return self._data.get(key)

    def delete(self, key):
        """
        Remove a key-value pair from the database.

        Args:
            key: The key to delete

        Returns:
            int: 1 if the key existed and was deleted, 0 if the key was not found

        Phase 4: Thread Safety
        ----------------------
        DELETE operations are critical - we need to ATOMICALLY:
        1. Check if a key exists
        2. Delete it if it exists

        Without the lock:
        - Thread A: Checks if "session:123" exists (YES)
        - Thread B: Deletes "session:123"
        - Thread A: Tries to delete "session:123" (KeyError - already gone!)

        The lock ensures the entire check-and-delete operation is atomic
        (happens as one indivisible unit).

        Example:
            >>> redis = MiniRedis()
            >>> redis.set("username", "alice")
            'OK'
            >>> redis.delete("username")
            1
            >>> redis.delete("username")
            0
        """
        # Acquire lock for the entire check-and-delete operation
        # This makes it atomic - no other thread can interfere between check and delete
        with self.lock:
            if key in self._data:
                del self._data[key]
                return 1
            return 0


class CommandParser:
    """
    Parses and executes Redis-like text commands against a MiniRedis instance.

    This class handles parsing raw text commands (e.g., "SET key value"),
    validates arguments, and dispatches them to the appropriate MiniRedis methods.
    """

    def __init__(self, db):
        """
        Initialize the CommandParser with a MiniRedis database instance.

        Args:
            db: A MiniRedis instance to execute commands against
        """
        self.db = db
        # Map of supported commands to their handlers
        self.commands = {
            "set": self._handle_set,
            "get": self._handle_get,
            "del": self._handle_del,
        }

    def execute(self, command_string):
        """
        Parse and execute a raw command string.

        Args:
            command_string: Raw text command (e.g., "SET user_name Iqra")

        Returns:
            str: Response message from the executed command or error message

        Example:
            >>> parser = CommandParser(MiniRedis())
            >>> parser.execute("SET username alice")
            'OK'
            >>> parser.execute("GET username")
            'alice'
        """
        # Trim whitespace and split into tokens
        tokens = command_string.strip().split()

        # Handle empty input
        if not tokens:
            return "ERR empty command"

        # Extract command name (case-insensitive)
        command_name = tokens[0].lower()
        args = tokens[1:]

        # Check if command is supported
        if command_name not in self.commands:
            return f"ERR unknown command '{tokens[0]}'"

        # Dispatch to appropriate handler
        handler = self.commands[command_name]
        return handler(command_name, args)

    def _handle_set(self, command_name, args):
        """
        Handle SET command: SET key value

        Args:
            command_name: The command name (for error messages)
            args: List of arguments [key, value]

        Returns:
            str: "OK" on success, or error message
        """
        if len(args) != 2:
            return f"ERR wrong number of arguments for '{command_name.upper()}' command"

        key, value = args
        self.db.set(key, value)
        return "OK"

    def _handle_get(self, command_name, args):
        """
        Handle GET command: GET key

        Args:
            command_name: The command name (for error messages)
            args: List of arguments [key]

        Returns:
            str: The value if found, "(nil)" if not found, or error message
        """
        if len(args) != 1:
            return f"ERR wrong number of arguments for '{command_name.upper()}' command"

        key = args[0]
        result = self.db.get(key)

        # Return "(nil)" for missing keys, matching Redis behavior
        if result is None:
            return "(nil)"

        return str(result)

    def _handle_del(self, command_name, args):
        """
        Handle DEL command: DEL key

        Args:
            command_name: The command name (for error messages)
            args: List of arguments [key]

        Returns:
            str: "(integer) 1" if deleted, "(integer) 0" if not found, or error message
        """
        if len(args) != 1:
            return f"ERR wrong number of arguments for '{command_name.upper()}' command"

        key = args[0]
        result = self.db.delete(key)
        return f"(integer) {result}"


class PersistenceManager:
    """
    Manages data persistence using an Append-Only File (AOF).

    Phase 5: Data Persistence
    -------------------------
    AOF (Append-Only File) is a durability mechanism used by Redis and other databases
    to survive server crashes and restarts. It works by logging every write operation
    to a file on disk.

    How AOF Provides Durability:
    ----------------------------
    1. Write Ahead Logging: Before returning success to the client, the command
       is written to disk. This ensures the operation is durable.

    2. Crash Recovery: On server restart, we replay all logged commands from the
       AOF file to rebuild the in-memory state exactly as it was.

    3. Append-Only: New commands are always appended to the end of the file.
       This is much faster than rewriting the entire database on each change.

    Trade-offs:
    -----------
    + Durability: Data survives crashes and restarts
    + Simple: Easy to understand and implement
    + Human-readable: AOF file contains plain text commands
    - File size: Can grow large over time (Redis uses AOF rewriting to compact)
    - Slower writes: Each write must hit disk (can be mitigated with buffering)

    Real-world usage:
    - Redis: Uses AOF + RDB (snapshots) for persistence
    - PostgreSQL: Uses WAL (Write-Ahead Log), similar concept
    - MySQL: Uses binlog for replication and recovery
    """

    def __init__(self, filepath="appendonly.aof", parser=None):
        """
        Initialize the persistence manager.

        Args:
            filepath: Path to the AOF file (default: appendonly.aof)
            parser: CommandParser instance to use for replaying commands
        """
        self.filepath = filepath
        self.parser = parser

        # Thread safety: Multiple client threads might try to log commands simultaneously
        # We need a lock to ensure file writes don't get interleaved/corrupted
        self.file_lock = threading.Lock()

        # Keep the file handle open for faster writes
        # Opening/closing the file for each command would be much slower
        self.aof_file = None

    def open_aof(self):
        """
        Open the AOF file in append mode.

        Mode 'a' means:
        - Create the file if it doesn't exist
        - Append to the end if it does exist (don't overwrite)
        - All writes go to the end of the file
        """
        try:
            # Open in append mode with UTF-8 encoding
            # buffering=1 means line-buffered (flush after each newline)
            self.aof_file = open(self.filepath, 'a', encoding='utf-8', buffering=1)
            print(f"[AOF] Opened AOF file: {self.filepath}")
        except Exception as e:
            print(f"[AOF ERROR] Failed to open AOF file: {e}")
            self.aof_file = None

    def close_aof(self):
        """Close the AOF file gracefully."""
        if self.aof_file:
            self.aof_file.close()
            print(f"[AOF] Closed AOF file: {self.filepath}")

    def log_command(self, command_str):
        """
        Log a write command to the AOF file.

        This method should be called AFTER a command executes successfully.
        Only mutating commands (SET, DEL) should be logged - not reads (GET).

        Args:
            command_str: The raw command string to log (e.g., "SET user:1 alice")

        Thread Safety:
        -------------
        Uses self.file_lock to ensure only one thread writes to the file at a time.
        Without this lock, multiple threads could interleave their writes, causing
        corrupted log entries like:
          SET usSET user:2 bobr:1 alice

        Durability:
        ----------
        We use flush() to force the OS to write buffered data to disk immediately.
        Without flush(), data might sit in memory buffers and be lost during a crash.
        For maximum durability, you could also call os.fsync(file.fileno()) to force
        the OS to write from its buffers to physical disk, but this is slower.
        """
        if not self.aof_file:
            return  # AOF not enabled or failed to open

        try:
            # Acquire lock before writing to file
            with self.file_lock:
                # Write the command followed by a newline
                self.aof_file.write(command_str + '\n')

                # Force the OS to flush buffers to disk
                # WHY: Without this, data might be lost if the process crashes
                # before the OS flushes its write buffers
                self.aof_file.flush()

        except Exception as e:
            print(f"[AOF ERROR] Failed to log command '{command_str}': {e}")

    def load_data(self):
        """
        Load and replay all commands from the AOF file to restore database state.

        This method is called during server startup BEFORE accepting client connections.

        Recovery Process:
        ----------------
        1. Check if AOF file exists
        2. If yes, open it and read line by line
        3. For each line (command), execute it through the CommandParser
        4. This rebuilds the in-memory database state exactly as it was

        Why This Works:
        --------------
        - AOF contains every write operation in the order they happened
        - Replaying them in the same order recreates the exact same state
        - It's like replaying a video frame by frame to get to the end

        Error Handling:
        --------------
        If the AOF file is corrupted (incomplete command, syntax error), we could:
        - Stop loading and report the error (current implementation)
        - Skip bad lines and continue (more forgiving, but might lose data)
        - Truncate the file at the corruption point (what Redis does)
        """
        # Check if AOF file exists
        if not os.path.exists(self.filepath):
            print(f"[AOF] No AOF file found at {self.filepath} - starting fresh")
            return

        if not self.parser:
            print("[AOF ERROR] No parser provided - cannot load data")
            return

        print(f"[AOF] Loading data from {self.filepath}...")

        try:
            # Open the file in read mode
            with open(self.filepath, 'r', encoding='utf-8') as f:
                command_count = 0

                # Read and replay each command line by line
                for line_num, line in enumerate(f, start=1):
                    # Remove leading/trailing whitespace and newlines
                    command = line.strip()

                    # Skip empty lines
                    if not command:
                        continue

                    # Execute the command through the parser
                    # This will call the appropriate MiniRedis method (set/delete)
                    result = self.parser.execute(command)

                    # Check if the command executed successfully
                    # If there's an error, we might have a corrupted AOF file
                    if result.startswith("ERR"):
                        print(f"[AOF WARNING] Line {line_num}: Command '{command}' "
                              f"failed with: {result}")
                        # You could choose to stop here or continue
                        # For now, we'll continue loading

                    command_count += 1

                print(f"[AOF] Successfully loaded {command_count} commands from AOF")

        except Exception as e:
            print(f"[AOF ERROR] Failed to load AOF file: {e}")


class RedisServer:
    """
    A multi-threaded TCP socket server for MiniRedis.

    Phase 3: Basic TCP Socket Server (single-threaded)
    Phase 4: Multi-Threaded Concurrency
    Phase 5: Data Persistence (AOF)

    This server listens on a specified host and port, accepts client connections,
    and spawns a NEW THREAD for each client. This allows multiple clients to
    connect and execute commands SIMULTANEOUSLY without blocking each other.

    Phase 5 adds durability through AOF (Append-Only File) persistence.

    Key Concepts:
    -------------
    - TCP (Transmission Control Protocol): Reliable, connection-oriented protocol
    - Socket: An endpoint for sending/receiving data over a network
    - Thread: Independent flow of execution that runs concurrently with other threads
    - AOF: Append-Only File for crash recovery and durability
    - Server workflow: Load AOF -> Create socket -> Bind -> Listen -> Accept -> Spawn Thread -> Repeat
    """

    def __init__(self, host="127.0.0.1", port=6379, aof_enabled=True, aof_filepath="appendonly.aof"):
        """
        Initialize the Redis server.

        Args:
            host: IP address to bind to (default: 127.0.0.1 = localhost)
            port: Port number to listen on (default: 6379, Redis standard port)
            aof_enabled: Enable AOF persistence (default: True)
            aof_filepath: Path to AOF file (default: appendonly.aof)
        """
        self.host = host
        self.port = port
        self.aof_enabled = aof_enabled

        # Create shared database and parser instances
        # NOTE: self.db is shared across ALL client threads - that's why it needs locks!
        self.db = MiniRedis()
        self.parser = CommandParser(self.db)

        # Phase 5: Initialize persistence manager
        if self.aof_enabled:
            self.persistence = PersistenceManager(filepath=aof_filepath, parser=self.parser)
        else:
            self.persistence = None

        # Phase 4: Track active client connections for monitoring
        # This counter is itself shared across threads, so it needs its own lock!
        self.active_clients = 0
        self.client_count_lock = threading.Lock()  # Protects active_clients counter

    def start(self):
        """
        Start the TCP server and begin accepting client connections.

        Phase 4: Multi-Threaded Server Model
        Phase 5: AOF Data Recovery
        ------------------------------------
        The main thread runs an infinite accept() loop. When a client connects,
        we immediately spawn a NEW THREAD to handle that client's requests.
        This allows the main thread to return to accept() immediately, ready
        to accept the next client connection without waiting.

        Phase 5 adds AOF loading BEFORE accepting connections to restore state.

        WHY threading.Thread is necessary:
        ----------------------------------
        Without threads (Phase 3 behavior):
        - Main thread: accept() -> handle_client() -> blocks until client disconnects
        - Problem: While handling Client A, Client B cannot connect!
        - Result: Server can only handle ONE client at a time (serialized)

        With threads (Phase 4 behavior):
        - Main thread: accept() -> spawn thread for handle_client() -> back to accept()
        - Each client gets its own thread running handle_client() independently
        - Problem solved: Server can handle 100s of concurrent clients simultaneously!

        Server Workflow:
        ---------------
        0. (Phase 5) Load data from AOF file if it exists
        1. Create a TCP socket
        2. Bind the socket to host:port
        3. Listen for incoming connections
        4. Loop forever:
           a. Accept a client connection
           b. Spawn a new thread to handle that client
           c. Immediately return to step 4a (accept next client)
        """
        # Phase 5: Step 0 - Load persisted data BEFORE accepting connections
        if self.persistence:
            self.persistence.load_data()
            self.persistence.open_aof()

        # Step 1: Create a TCP socket
        # AF_INET = IPv4 addressing
        # SOCK_STREAM = TCP (reliable, connection-oriented)
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Socket option: Allow reusing the address immediately after server restart
        # Without this, you'd get "Address already in use" errors
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        try:
            # Step 2: Bind the socket to the specified host and port
            # This associates the socket with a specific network interface and port number
            server_socket.bind((self.host, self.port))

            # Step 3: Listen for incoming connections
            # The argument (5) is the backlog queue size - max number of queued connections
            server_socket.listen(5)

            print(f"[SERVER] MiniRedis server started on {self.host}:{self.port}")
            print(f"[SERVER] Multi-threaded mode: Each client gets its own thread")
            if self.persistence:
                print(f"[SERVER] AOF persistence enabled: {self.persistence.filepath}")
            print(f"[SERVER] Waiting for client connections...")
            print(f"[SERVER] Press Ctrl+C to stop the server\n")

            # Step 4: Main server loop - continuously accept new connections
            while True:
                try:
                    # Accept a client connection (BLOCKS until a client connects)
                    # Returns: client_socket (for communication) and client_address (IP, port)
                    client_socket, client_address = server_socket.accept()

                    # Phase 4: Spawn a new thread to handle this client
                    # WHY: This allows the main loop to immediately return to accept()
                    # and accept more clients while this client is being handled
                    client_thread = threading.Thread(
                        target=self.handle_client,  # Function to run in the thread
                        args=(client_socket, client_address),  # Arguments to pass
                        daemon=True  # Daemon threads automatically terminate when main exits
                    )

                    # Start the thread - handle_client() now runs asynchronously
                    client_thread.start()

                    # Main thread immediately continues here and loops back to accept()
                    # The spawned thread runs handle_client() independently in the background

                except KeyboardInterrupt:
                    # Gracefully handle Ctrl+C shutdown
                    print("\n[SERVER] Shutting down gracefully...")
                    break

        except Exception as e:
            print(f"[ERROR] Server error: {e}")

        finally:
            # Phase 5: Close AOF file gracefully
            if self.persistence:
                self.persistence.close_aof()

            # Clean up: close the server socket
            server_socket.close()
            print("[SERVER] Server socket closed. Goodbye!")

    def handle_client(self, client_socket, client_address):
        """
        Handle communication with a connected client.

        Phase 4: Thread-Based Client Handling
        -------------------------------------
        This method now runs in its OWN THREAD (spawned by start()), allowing
        multiple clients to send commands simultaneously without blocking each other.

        Each client gets:
        - Its own thread (independent execution)
        - Its own socket (independent communication channel)
        - Shared access to self.db (protected by locks inside MiniRedis)

        Args:
            client_socket: The socket object for this specific client connection
            client_address: Tuple of (IP, port) identifying the client

        Communication Flow:
        ------------------
        1. Increment active client counter (thread-safe)
        2. Receive data from client (as bytes)
        3. Decode bytes to UTF-8 string
        4. Parse and execute the command (thread-safe via locks in MiniRedis)
        5. Encode response to UTF-8 bytes
        6. Send response back to client
        7. Decrement active client counter when done (thread-safe)
        """
        # Phase 4: Increment active client counter (thread-safe)
        # We need a lock here because active_clients is shared across all threads
        with self.client_count_lock:
            self.active_clients += 1
            current_count = self.active_clients

        # Get the current thread's name for logging
        thread_name = threading.current_thread().name

        # Log client connection with thread information
        print(f"[CONNECT] Client {client_address} connected")
        print(f"[THREAD] Handling on thread: {thread_name}")
        print(f"[STATS] Active clients: {current_count}\n")

        try:
            # Send a welcome message to the client
            welcome_msg = f"+MiniRedis Server Ready (Thread: {thread_name})\r\n"
            # Encoding: Convert string to bytes using UTF-8 encoding
            # WHY: Network sockets transmit raw bytes, not strings
            client_socket.sendall(welcome_msg.encode('utf-8'))

            # Client communication loop
            while True:
                # Receive data from the client
                # recv(1024) reads up to 1024 bytes at a time
                # BLOCKS until data arrives or connection closes
                # NOTE: This blocks ONLY this thread, not the entire server!
                data = client_socket.recv(1024)

                # If recv() returns empty bytes, the client has disconnected
                if not data:
                    print(f"[DISCONNECT] Client {client_address} disconnected")
                    break

                # Decode bytes to UTF-8 string
                # WHY: We need to convert raw bytes back to readable text
                command = data.decode('utf-8').strip()

                # Skip empty commands
                if not command:
                    continue

                # Log the received command
                print(f"[{client_address}] Command received: {command}")

                # Special handling for QUIT command
                if command.upper() == "QUIT":
                    response = "+OK Goodbye!\r\n"
                    client_socket.sendall(response.encode('utf-8'))
                    print(f"[DISCONNECT] Client {client_address} sent QUIT")
                    break

                # Execute the command through our parser
                # NOTE: self.parser.execute() calls self.db methods, which use locks
                # This ensures thread-safe access to the shared database
                result = self.parser.execute(command)

                # Phase 5: Log write commands to AOF file for durability
                # Only mutating commands (SET, DEL) should be persisted, not reads (GET)
                # We log AFTER execution to ensure the command succeeded
                if self.persistence and not result.startswith("ERR"):
                    command_upper = command.split()[0].upper() if command.split() else ""
                    if command_upper in ("SET", "DEL"):
                        # Log the command to the AOF file
                        # This ensures the operation survives crashes
                        self.persistence.log_command(command)

                # Format the response
                response = f"{result}\r\n"

                # Send the response back to the client
                # sendall() ensures ALL bytes are sent (unlike send())
                # Encoding: Convert string response to bytes
                client_socket.sendall(response.encode('utf-8'))

                print(f"[{client_address}] Response sent: {result}")

        except ConnectionResetError:
            # Client forcefully closed the connection
            print(f"[ERROR] Connection reset by client {client_address}")

        except Exception as e:
            # Handle any other errors during communication
            print(f"[ERROR] Error handling client {client_address}: {e}")

        finally:
            # Phase 4: Decrement active client counter (thread-safe)
            with self.client_count_lock:
                self.active_clients -= 1
                remaining_count = self.active_clients

            # Always close the client socket when done
            client_socket.close()

            # Log client disconnection with updated count
            print(f"[DISCONNECT] Connection with {client_address} closed")
            print(f"[STATS] Active clients: {remaining_count}\n")


if __name__ == "__main__":
    print("=" * 70)
    print("MiniRedis - Phase 4: Multi-Threaded Concurrent Server")
    print("=" * 70)
    print("\nPhase 4 Features:")
    print("  - Each client connection spawns a new thread")
    print("  - Multiple clients can connect and execute commands simultaneously")
    print("  - Thread-safe database operations with threading.Lock()")
    print("  - Real-time active client count tracking")
    print("\nStarting MiniRedis Multi-Threaded TCP Server...")
    print("\nHow to test multi-threading:")
    print("  1. Start this server")
    print("  2. Open MULTIPLE terminal windows")
    print("  3. In each window, connect with: telnet 127.0.0.1 6379")
    print("     Or use: nc 127.0.0.1 6379")
    print("  4. Send commands from multiple clients simultaneously!")
    print("\nSupported commands:")
    print("  SET key value  - Store a key-value pair")
    print("  GET key        - Retrieve a value")
    print("  DEL key        - Delete a key")
    print("  QUIT           - Disconnect from server")
    print("\nExample test scenario:")
    print("  Client 1: SET counter 0")
    print("  Client 2: GET counter")
    print("  Client 1: SET counter 1")
    print("  Client 3: GET counter")
    print("  Client 2: DEL counter")
    print("\n" + "=" * 70 + "\n")

    # Create and start the multi-threaded server
    server = RedisServer(host="127.0.0.1", port=6379)
    server.start()
