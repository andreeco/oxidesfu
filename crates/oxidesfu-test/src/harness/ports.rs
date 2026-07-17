    static RESERVED_LOCAL_PORTS: std::sync::OnceLock<Mutex<HashSet<u16>>> =
        std::sync::OnceLock::new();

    fn reserve_local_port() -> u16 {
        loop {
            let listener = std::net::TcpListener::bind("127.0.0.1:0")
                .expect("port probe listener should bind");
            let port = listener
                .local_addr()
                .expect("port probe listener should have local addr")
                .port();
            let mut reserved_ports = RESERVED_LOCAL_PORTS
                .get_or_init(|| Mutex::new(HashSet::new()))
                .lock()
                .expect("local port registry should not be poisoned");
            if reserved_ports.insert(port) {
                return port;
            }
        }
    }
