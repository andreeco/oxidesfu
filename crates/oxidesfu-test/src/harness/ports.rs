    fn reserve_local_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .expect("port probe listener should bind")
            .local_addr()
            .expect("port probe listener should have local addr")
            .port()
    }
