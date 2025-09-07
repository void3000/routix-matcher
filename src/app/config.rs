use std::net::SocketAddr;

pub struct Config {
    pub socket_addr: SocketAddr,
}

impl Config {
    pub fn new(socket_addr: &str) -> Self {
        let socket_addr: SocketAddr = socket_addr
            .parse()
            .expect("Invalid socket address");

        Self { socket_addr }
    }
}
