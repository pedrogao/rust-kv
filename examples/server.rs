use anyhow::Result;
use async_prost::AsyncProstStream;
use futures::prelude::*;
use kv::{CommandRequest, CommandResponse, MemTable, Service};
use tokio::net::TcpListener;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let service = Service::new(MemTable::new());
    let addr = "127.0.0.1:9527";
    // 绑定 9527 端口
    let listener = TcpListener::bind(addr).await?;
    info!("Start listening on {}", addr);
    loop {
        // 接受一个连接
        let (stream, addr) = listener.accept().await?;
        info!("Client {:?} connected", addr);
        let srv = service.clone();
        // 开启一个协程处理
        tokio::spawn(async move {
            let mut stream =
                AsyncProstStream::<_, CommandRequest, CommandResponse, _>::from(stream).for_async();
            while let Some(Ok(cmd)) = stream.next().await {
                info!("Got a new command: {:?}", cmd);
                let res = srv.execute(cmd);
                stream.send(res).await.unwrap();
            }
            info!("Client {:?} disconnected", addr);
        });
    }
}
