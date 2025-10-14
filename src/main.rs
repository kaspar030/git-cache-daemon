use std::io::{Error, ErrorKind};
use std::process::Command;
use std::time::Duration;

use camino::{Utf8Component, Utf8Path, Utf8PathBuf};
use monoio::io::{AsyncReadRent, Splitable};
use monoio::net::{TcpListener, TcpStream};
use monoio::select;
use tracing::info;

use monoio::io::zero_copy;

#[monoio::main(timer_enabled = true)]
async fn main() {
    // initialize logging
    let subscriber = tracing_subscriber::FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    // start listening
    let listener = TcpListener::bind("127.0.0.1:9418").unwrap();
    info!("listening");
    loop {
        let incoming = listener.accept().await;
        match incoming {
            Ok((stream, addr)) => {
                info!("accepted a connection from {addr}");
                monoio::spawn(handle_client(stream));
            }
            Err(e) => {
                info!("accepting connection failed: {e}");
            }
        }
    }
}

async fn handle_client(mut stream: TcpStream) -> std::io::Result<()> {
    let (host, path) = parse_request(&mut stream).await?;

    info!("client requests {host} {path}");

    prefetch(&format!("https://{host}{path}")).await?;

    info!("prefetch ok, serving repo");
    upload_pack(stream, host, path).await?;

    Ok(())
}

async fn prefetch(url: &str) -> Result<(), Error> {
    let mut command = Command::new("git")
        .env("GIT_CONFIG_GLOBAL", "/home/kaspar/.gitcache/config")
        .args(["cache", "prefetch", "-U", url])
        .spawn()?;

    for _ in 0..100 {
        if command.try_wait()?.is_some() {
            info!("child reaped");
            break;
        }
        monoio::time::sleep(Duration::from_secs(1)).await;
    }

    Ok(())
}

async fn upload_pack(stream: TcpStream, host: Utf8PathBuf, path: Utf8PathBuf) -> Result<(), Error> {
    let (stdin_recv, mut stdin_send) = monoio::net::unix::new_pipe()?;
    let (mut stdout_recv, stdout_send) = monoio::net::unix::new_pipe()?;

    let mut path = Utf8PathBuf::from(&format!("/home/kaspar/.gitcache/{host}{path}"));
    path.set_extension("git");

    let mut command = Command::new("git-upload-pack")
        .env("GIT_CONFIG_COUNT", "1")
        .env("GIT_CONFIG_KEY_0", "uploadpack.allowAnySHA1InWant")
        .env("GIT_CONFIG_VALUE_0", "true")
        .args(["--strict", path.as_str()])
        .stdin(stdin_recv)
        .stdout(stdout_send)
        .spawn()?;

    let (mut read, mut write) = stream.into_split();

    let mut in_n = None;
    let mut out_n = None;

    loop {
        info!("entering loop");
        select! {
            to_stream = zero_copy(&mut stdout_recv, &mut write), if out_n.is_none() => {
                info!("to_stream {:?}", to_stream);
                out_n = Some(to_stream);
            }
            from_stream = zero_copy(&mut read, &mut stdin_send), if in_n.is_none() => {
                info!("from_stream {:?}", from_stream);
                in_n = Some(from_stream);
            }
        };

        if out_n.is_some() && in_n.is_some() {
            if let Some(res) = command.try_wait()? {
                if !res.success() {
                    info!("warn: git-upload-pack errored");
                }
                break;
            } else {
                info!("child still running");
                monoio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }

    Ok(())
}

async fn parse_request(stream: &mut TcpStream) -> Result<(Utf8PathBuf, Utf8PathBuf), Error> {
    fn bad_pkt() -> Error {
        Error::new(ErrorKind::InvalidData, "Malformed packet")
    }
    let mut buf: Vec<u8> = Vec::with_capacity(8 * 1024);
    let mut res;
    loop {
        // read
        (res, buf) = stream.read(buf).await;
        let mut buf_pos = 0usize;
        buf_pos += res?;
        if buf_pos == 0 {
            info!("connection with {} dropped", stream.peer_addr().unwrap());
            return Err(Error::from(ErrorKind::UnexpectedEof));
        }

        info!("got {buf_pos}b");
        if buf_pos < 4 {
            info!("expected four bytes");
        }

        if &buf[0..4] == b"0000" {
            info!("got flush-pkt");
            // clear
            buf.clear();
            continue;
        }

        let pkt_len_str = str::from_utf8(&buf[0..4]).map_err(|_| bad_pkt())?;
        let pkt_len = usize::from_str_radix(pkt_len_str, 16).map_err(|_| bad_pkt())?;

        if pkt_len != buf_pos {
            return Err(bad_pkt());
        }

        if pkt_len == 4 {
            // clear
            buf.clear();
            continue;
        }

        let payload = &buf[4..];

        let parts: Result<Vec<&str>, _> = payload
            .split(|&item| item == 0)
            .filter(|part| part.len() != 0)
            .map(|part| str::from_utf8(part))
            .collect();

        let parts = parts.map_err(|_| bad_pkt())?;

        let mut cmd_and_pathname = parts[0].split(' ');
        let request_command = cmd_and_pathname.next().ok_or_else(|| bad_pkt())?;
        let pathname = cmd_and_pathname.next().ok_or_else(|| bad_pkt())?;

        if request_command != "git-upload-pack" {
            return Err(bad_pkt());
        }

        return Ok(split_hostname(Utf8Path::new(pathname)));
    }
}

fn split_hostname(path: &Utf8Path) -> (Utf8PathBuf, Utf8PathBuf) {
    let mut components: Vec<_> = path.components().collect();
    let host = if let Some(Utf8Component::RootDir) = components.get(0) {
        components.remove(1)
    } else {
        components.remove(0)
    };
    let path = components.iter().collect::<Utf8PathBuf>();

    (Utf8PathBuf::from(host.as_str()), path)
}
