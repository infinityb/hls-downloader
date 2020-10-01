use std::env;
use std::fs::File;
use std::collections::BTreeMap;
use std::time::Instant;
use std::io::Write;
use std::path::{PathBuf, Path};
use std::ffi::OsString;

use tokio::io::{AsyncBufReadExt, BufReader};
use clap::{App, Arg};
use futures::stream::StreamExt;
use hyper::client::HttpConnector;
use hyper::Client;
use hyper_tls::HttpsConnector;
use ksuid::Ksuid;
use linked_hash_map::LinkedHashMap;
use metrics::{timing, counter, value};
use metrics_runtime::Receiver;
use tokio::sync::mpsc;
use tokio::time::{Duration, timeout, delay_for};
use tracing::{event, Level};
use tracing_subscriber::filter::LevelFilter as TracingLevelFilter;
use tracing_subscriber::FmtSubscriber;
use url::Url;
use serde::{Serialize, Deserialize};

mod event_logger;

use self::event_logger::LogExporter;    

const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
const CARGO_PKG_NAME: &str = env!("CARGO_PKG_NAME");

#[derive(Serialize, Deserialize)]
struct Command {
    output_dir: PathBuf,
    hls_uri: String,
}


#[tokio::main]
async fn main() {
    let mut my_subscriber_builder = FmtSubscriber::builder();

    let app = App::new(CARGO_PKG_NAME)
        .version(CARGO_PKG_VERSION)
        .author("Stacey Ell <stacey.ell@gmail.com>")
        .about("An HLS downloader")
        .arg(
            Arg::with_name("v")
                .short("v")
                .multiple(true)
                .help("Sets the level of verbosity"),
        )
        .arg(
            Arg::with_name("prometheus-bind-address")
                .long("prometheus-bind-address")
                .value_name("ADDRESS")
                .help("The address to bind to for prometheus")
                .takes_value(true),
        );
        // .arg(
        //     Arg::with_name("output-dir")
        //         .long("output-dir")
        //         .value_name("DIR")
        //         .help("the directory to put transport streams into")
        //         .required(true)
        //         .takes_value(true),
        // )
        // .arg(
        //     Arg::with_name("hls-uri")
        //         .value_name("URI")
        //         .help("HLS URI")
        //         .takes_value(true)
        //         .required(true)
        //         .index(1),
        // );

    let matches = app.get_matches();

    let verbosity = matches.occurrences_of("v");
    let should_print_test_logging = 4 < verbosity;

    my_subscriber_builder = my_subscriber_builder.with_max_level(match verbosity {
        0 => TracingLevelFilter::ERROR,
        1 => TracingLevelFilter::WARN,
        2 => TracingLevelFilter::INFO,
        3 => TracingLevelFilter::DEBUG,
        _ => TracingLevelFilter::TRACE,
    });

    tracing::subscriber::set_global_default(my_subscriber_builder.finish())
        .expect("setting tracing default failed");

    if should_print_test_logging {
        print_test_logging();
    }

    let metrics_rx = Receiver::builder()
        .build()
        .expect("failed to create receiver");

    let metrics_controller = metrics_rx.controller();
    metrics_rx.install();

    if let Some(addr) = matches.value_of("prometheus-bind-address") {
        let addr = addr.parse().unwrap();
        tokio::task::spawn(async move {
            let prom = metrics_runtime::observers::PrometheusBuilder::new();

            let ex = metrics_runtime::exporters::HttpExporter::new(metrics_controller, prom, addr);

            event!(Level::INFO, "metrics exported on {}", addr);

            if let Err(err) = ex.async_run().await {
                event!(Level::ERROR, "metrics exporter returned an error: {}", err);
            }
        });
    } else {
        tokio::task::spawn(async move {
            let prom = metrics_runtime::observers::JsonBuilder::new();

            let ex = LogExporter::new(metrics_controller, prom, std::time::Duration::new(15, 0));

            ex.async_run().await;
        });
    }

    let https = HttpsConnector::new();
    let client = Client::builder()
        .pool_idle_timeout(Duration::from_secs(30))
        .http1_title_case_headers(true)
        .build::<_, hyper::Body>(https);


    let mut stdin = BufReader::new(tokio::io::stdin());
    let mut stdin_lines = stdin.lines();


    while let Some(line) = stdin_lines.next().await {
        let line = line.unwrap();
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let cmd: Command = serde_json::from_str(line).unwrap();
        let client = client.clone();

        tokio::task::spawn(async move {
            let rx = hls_url_emitter_task(cmd.hls_uri.clone());
            let base_dir: PathBuf = Path::new(&cmd.output_dir).into();
            let session_id = Ksuid::generate();
            let mut completion_stream = rx.map(|v: HlsElement| {
                retrying_request(base_dir.clone(), &session_id, &client, v)
            }).buffered(3);

            while let Some(v) = completion_stream.next().await {
                if let Err(err) = v {
                    event!(Level::ERROR, "completion error - {:?}", err);
                }
            }
        });
    }
}

#[allow(clippy::cognitive_complexity)] // macro bug around event!()
fn print_test_logging() {
    event!(Level::TRACE, "logger initialized - trace check");
    event!(Level::DEBUG, "logger initialized - debug check");
    event!(Level::INFO, "logger initialized - info check");
    event!(Level::WARN, "logger initialized - warn check");
    event!(Level::ERROR, "logger initialized - error check");
}

async fn uri_to_buf(hyper_client: &Client<HttpsConnector<HttpConnector>>, uri: String) -> Result<hyper::body::Bytes, failure::Error> {
    let hyper_uri: hyper::Uri = uri.parse()?;
    let mut res = hyper_client.get(hyper_uri).await?;

    let mut redirect_count: u64 = 4;
    while res.status().is_redirection() {
        if redirect_count == 0 {
            return Err(failure::format_err!("too many redirects"));
        }

        let mut new_url = None;
        if let Some(data) = res.headers().get(hyper::header::LOCATION) {
            if let Ok(s) = data.to_str() {
                new_url = Some(s);
            }
        }
        if let Some(s) = new_url {
            redirect_count -= 1;

            let url = Url::parse(&uri)?;
            let url = url.join(s)?;
            let hyper_uri: hyper::Uri = url.as_str().parse()?;
            res = hyper_client.get(hyper_uri).await?;
        } else {
            return Err(failure::format_err!("bad redirect - {:#?}", res.headers()));
        }
    }

    if res.status() != 200 {
        return Err(failure::format_err!("bad status - {}", res.status()));
    }
    
    let buf = hyper::body::to_bytes(res).await?;

    counter!("http_video_bytes_read", buf.len() as u64);

    Ok(buf)
}

async fn retrying_request(base_dir: PathBuf, session_id: &Ksuid, hyper_client: &Client<HttpsConnector<HttpConnector>>, hls: HlsElement) -> Result<(), failure::Error> {
    let session_id = *session_id;

    let video_buf;
    let mut i: u64 = 0;
    loop {
        let req_start_time = Instant::now();
        match timeout(Duration::from_secs(10), uri_to_buf(hyper_client, hls.media_uri.clone())).await {
            Ok(Ok(buf)) => {
                video_buf = buf;
                timing!("http_video_fetch_success", req_start_time.elapsed());
                break;
            }
            Ok(Err(err)) => {
                if i == 4 {
                    return Err(err);
                }

                timing!("http_video_fetch_server_error", req_start_time.elapsed());
                delay_for(Duration::from_millis(100)).await;
                event!(Level::ERROR, "encountered error fetching video, attempt#{}: {:?}", i, err);
                i += 1;
                continue;
            },
            Err(err) => {
                if i == 4 {
                    return Err(err.into());
                }

                timing!("http_video_fetch_timeout_error", req_start_time.elapsed());
                delay_for(Duration::from_millis(100)).await;
                event!(Level::ERROR, "encountered error fetching video, attempt#{}: timed out", i);
                i += 1;
                continue;
            }
        }
    }

    counter!("http_video_fetch_count", 1);

    tokio::task::spawn_blocking(move || -> Result<(), failure::Error> {
        let start_time = Instant::now();
        let ultimate_filename = format!("{}_{:08}.ts", session_id.to_base62(), hls.request_index);
        let ultimate_filepath = base_dir.join(&ultimate_filename);
        let tmp_filename = format!("{}_{:08}.ts.tmp", session_id.to_base62(), hls.request_index);
        let tmp_filepath = base_dir.join(&tmp_filename);
        let mut f = File::create(&tmp_filepath)?;
        f.write_all(&video_buf[..])?;
        drop(f);

        std::fs::rename(&tmp_filepath, &ultimate_filepath)?;

        timing!("filesystem_write_latency", start_time.elapsed());
        event!(Level::INFO, "wrote file {}", ultimate_filepath.display());

        Ok(())
    }).await??;

    Ok(())
}

#[derive(Debug)]
struct HlsElement {
    request_index: u64,
    extensions: BTreeMap<String, String>,
    media_uri: String,
}

#[derive(Default)]
struct StreamContext {
    seen_uris: LinkedHashMap<String, ()>,
}

fn parse_m3u(data: &[u8], into: &mut Vec<HlsElement>) -> Result<(), failure::Error> {
    let body = std::str::from_utf8(data)?;

    let mut extensions = BTreeMap::new();
    for line in body.split('\n') {
        if let Some(meta) = deprefix("#EXT-X-", &line[..]) {
            if let Some(colon_pos) = meta.find(':') {
                extensions.insert(
                    meta[..colon_pos].to_string(),
                    meta[colon_pos + 1..].to_string());
            }
            continue;
        }

        if line.starts_with("#") {
            continue;
        }

        let line_trimmed = line.trim();

        let taken_extensions = std::mem::replace(&mut extensions, BTreeMap::new());

        if line_trimmed.is_empty() {
            continue;
        }

        into.push(HlsElement {
            request_index: 0, // filled in later.
            extensions: taken_extensions,
            media_uri: line.trim().to_string(),
        });
    }

    Ok(())
}

fn hls_url_emitter_task(uri_s: String) -> mpsc::Receiver<HlsElement> {
    let (mut tx, rx) = mpsc::channel(30);
    tokio::spawn(async move {
        let https = HttpsConnector::new();
        let client = Client::builder()
            .pool_idle_timeout(Duration::from_secs(30))
            .http1_title_case_headers(true)
            .build::<_, hyper::Body>(https);

        const FAILURE_BACKOFF_INITIAL: Duration = Duration::from_secs(1);
        const REQUEST_INTERVAL_INITIAL: Duration = Duration::from_secs(1);

        let mut failure_backoff = FAILURE_BACKOFF_INITIAL;
        let mut request_interval = REQUEST_INTERVAL_INITIAL;

        let mut ctx: StreamContext = Default::default();


        let uri: hyper::Uri = uri_s.parse().unwrap();
        let mut item_counter = 0;
        let mut ele = Vec::new();
        let mut last_new_item_seen = Instant::now();
        loop {
            let req_time = std::time::Instant::now();
            event!(Level::INFO, "requesting {}", uri_s);
            let res = match client.get(uri.clone()).await {
                Ok(res) => res,
                Err(err) => {
                    event!(Level::ERROR, "error: {:?}", err);
                    delay_for(failure_backoff).await;
                    failure_backoff += failure_backoff;
                    continue;
                }
            };

            event!(Level::INFO, response_code = res.status().as_u16(), "got response");

            if res.status() == 404 {
                break;
            }
            if res.status() != 200 {
                event!(Level::WARN, "hls request failed (bad status) - retrying in {:?}", failure_backoff);
                delay_for(failure_backoff).await;
                failure_backoff += failure_backoff;
                continue;
            }
            timing!("http_meta_fetch_success_headers", req_time.elapsed());

            let buf = match hyper::body::to_bytes(res).await {
                Ok(buf) => buf,
                Err(err) => {
                    event!(Level::WARN, "hls request failed (body read error) - retrying in {:?}", failure_backoff);
                    event!(Level::ERROR, "error: {:?}", err);
                    delay_for(failure_backoff).await;
                    failure_backoff += failure_backoff;
                    continue;
                }
            };
            timing!("http_meta_fetch_success", req_time.elapsed());

            if let Err(err) = parse_m3u(&buf, &mut ele) {
                event!(Level::WARN, "hls request failed (body parse error) - retrying in {:?}", failure_backoff);
                event!(Level::ERROR, "error processing hls - {}", err);
                delay_for(failure_backoff).await;
                failure_backoff += failure_backoff;
                continue;
            }

            let mut previously_seen = 0;
            let mut new_items_seen = 0;
            for mut item in ele.drain(..) {
                while ctx.seen_uris.len() > 500 {
                    ctx.seen_uris.pop_front();
                }

                if ctx.seen_uris.insert(item.media_uri.clone(), ()).is_some() {
                    previously_seen += 1;
                } else {
                    new_items_seen += 1;
                    item.request_index = item_counter;
                    item_counter += 1;

                    if tx.send(item).await.is_err() {
                        return;
                    }
                }
            }

            event!(Level::DEBUG,
                new_items = new_items_seen,
                total_items = previously_seen + new_items_seen,
                 "loaded new playlist");

            if new_items_seen > 0 {
                last_new_item_seen = Instant::now();
            }

            counter!("meta_request_count", 1);
            counter!("meta_items_seen_new", new_items_seen);
            counter!("meta_items_seen_previously", previously_seen);
            value!("meta_items_seen_new2", new_items_seen);
            value!("meta_items_seen_previously2", previously_seen);

            if new_items_seen > previously_seen {
                request_interval *= 4;
                request_interval /= 5;
            } else {
                request_interval += request_interval / 3;
            }

            timing!("meta_request_interval", request_interval);

            // TODO: don't hardcode
            if Duration::from_secs(60) < last_new_item_seen.elapsed() {
                return;
            }
            
            delay_for(request_interval).await;
        }
    });

    rx
}

fn deprefix<'a>(prefix: &str, data: &'a str) -> Option<&'a str> {
    if data.starts_with(prefix) {
        Some(&data[prefix.len()..])
    } else {
        None
    }
}