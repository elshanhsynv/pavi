//! Files read over HTTP: `https://`, `http://`, `s3://`, `gs://` and `az://`
//! URLs (ADR 0008).
//!
//! Requests are blocking and run on the caller's thread (a runtime worker). A
//! Parquet object is read with `Range` requests through [`RemoteReader`], which
//! implements the `parquet` crate's `ChunkReader`; anything else is downloaded
//! to a temporary file first.

use std::{
    collections::VecDeque,
    fs::File,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use parquet::{
    errors::ParquetError,
    file::reader::{ChunkReader, Length},
};
use ring::{digest, hmac};

use crate::source::Cancelled;

/// Fetched byte ranges kept per remote object, so paging through a row group
/// doesn't download its column chunks again.
pub const REMOTE_CACHE_BYTES: usize = 256 * 1024 * 1024;
/// Larger ranges are fetched but not kept.
pub const MAX_CACHED_RANGE_BYTES: usize = 128 * 1024 * 1024;
/// Largest object downloaded whole (servers without range requests, CSV, JSON
/// and Arrow objects).
pub const MAX_DOWNLOAD_BYTES: u64 = 32 * 1024 * 1024 * 1024;
/// Bytes asked for at a time by a reader that reads a range sequentially.
const READ_BLOCK_BYTES: u64 = 1024 * 1024;
/// Attempts per request, for connection errors and 5xx answers.
const ATTEMPTS: u32 = 3;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);
const BODY_TIMEOUT: Duration = Duration::from_secs(300);
/// SHA-256 of an empty body, the payload hash of a signed GET.
const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
const AZURE_API_VERSION: &str = "2021-08-06";

/// `true` for a path that is really a URL PAVI reads over HTTP.
pub fn is_remote(path: &Path) -> bool {
    path.to_str().is_some_and(|text| {
        let lower = text.to_ascii_lowercase();
        ["https://", "http://", "s3://", "gs://", "az://"]
            .iter()
            .any(|scheme| lower.starts_with(scheme))
    })
}

/// The path to show for `path`: a URL without its query string, which may carry
/// a signature or token; a local path as it is.
pub fn display_path(path: &Path) -> PathBuf {
    path_part(path)
}

/// A URL's path without its query string, for deciding the file format.
pub(crate) fn path_part(path: &Path) -> PathBuf {
    match path.to_str() {
        Some(text) if is_remote(path) => {
            PathBuf::from(text.split(['?', '#']).next().unwrap_or(text))
        }
        _ => path.to_path_buf(),
    }
}

/// S3 credentials for Signature Version 4.
#[derive(Clone, Debug, PartialEq)]
struct Credentials {
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
enum Auth {
    Anonymous,
    Bearer(String),
    SigV4 {
        credentials: Credentials,
        region: String,
    },
}

/// Where a request goes and how it is authorised.
#[derive(Clone, Debug, PartialEq)]
struct Target {
    /// Full URL, as sent.
    url: String,
    /// Host header value, for signing.
    host: String,
    /// The URL's path, percent-encoded, starting with `/`.
    path: String,
    auth: Auth,
    headers: Vec<(String, String)>,
    /// S3 answers a request to the wrong region with a redirect that has no
    /// `Location`; it must come back to [`RemoteObject::open`], not be followed.
    follow_redirects: bool,
}

/// One remote object: its size, and a cache of the ranges read from it.
pub(crate) struct RemoteObject {
    target: Target,
    length: u64,
    modified: Option<SystemTime>,
    ranges: bool,
    cache: Mutex<RangeCache>,
}

impl std::fmt::Debug for RemoteObject {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RemoteObject")
            .field("length", &self.length)
            .finish_non_exhaustive()
    }
}

impl RemoteObject {
    /// Finds the object at `url` and its size with a one-byte range request.
    pub(crate) fn open(url: &str) -> Result<Self> {
        let mut target = resolve(url, &Environment::from_process())?;
        let mut probe = request(&target, Some((0, 0)))?;
        // S3 answers a request to the wrong region with the bucket's region.
        if let Some(region) = probe
            .headers()
            .get("x-amz-bucket-region")
            .and_then(|value| value.to_str().ok())
            .filter(|_| matches!(probe.status().as_u16(), 301 | 400))
            .map(str::to_owned)
        {
            let mut environment = Environment::from_process();
            environment.region = Some(region);
            target = resolve(url, &environment)?;
            probe = request(&target, Some((0, 0)))?;
        }
        let status = probe.status().as_u16();
        check_status(status, url, &target)?;
        let header = |name: &str| {
            probe
                .headers()
                .get(name)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned)
        };
        let modified = header("last-modified").and_then(|text| parse_http_date(&text));
        let (length, ranges) = match status {
            206 => {
                let total = header("content-range")
                    .and_then(|range| range.rsplit('/').next().map(str::to_owned))
                    .and_then(|total| total.parse::<u64>().ok())
                    .ok_or_else(|| anyhow!("{url}: the server sent no object size"))?;
                (total, true)
            }
            // The whole object came back: the server ignores ranges.
            _ => (
                header("content-length")
                    .and_then(|length| length.parse::<u64>().ok())
                    .unwrap_or(0),
                false,
            ),
        };
        Ok(Self {
            target,
            length,
            modified,
            ranges,
            cache: Mutex::new(RangeCache::default()),
        })
    }

    pub(crate) fn len(&self) -> u64 {
        self.length
    }

    /// The HTTPS URL requests go to (with any SAS token).
    pub(crate) fn url(&self) -> &str {
        &self.target.url
    }

    pub(crate) fn modified(&self) -> Option<SystemTime> {
        self.modified
    }

    /// Whether the server answers range requests; if not, read it by downloading.
    pub(crate) fn supports_ranges(&self) -> bool {
        self.ranges
    }

    /// `length` bytes from `start`, from the cache or with one range request.
    pub(crate) fn read_range(&self, start: u64, length: usize) -> Result<Bytes> {
        if length == 0 {
            return Ok(Bytes::new());
        }
        let end = start
            .checked_add(length as u64)
            .filter(|end| *end <= self.length)
            .ok_or_else(|| anyhow!("read past the end of the remote object"))?;
        if let Some(bytes) = self
            .cache
            .lock()
            .map_err(|_| anyhow!("remote cache lock poisoned"))?
            .get(start, length)
        {
            return Ok(bytes);
        }
        let response = request(&self.target, Some((start, end - 1)))?;
        let status = response.status().as_u16();
        if status != 206 {
            check_status(status, &self.target.url, &self.target)?;
            bail!("the server stopped answering range requests (status {status})");
        }
        let body = response
            .into_body()
            .into_with_config()
            .limit(length as u64 + 1)
            .read_to_vec()
            .context("read a remote byte range")?;
        if body.len() != length {
            bail!(
                "the server sent {} bytes for a {length}-byte range",
                body.len()
            );
        }
        let bytes = Bytes::from(body);
        if length <= MAX_CACHED_RANGE_BYTES {
            self.cache
                .lock()
                .map_err(|_| anyhow!("remote cache lock poisoned"))?
                .insert(start, bytes.clone());
        }
        Ok(bytes)
    }

    /// Downloads the whole object to `path`. `checkpoint` gets the share done
    /// (0 to 1) and cancels the download by returning `true`.
    pub(crate) fn download(&self, path: &Path, checkpoint: &dyn Fn(f64) -> bool) -> Result<()> {
        let response = request(&self.target, None)?;
        let status = response.status().as_u16();
        check_status(status, &self.target.url, &self.target)?;
        let mut reader = response
            .into_body()
            .into_with_config()
            .limit(MAX_DOWNLOAD_BYTES)
            .reader();
        let mut file = std::io::BufWriter::new(
            File::create(path).with_context(|| format!("create {}", path.display()))?,
        );
        let mut buffer = vec![0_u8; 256 * 1024];
        let mut written = 0_u64;
        loop {
            if checkpoint(written as f64 / self.length.max(1) as f64) {
                return Err(Cancelled.into());
            }
            let read = reader
                .read(&mut buffer)
                .context("download the remote file")?;
            if read == 0 {
                break;
            }
            written += read as u64;
            if written > MAX_DOWNLOAD_BYTES {
                bail!(
                    "the remote file is larger than {} GB",
                    MAX_DOWNLOAD_BYTES / (1024 * 1024 * 1024)
                );
            }
            file.write_all(&buffer[..read])?;
        }
        file.flush()?;
        Ok(())
    }
}

/// The ranges fetched most recently, at most [`REMOTE_CACHE_BYTES`] together.
#[derive(Default)]
struct RangeCache {
    ranges: VecDeque<(u64, Bytes)>,
    bytes: usize,
}

impl RangeCache {
    /// A slice of a cached range that holds `[start, start + length)`.
    fn get(&mut self, start: u64, length: usize) -> Option<Bytes> {
        let position = self.ranges.iter().position(|(cached, bytes)| {
            *cached <= start && start + length as u64 <= cached + bytes.len() as u64
        })?;
        let entry = self.ranges.remove(position)?;
        let offset = (start - entry.0) as usize;
        let slice = entry.1.slice(offset..offset + length);
        // Most recently used last.
        self.ranges.push_back(entry);
        Some(slice)
    }

    fn insert(&mut self, start: u64, bytes: Bytes) {
        self.bytes += bytes.len();
        self.ranges.push_back((start, bytes));
        while self.bytes > REMOTE_CACHE_BYTES {
            match self.ranges.pop_front() {
                Some((_, evicted)) => self.bytes -= evicted.len(),
                None => break,
            }
        }
    }
}

/// Reads a remote object for the `parquet` crate.
#[derive(Clone, Debug)]
pub(crate) struct RemoteReader(pub(crate) Arc<RemoteObject>);

impl Length for RemoteReader {
    fn len(&self) -> u64 {
        self.0.len()
    }
}

impl ChunkReader for RemoteReader {
    type T = RangeReader;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        Ok(RangeReader {
            object: Arc::clone(&self.0),
            position: start,
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        self.0
            .read_range(start, length)
            .map_err(|error| ParquetError::External(error.into()))
    }
}

/// Reads a remote object from a position to its end, a block at a time.
pub(crate) struct RangeReader {
    object: Arc<RemoteObject>,
    position: u64,
}

impl Read for RangeReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let left = self.object.len().saturating_sub(self.position);
        let length = left.min(READ_BLOCK_BYTES).min(buffer.len() as u64) as usize;
        if length == 0 {
            return Ok(0);
        }
        // Whole blocks, so a sequential reader hits the cache.
        let block_start = self.position - self.position % READ_BLOCK_BYTES;
        let block_length = (self.object.len() - block_start).min(READ_BLOCK_BYTES) as usize;
        let block = self
            .object
            .read_range(block_start, block_length)
            .map_err(std::io::Error::other)?;
        let offset = (self.position - block_start) as usize;
        let length = length.min(block.len() - offset);
        buffer[..length].copy_from_slice(&block[offset..offset + length]);
        self.position += length as u64;
        Ok(length)
    }
}

/// The settings a URL is resolved with: credentials and endpoints from the
/// environment and `~/.aws/credentials`.
#[derive(Clone, Debug, Default)]
struct Environment {
    region: Option<String>,
    s3_endpoint: Option<String>,
    aws: Option<Credentials>,
    google_token: Option<String>,
    azure_sas: Option<String>,
}

impl Environment {
    fn from_process() -> Self {
        let variable = |name: &str| {
            std::env::var(name)
                .ok()
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty())
        };
        let aws = match (
            variable("AWS_ACCESS_KEY_ID"),
            variable("AWS_SECRET_ACCESS_KEY"),
        ) {
            (Some(access_key), Some(secret_key)) => Some(Credentials {
                access_key,
                secret_key,
                session_token: variable("AWS_SESSION_TOKEN"),
            }),
            _ => aws_credentials_file(
                variable("AWS_SHARED_CREDENTIALS_FILE"),
                &variable("AWS_PROFILE").unwrap_or_else(|| "default".to_owned()),
            ),
        };
        Self {
            region: variable("AWS_REGION").or_else(|| variable("AWS_DEFAULT_REGION")),
            s3_endpoint: variable("AWS_ENDPOINT_URL_S3").or_else(|| variable("AWS_ENDPOINT_URL")),
            aws,
            google_token: variable("GOOGLE_OAUTH_ACCESS_TOKEN"),
            azure_sas: variable("AZURE_STORAGE_SAS_TOKEN"),
        }
    }
}

/// Reads a profile from the AWS shared credentials file.
fn aws_credentials_file(path: Option<String>, profile: &str) -> Option<Credentials> {
    let path = path.map(PathBuf::from).or_else(|| {
        std::env::var_os("HOME")
            .or_else(|| std::env::var_os("USERPROFILE"))
            .map(|home| PathBuf::from(home).join(".aws").join("credentials"))
    })?;
    let text = std::fs::read_to_string(path).ok()?;
    parse_aws_credentials(&text, profile)
}

fn parse_aws_credentials(text: &str, profile: &str) -> Option<Credentials> {
    let mut in_profile = false;
    let (mut access_key, mut secret_key, mut session_token) = (None, None, None);
    for line in text.lines().map(str::trim) {
        if let Some(name) = line
            .strip_prefix('[')
            .and_then(|line| line.strip_suffix(']'))
        {
            in_profile = name.trim() == profile;
            continue;
        }
        if !in_profile {
            continue;
        }
        if let Some((key, value)) = line.split_once('=') {
            let value = Some(value.trim().to_owned());
            match key.trim() {
                "aws_access_key_id" => access_key = value,
                "aws_secret_access_key" => secret_key = value,
                "aws_session_token" => session_token = value,
                _ => {}
            }
        }
    }
    Some(Credentials {
        access_key: access_key?,
        secret_key: secret_key?,
        session_token,
    })
}

/// Turns a URL into the HTTPS request that reads it.
fn resolve(url: &str, environment: &Environment) -> Result<Target> {
    let (scheme, rest) = url
        .split_once("://")
        .ok_or_else(|| anyhow!("{url} is not a URL"))?;
    let scheme = scheme.to_ascii_lowercase();
    let split = |rest: &str| -> Result<(String, String)> {
        let (first, key) = rest.split_once('/').unwrap_or((rest, ""));
        if first.is_empty() || key.is_empty() || key.ends_with('/') {
            bail!("{url} names a folder; PAVI opens one remote file at a time");
        }
        Ok((first.to_owned(), key.to_owned()))
    };
    match scheme.as_str() {
        "http" | "https" => {
            let host = rest.split(['/', '?', '#']).next().unwrap_or_default();
            let path = rest[host.len()..].split(['?', '#']).next().unwrap_or("/");
            Ok(Target {
                url: url.to_owned(),
                host: host.to_owned(),
                path: if path.is_empty() { "/" } else { path }.to_owned(),
                auth: Auth::Anonymous,
                headers: Vec::new(),
                follow_redirects: true,
            })
        }
        "s3" => {
            let (bucket, key) = split(rest)?;
            let region = environment
                .region
                .clone()
                .unwrap_or_else(|| "us-east-1".to_owned());
            let key = encode_path(&key);
            let (base, host, path) = match &environment.s3_endpoint {
                Some(endpoint) => {
                    let endpoint = endpoint.trim_end_matches('/');
                    let host = endpoint
                        .split_once("://")
                        .map_or(endpoint, |(_, host)| host)
                        .split('/')
                        .next()
                        .unwrap_or_default()
                        .to_owned();
                    (endpoint.to_owned(), host, format!("/{bucket}/{key}"))
                }
                // Dotted bucket names don't match the wildcard certificate of
                // virtual-hosted URLs, so they use the path style.
                None if bucket.contains('.') => {
                    let host = format!("s3.{region}.amazonaws.com");
                    (format!("https://{host}"), host, format!("/{bucket}/{key}"))
                }
                None => {
                    let host = format!("{bucket}.s3.{region}.amazonaws.com");
                    (format!("https://{host}"), host, format!("/{key}"))
                }
            };
            Ok(Target {
                url: format!("{base}{path}"),
                host,
                path,
                auth: match &environment.aws {
                    Some(credentials) => Auth::SigV4 {
                        credentials: credentials.clone(),
                        region,
                    },
                    None => Auth::Anonymous,
                },
                headers: Vec::new(),
                follow_redirects: false,
            })
        }
        "gs" => {
            let (bucket, key) = split(rest)?;
            let host = "storage.googleapis.com".to_owned();
            let path = format!("/{bucket}/{}", encode_path(&key));
            Ok(Target {
                url: format!("https://{host}{path}"),
                host,
                path,
                auth: environment
                    .google_token
                    .clone()
                    .map_or(Auth::Anonymous, Auth::Bearer),
                headers: Vec::new(),
                follow_redirects: true,
            })
        }
        "az" => {
            let (account, rest) = split(rest)?;
            let (container, blob) = split(&rest)?;
            let host = format!("{account}.blob.core.windows.net");
            let path = format!("/{container}/{}", encode_path(&blob));
            let query = environment
                .azure_sas
                .as_deref()
                .map(|sas| format!("?{}", sas.trim_start_matches('?')))
                .unwrap_or_default();
            Ok(Target {
                url: format!("https://{host}{path}{query}"),
                host,
                path,
                auth: Auth::Anonymous,
                headers: vec![("x-ms-version".to_owned(), AZURE_API_VERSION.to_owned())],
                follow_redirects: true,
            })
        }
        _ => bail!("{url}: PAVI reads https, http, s3, gs and az URLs"),
    }
}

/// Percent-encodes an object key for a URL path, keeping `/`.
fn encode_path(key: &str) -> String {
    let mut encoded = String::with_capacity(key.len());
    for byte in key.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~' | b'/') {
            encoded.push(byte as char);
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

fn agent() -> &'static ureq::Agent {
    static AGENT: OnceLock<ureq::Agent> = OnceLock::new();
    AGENT.get_or_init(|| {
        // The operating system's certificate store (no bundled root list).
        let roots = rustls_native_certs::load_native_certs()
            .certs
            .iter()
            .map(|certificate| ureq::tls::Certificate::from_der(certificate.as_ref()).to_owned())
            .collect::<Vec<_>>();
        ureq::Agent::config_builder()
            .http_status_as_error(false)
            .user_agent("pavi")
            .max_redirects(5)
            .timeout_connect(Some(CONNECT_TIMEOUT))
            .timeout_recv_response(Some(RESPONSE_TIMEOUT))
            .timeout_recv_body(Some(BODY_TIMEOUT))
            .tls_config(
                ureq::tls::TlsConfig::builder()
                    .root_certs(ureq::tls::RootCerts::new_with_certs(&roots))
                    .build(),
            )
            .build()
            .new_agent()
    })
}

/// Sends a GET for `range` (inclusive) or the whole object, retrying
/// connection errors and 5xx answers.
fn request(target: &Target, range: Option<(u64, u64)>) -> Result<ureq::http::Response<ureq::Body>> {
    let mut last_error = None;
    for attempt in 0..ATTEMPTS {
        if attempt > 0 {
            std::thread::sleep(Duration::from_millis(250 << attempt));
        }
        let mut headers = target.headers.clone();
        if let Some((first, last)) = range {
            headers.push(("range".to_owned(), format!("bytes={first}-{last}")));
        }
        match &target.auth {
            Auth::Anonymous => {}
            Auth::Bearer(token) => {
                headers.push(("authorization".to_owned(), format!("Bearer {token}")))
            }
            Auth::SigV4 {
                credentials,
                region,
            } => sign_v4(target, credentials, region, &mut headers, SystemTime::now()),
        }
        let mut builder = agent().get(&target.url);
        if !target.follow_redirects {
            builder = builder.config().max_redirects(0).build();
        }
        for (name, value) in &headers {
            builder = builder.header(name.as_str(), value.as_str());
        }
        match builder.call() {
            Ok(response) if response.status().is_server_error() => {
                last_error = Some(anyhow!("server error {}", response.status()));
            }
            Ok(response) => return Ok(response),
            Err(error) => last_error = Some(anyhow!(error)),
        }
    }
    Err(last_error
        .unwrap_or_else(|| anyhow!("no attempt made"))
        .context(format!("request {}", redact(&target.url))))
}

/// `url` without its query string, which may carry a signature or token.
pub(crate) fn redact(url: &str) -> &str {
    url.split('?').next().unwrap_or(url)
}

/// Turns an error status into a message that says what to do.
fn check_status(status: u16, url: &str, target: &Target) -> Result<()> {
    let url = redact(url);
    match status {
        200..=299 => Ok(()),
        401 | 403 => {
            let hint = match (&target.auth, url.split("://").next().unwrap_or_default()) {
                (Auth::Anonymous, "s3") => {
                    "; set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or add them to ~/.aws/credentials"
                }
                (Auth::Anonymous, "gs") => {
                    "; set GOOGLE_OAUTH_ACCESS_TOKEN (gcloud auth print-access-token)"
                }
                (Auth::Anonymous, "az") => "; set AZURE_STORAGE_SAS_TOKEN",
                _ => "",
            };
            bail!("access to {url} was denied (HTTP {status}){hint}")
        }
        404 => bail!("{url} was not found"),
        416 => bail!("{url} is empty"),
        _ => bail!("{url}: the server answered HTTP {status}"),
    }
}

/// Adds AWS Signature Version 4 headers for a GET of `target`.
fn sign_v4(
    target: &Target,
    credentials: &Credentials,
    region: &str,
    headers: &mut Vec<(String, String)>,
    now: SystemTime,
) {
    let (date, time) = utc_date_time(now);
    let stamp = format!("{date}T{time}Z");
    headers.push(("x-amz-content-sha256".to_owned(), EMPTY_SHA256.to_owned()));
    headers.push(("x-amz-date".to_owned(), stamp.clone()));
    if let Some(token) = &credentials.session_token {
        headers.push(("x-amz-security-token".to_owned(), token.clone()));
    }
    let mut signed = headers
        .iter()
        .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_owned()))
        .collect::<Vec<_>>();
    signed.push(("host".to_owned(), target.host.clone()));
    signed.sort();
    let canonical_headers = signed
        .iter()
        .map(|(name, value)| format!("{name}:{value}\n"))
        .collect::<String>();
    let signed_names = signed
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let canonical_request = format!(
        "GET\n{}\n\n{canonical_headers}\n{signed_names}\n{EMPTY_SHA256}",
        target.path
    );
    let scope = format!("{date}/{region}/s3/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{stamp}\n{scope}\n{}",
        hex(digest::digest(&digest::SHA256, canonical_request.as_bytes()).as_ref())
    );
    let mut key = format!("AWS4{}", credentials.secret_key).into_bytes();
    for part in [date.as_str(), region, "s3", "aws4_request"] {
        key = hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, &key), part.as_bytes())
            .as_ref()
            .to_vec();
    }
    let signature = hex(hmac::sign(
        &hmac::Key::new(hmac::HMAC_SHA256, &key),
        string_to_sign.as_bytes(),
    )
    .as_ref());
    headers.push((
        "authorization".to_owned(),
        format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_names}, Signature={signature}",
            credentials.access_key
        ),
    ));
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// `(YYYYMMDD, HHMMSS)` in UTC.
fn utc_date_time(time: SystemTime) -> (String, String) {
    let seconds = time
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs());
    let (year, month, day) = civil_from_days((seconds / 86_400) as i64);
    let of_day = seconds % 86_400;
    (
        format!("{year:04}{month:02}{day:02}"),
        format!(
            "{:02}{:02}{:02}",
            of_day / 3600,
            of_day / 60 % 60,
            of_day % 60
        ),
    )
}

/// Days since 1970-01-01 to a calendar date (Howard Hinnant's algorithm).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = (day_of_year - (153 * month_index + 2) / 5 + 1) as u32;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    } as u32;
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// Parses an HTTP date such as `Sun, 06 Nov 1994 08:49:37 GMT`.
fn parse_http_date(text: &str) -> Option<SystemTime> {
    let parts = text.split_whitespace().collect::<Vec<_>>();
    let [_, day, month, year, time, "GMT"] = parts.as_slice() else {
        return None;
    };
    let day = day.parse::<i64>().ok()?;
    let month = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ]
    .iter()
    .position(|name| name == month)? as i64
        + 1;
    let year = year.parse::<i64>().ok()?;
    let mut clock = time.split(':').map(|part| part.parse::<i64>().ok());
    let (hour, minute, second) = (clock.next()??, clock.next()??, clock.next()??);
    // Days from civil (inverse of `civil_from_days`).
    let (y, m) = if month <= 2 {
        (year - 1, month + 9)
    } else {
        (year, month - 3)
    };
    let era = y.div_euclid(400);
    let year_of_era = y.rem_euclid(400);
    let day_of_year = (153 * m + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    let days = era * 146_097 + day_of_era - 719_468;
    let seconds = u64::try_from(days * 86_400 + hour * 3600 + minute * 60 + second).ok()?;
    Some(UNIX_EPOCH + Duration::from_secs(seconds))
}

#[cfg(test)]
mod tests {
    use std::{
        io::{BufRead, BufReader},
        net::TcpListener,
    };

    use super::*;

    #[test]
    fn recognises_urls_and_strips_queries_for_the_format() {
        assert!(is_remote(Path::new("s3://bucket/a.parquet")));
        assert!(is_remote(Path::new("HTTPS://example.com/a.csv")));
        assert!(!is_remote(Path::new("C:\\data\\a.parquet")));
        assert!(!is_remote(Path::new("data/a.parquet")));
        assert_eq!(
            path_part(Path::new("https://h/x/a.csv?sig=1&b=2")),
            PathBuf::from("https://h/x/a.csv")
        );
    }

    #[test]
    fn resolves_cloud_urls_to_https_requests() {
        let environment = Environment {
            region: Some("eu-west-1".into()),
            azure_sas: Some("?sv=1&sig=x".into()),
            google_token: Some("token".into()),
            ..Environment::default()
        };
        let s3 = resolve("s3://my-bucket/dir/a b.parquet", &environment).unwrap();
        assert_eq!(
            s3.url,
            "https://my-bucket.s3.eu-west-1.amazonaws.com/dir/a%20b.parquet"
        );
        assert_eq!(s3.auth, Auth::Anonymous);
        let dotted = resolve("s3://my.bucket/a.parquet", &environment).unwrap();
        assert_eq!(
            dotted.url,
            "https://s3.eu-west-1.amazonaws.com/my.bucket/a.parquet"
        );
        let minio = resolve(
            "s3://b/k.parquet",
            &Environment {
                s3_endpoint: Some("http://localhost:9000/".into()),
                ..Environment::default()
            },
        )
        .unwrap();
        assert_eq!(minio.url, "http://localhost:9000/b/k.parquet");
        assert_eq!(minio.host, "localhost:9000");

        let gs = resolve("gs://bucket/k.parquet", &environment).unwrap();
        assert_eq!(gs.url, "https://storage.googleapis.com/bucket/k.parquet");
        assert_eq!(gs.auth, Auth::Bearer("token".into()));
        let az = resolve("az://acct/container/dir/k.parquet", &environment).unwrap();
        assert_eq!(
            az.url,
            "https://acct.blob.core.windows.net/container/dir/k.parquet?sv=1&sig=x"
        );

        assert!(resolve("s3://bucket/dir/", &environment).is_err());
        assert!(resolve("s3://bucket", &environment).is_err());
        assert!(resolve("ftp://h/a", &environment).is_err());
    }

    /// The "GET Object" example from the AWS Signature Version 4 documentation.
    #[test]
    fn signs_requests_like_the_aws_example() {
        let target = Target {
            url: "https://examplebucket.s3.amazonaws.com/test.txt".into(),
            host: "examplebucket.s3.amazonaws.com".into(),
            path: "/test.txt".into(),
            auth: Auth::Anonymous,
            headers: Vec::new(),
            follow_redirects: false,
        };
        let credentials = Credentials {
            access_key: "AKIAIOSFODNN7EXAMPLE".into(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
            session_token: None,
        };
        let mut headers = vec![("range".to_owned(), "bytes=0-9".to_owned())];
        let when = UNIX_EPOCH + Duration::from_secs(1_369_353_600); // 2013-05-24T00:00:00Z
        sign_v4(&target, &credentials, "us-east-1", &mut headers, when);
        let authorization = &headers.last().unwrap().1;
        assert_eq!(
            authorization,
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
             SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
             Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }

    #[test]
    fn reads_credentials_files_and_http_dates() {
        let file = "[default]\naws_access_key_id = A\naws_secret_access_key=S\n\n[work]\naws_access_key_id=W\naws_secret_access_key=X\naws_session_token=T\n";
        assert_eq!(
            parse_aws_credentials(file, "work"),
            Some(Credentials {
                access_key: "W".into(),
                secret_key: "X".into(),
                session_token: Some("T".into())
            })
        );
        assert_eq!(
            parse_aws_credentials(file, "default").unwrap().access_key,
            "A"
        );
        assert_eq!(parse_aws_credentials(file, "missing"), None);
        assert_eq!(
            parse_http_date("Sun, 06 Nov 1994 08:49:37 GMT"),
            Some(UNIX_EPOCH + Duration::from_secs(784_111_777))
        );
        assert_eq!(parse_http_date("yesterday"), None);
        assert_eq!(
            utc_date_time(UNIX_EPOCH + Duration::from_secs(951_782_400)),
            ("20000229".to_owned(), "000000".to_owned())
        );
    }

    #[test]
    fn cache_serves_contained_ranges_and_stays_bounded() {
        let mut cache = RangeCache::default();
        cache.insert(100, Bytes::from(vec![7_u8; 50]));
        assert_eq!(cache.get(110, 10).unwrap().len(), 10);
        assert!(cache.get(140, 20).is_none());
        let big = MAX_CACHED_RANGE_BYTES;
        cache.insert(1_000, Bytes::from(vec![0_u8; big]));
        cache.insert(2_000_000_000, Bytes::from(vec![0_u8; big]));
        cache.insert(4_000_000_000, Bytes::from(vec![0_u8; big]));
        assert!(cache.bytes <= REMOTE_CACHE_BYTES);
        assert!(cache.get(110, 10).is_none(), "the oldest range was evicted");
    }

    /// Serves `body` over plain HTTP on localhost, answering `Range` requests
    /// when `ranges` is set. Returns the base URL.
    pub(crate) fn serve(body: Vec<u8>, ranges: bool) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut range = None;
                let mut path = String::new();
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                        break;
                    }
                    if line.starts_with("GET ") {
                        path = line
                            .split_whitespace()
                            .nth(1)
                            .unwrap_or_default()
                            .to_owned();
                    }
                    let lower = line.to_ascii_lowercase();
                    if let Some(value) = lower.strip_prefix("range: bytes=") {
                        let (first, last) = value.trim().split_once('-').unwrap();
                        range = Some((
                            first.parse::<usize>().unwrap(),
                            last.parse::<usize>().unwrap(),
                        ));
                    }
                }
                let response = if path.contains("missing") {
                    b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        .to_vec()
                } else if let (true, Some((first, last))) = (ranges, range) {
                    let last = last.min(body.len() - 1);
                    let mut response = format!(
                        "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {first}-{last}/{}\r\n\
                         Content-Length: {}\r\nLast-Modified: Sun, 06 Nov 1994 08:49:37 GMT\r\n\
                         Connection: close\r\n\r\n",
                        body.len(),
                        last + 1 - first
                    )
                    .into_bytes();
                    response.extend_from_slice(&body[first..=last]);
                    response
                } else {
                    let mut response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .into_bytes();
                    response.extend_from_slice(&body);
                    response
                };
                let _ = stream.write_all(&response);
            }
        });
        format!("http://{address}")
    }

    #[test]
    fn opens_parquet_and_csv_files_over_http() {
        use arrow_array::{ArrayRef, Int32Array, RecordBatch};
        use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};

        use crate::{FileFormat, FilterExpr, ParquetSource, Projection};

        let batch = RecordBatch::try_from_iter(vec![(
            "id",
            Arc::new(Int32Array::from_iter_values(0..1_000)) as ArrayRef,
        )])
        .unwrap();
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(
            &mut bytes,
            batch.schema(),
            Some(
                WriterProperties::builder()
                    .set_max_row_group_row_count(Some(300))
                    .build(),
            ),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let ids = |source: &ParquetSource, first: u64, count: usize| {
            let window = source
                .read_window(first, count, &Projection::all(1))
                .unwrap();
            window
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        };
        // Range requests: read in place, across row groups, with filters.
        let ranged = serve(bytes.clone(), true);
        let source = ParquetSource::open(format!("{ranged}/dir/data.parquet")).unwrap();
        assert_eq!(source.row_count(), 1_000);
        assert_eq!(source.row_groups().len(), 4);
        assert_eq!(source.total_bytes(), bytes.len() as u64);
        assert_eq!(ids(&source, 298, 4), [298, 299, 300, 301]);
        let filtered = source
            .read_filtered_window(
                &FilterExpr::parse("id >= 995").unwrap(),
                0,
                10,
                &Projection::all(1),
            )
            .unwrap();
        assert_eq!(filtered.num_rows(), 5);
        assert!(!source.is_downloaded());

        // No range requests: downloaded first.
        let plain = serve(bytes, false);
        let source = ParquetSource::open(format!("{plain}/data.parquet")).unwrap();
        assert_eq!(ids(&source, 998, 2), [998, 999]);
        assert!(source.is_downloaded());

        // A CSV URL with a query string (as a pre-signed URL has).
        let csv = serve(b"a,b\n1,x\n2,y\n".to_vec(), true);
        let source = ParquetSource::open(format!("{csv}/t.csv?X-Amz-Signature=abc")).unwrap();
        assert_eq!(source.format(), FileFormat::Csv);
        assert_eq!(source.row_count(), 2);

        let error = format!(
            "{:#}",
            ParquetSource::open(format!("{csv}/missing.parquet?token=secret"))
                .err()
                .unwrap()
        );
        assert!(error.contains("not found"), "{error}");
        assert!(
            !error.contains("secret"),
            "the query string is not shown: {error}"
        );
    }

    #[test]
    fn reads_ranges_and_downloads_from_a_server() {
        let body = (0..=255_u8)
            .cycle()
            .take(3 * READ_BLOCK_BYTES as usize + 10)
            .collect::<Vec<_>>();
        let base = serve(body.clone(), true);
        let object = RemoteObject::open(&format!("{base}/data.bin")).unwrap();
        assert!(object.supports_ranges());
        assert_eq!(object.len(), body.len() as u64);
        assert_eq!(
            object.modified(),
            Some(UNIX_EPOCH + Duration::from_secs(784_111_777))
        );
        assert_eq!(&object.read_range(5, 4).unwrap()[..], &body[5..9]);
        assert!(object.read_range(body.len() as u64 - 2, 4).is_err());

        let reader = RemoteReader(Arc::new(object));
        let mut all = Vec::new();
        reader.get_read(7).unwrap().read_to_end(&mut all).unwrap();
        assert_eq!(all, body[7..]);

        let error = RemoteObject::open(&format!("{base}/missing.parquet"))
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("not found"), "{error}");

        // A server without range requests is read by downloading.
        let plain = serve(body.clone(), false);
        let object = RemoteObject::open(&format!("{plain}/data.bin")).unwrap();
        assert!(!object.supports_ranges());
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("download");
        object.download(&path, &|_| false).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), body);
        assert!(
            object
                .download(&path, &|_| true)
                .unwrap_err()
                .downcast_ref::<Cancelled>()
                .is_some()
        );
    }
}
