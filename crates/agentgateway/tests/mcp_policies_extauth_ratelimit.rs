#![cfg(feature = "mcp_integration")]

// Thorough MCP policy tests for /sse: extAuthZ, local/remote rate-limit, isolation, concurrency,
// SSE smoke, and (opt-in) outage semantics. CI-friendly: skips if `npx` is missing.

use std::{
	collections::HashMap,
	io::Write,
	net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener},
	path::Path,
	process::Stdio,
	sync::{Arc, Mutex},
	time::Duration,
};

use assert_cmd::prelude::*;
use reqwest::StatusCode;
use tempfile::{NamedTempFile, TempPath};
use tokio::{
	net::TcpStream,
	process::Command,
	time::{Instant, sleep, timeout},
};
use which::which;

use futures::future::join_all;
use futures_util::stream::StreamExt;

use tonic::{Request, Response, transport::Server};

struct ManagedChild {
	child: Option<tokio::process::Child>,
	_temp_path: Option<TempPath>,
}

impl ManagedChild {
	fn new(child: tokio::process::Child) -> Self {
		Self {
			child: Some(child),
			_temp_path: None,
		}
	}

	fn with_temp(child: tokio::process::Child, temp_path: TempPath) -> Self {
		Self {
			child: Some(child),
			_temp_path: Some(temp_path),
		}
	}

	async fn kill(&mut self) {
		if let Some(mut child) = self.child.take() {
			let _ = child.start_kill();
			let _ = child.wait().await;
		}
	}
}

impl Drop for ManagedChild {
	fn drop(&mut self) {
		if let Some(child) = self.child.as_mut() {
			let _ = child.start_kill();
		}
	}
}

// Envoy extAuthZ (v3) and RateLimit (v3) generated types
use envoy_prost_tonic::envoy::service::auth::v3::{
	CheckRequest, CheckResponse, DeniedHttpResponse, OkHttpResponse,
	authorization_server::{Authorization, AuthorizationServer},
	check_response,
};
use envoy_prost_tonic::envoy::service::ratelimit::v3::{
	RateLimitRequest, RateLimitResponse,
	rate_limit_response::Code as RlCode,
	rate_limit_service_server::{RateLimitService, RateLimitServiceServer},
};
use envoy_prost_tonic::google::rpc::Status as GrpcStatus;

// ------------------------ helpers ------------------------

fn free_port() -> u16 {
	TcpListener::bind(("127.0.0.1", 0))
		.unwrap()
		.local_addr()
		.unwrap()
		.port()
}

async fn wait_for_tcp(addr: &str, timeout: Duration) -> std::io::Result<()> {
	let deadline = Instant::now() + timeout;
	loop {
		match TcpStream::connect(addr).await {
			Ok(stream) => {
				drop(stream);
				return Ok(());
			},
			Err(err) => {
				if Instant::now() >= deadline {
					return Err(err);
				}
				sleep(Duration::from_millis(250)).await;
			},
		}
	}
}

async fn wait_http_ready(url: &str, expected: &[StatusCode]) {
	let client = reqwest::Client::builder()
		.timeout(Duration::from_millis(800))
		.build()
		.unwrap();
	for _ in 0..80 {
		if let Ok(resp) = client
			.get(url)
			.header("accept", "text/event-stream")
			.send()
			.await
		{
			if expected.contains(&resp.status()) {
				return;
			}
		}
		sleep(Duration::from_millis(250)).await;
	}
	panic!("Gateway not ready at {url}");
}

async fn spawn_mcp_everything(npx_path: &Path) -> (ManagedChild, String, u16) {
	let port = free_port();
	let port_arg = port.to_string();
	let child = Command::new(npx_path)
		.arg("--yes")
		.arg("@modelcontextprotocol/server-everything")
		.arg("--port")
		.arg(&port_arg)
		.arg("--host")
		.arg("127.0.0.1")
		.stdin(Stdio::null())
		.stdout(Stdio::null())
		.stderr(Stdio::null())
		.spawn()
		.expect("failed to spawn server-everything");

	wait_for_tcp(&format!("127.0.0.1:{port}"), Duration::from_secs(10))
		.await
		.expect("MCP server failed to start in time");

	(ManagedChild::new(child), "everything".to_string(), port)
}

fn gateway_config_yaml(gw_port: u16, mcp_name: &str, mcp_port: u16, policies: &str) -> String {
	let mut rendered = String::new();
	for line in policies.lines() {
		rendered.push_str("      ");
		rendered.push_str(line);
		rendered.push('\n');
	}

	format!(
		r#"
binds:
- port: {gw_port}
  listeners:
  - name: default
    protocol: HTTP
    routes:
    - policies:
{rendered}      backends:
      - mcp:
          targets:
          - name: {mcp_name}
            sse:
              host: 127.0.0.1
              port: {mcp_port}
              path: /sse
"#
	)
}

async fn spawn_agentgateway_with_config(gw_port: u16, yaml: &str) -> ManagedChild {
	let mut tmp = NamedTempFile::new().expect("config tmp file");
	tmp.write_all(yaml.as_bytes()).unwrap();
	let temp_path = tmp.into_temp_path();

	let bin_path = assert_cmd::cargo::cargo_bin("agentgateway");

	let child = Command::new(bin_path)
		.arg("-f")
		.arg(temp_path.as_os_str())
		.stdout(Stdio::null())
		.stderr(Stdio::null())
		.spawn()
		.expect("failed to start agentgateway");

	wait_for_tcp(&format!("127.0.0.1:{gw_port}"), Duration::from_secs(10))
		.await
		.expect("agentgateway failed to start in time");
	ManagedChild::with_temp(child, temp_path)
}

async fn spawn_agentgateway(
	gw_port: u16,
	mcp_name: &str,
	mcp_port: u16,
	policies: &str,
) -> ManagedChild {
	let yaml = gateway_config_yaml(gw_port, mcp_name, mcp_port, policies);
	spawn_agentgateway_with_config(gw_port, &yaml).await
}

// Read first SSE chunk within timeout. Returns true if any bytes observed.
async fn read_some_sse_bytes(url: &str, header: Option<(&str, &str)>) -> bool {
	let client = reqwest::Client::new();
	let mut req = client.get(url).header("accept", "text/event-stream");
	if let Some((k, v)) = header {
		req = req.header(k, v);
	}
	let resp = req.send().await.expect("GET /sse");
	if resp.status() != StatusCode::OK {
		return false;
	}
	let mut stream = resp.bytes_stream();
	match timeout(Duration::from_secs(5), stream.next()).await {
		Ok(Some(Ok(b))) if !b.is_empty() => true,
		_ => false,
	}
}

// ------------------------ extAuthz servers ------------------------

#[derive(Default)]
struct DenyAllAuthz;

#[tonic::async_trait]
impl Authorization for DenyAllAuthz {
	async fn check(
		&self,
		_req: Request<CheckRequest>,
	) -> Result<Response<CheckResponse>, tonic::Status> {
		let resp = CheckResponse {
			status: Some(GrpcStatus {
				code: 7,
				message: "denied-by-test".into(),
				details: vec![],
			}), // PERMISSION_DENIED
			http_response: Some(check_response::HttpResponse::DeniedHttpResponse(
				DeniedHttpResponse {
					status: Some(envoy_prost_tonic::envoy::r#type::v3::HttpStatus { code: 403 }),
					body: "denied".into(),
					headers: vec![],
				},
			)),
			..Default::default()
		};
		Ok(Response::new(resp))
	}
}

async fn spawn_deny_extauthz_on(addr: SocketAddr) -> tokio::task::JoinHandle<()> {
	tokio::spawn(async move {
		Server::builder()
			.add_service(AuthorizationServer::new(DenyAllAuthz::default()))
			.serve(addr)
			.await
			.expect("authz server (deny)");
	})
}

#[derive(Default)]
struct AllowAllAuthz;

#[tonic::async_trait]
impl Authorization for AllowAllAuthz {
	async fn check(
		&self,
		_req: Request<CheckRequest>,
	) -> Result<Response<CheckResponse>, tonic::Status> {
		let resp = CheckResponse {
			status: Some(GrpcStatus {
				code: 0,
				message: "ok".into(),
				details: vec![],
			}),
			http_response: Some(check_response::HttpResponse::OkHttpResponse(
				OkHttpResponse {
					headers: vec![],
					..Default::default()
				},
			)),
			..Default::default()
		};
		Ok(Response::new(resp))
	}
}

async fn spawn_allow_extauthz_on(addr: SocketAddr) -> tokio::task::JoinHandle<()> {
	tokio::spawn(async move {
		Server::builder()
			.add_service(AuthorizationServer::new(AllowAllAuthz::default()))
			.serve(addr)
			.await
			.expect("authz server (allow)");
	})
}

// ------------------------ RLS (remote rate-limit) stub ------------------------

#[derive(Clone, Default)]
struct MemoryRls {
	counts: Arc<Mutex<HashMap<String, u32>>>,
	limit: u32,
}

#[tonic::async_trait]
impl RateLimitService for MemoryRls {
	async fn should_rate_limit(
		&self,
		req: Request<RateLimitRequest>,
	) -> Result<Response<RateLimitResponse>, tonic::Status> {
		let r = req.into_inner();
		let mut key = r.domain.clone();
		for d in r.descriptors {
			key.push('|');
			for e in d.entries {
				key.push_str(&format!("{}={};", e.key, e.value));
			}
		}
		let mut map = self.counts.lock().unwrap();
		let entry = map.entry(key).or_insert(0);
		*entry += 1;

		let over = *entry > self.limit;
		let code = if over { RlCode::OverLimit } else { RlCode::Ok } as i32;

		Ok(Response::new(RateLimitResponse {
			overall_code: code,
			..Default::default()
		}))
	}
}

async fn spawn_rls_on(addr: SocketAddr, limit: u32) -> tokio::task::JoinHandle<()> {
	let svc = MemoryRls {
		counts: Default::default(),
		limit,
	};
	tokio::spawn(async move {
		Server::builder()
			.add_service(RateLimitServiceServer::new(svc))
			.serve(addr)
			.await
			.expect("rls server");
	})
}

// =============================== TESTS ===============================

#[tokio::test(flavor = "multi_thread")]
async fn mcp_extauth_denies_unauthorized() {
	if which("npx").is_err() && which("npx.cmd").is_err() {
		eprintln!("skipping MCP integration test: `npx` not found");
		return;
	}

	let npx_path = which("npx")
		.or_else(|_| which("npx.cmd"))
		.expect("`npx` should resolve after availability check");

	let gw_port = free_port();
	let authz_port = free_port();
	let authz_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), authz_port);
	let authz = spawn_deny_extauthz_on(authz_addr).await;
	wait_for_tcp(&format!("127.0.0.1:{authz_port}"), Duration::from_secs(10))
		.await
		.expect("extAuthz deny server ready");
	let (mut mcp_child, mcp_name, mcp_port) = spawn_mcp_everything(npx_path.as_path()).await;

	let policies = format!("extAuthz:\n  host: 127.0.0.1:{authz_port}",);

	let mut gw = spawn_agentgateway(gw_port, &mcp_name, mcp_port, &policies).await;
	let sse = format!("http://127.0.0.1:{gw_port}/sse");
	wait_http_ready(&sse, &[StatusCode::FORBIDDEN, StatusCode::OK]).await;

	let resp = reqwest::Client::new()
		.get(&sse)
		.header("accept", "text/event-stream")
		.send()
		.await
		.unwrap();
	assert_eq!(
		resp.status(),
		StatusCode::FORBIDDEN,
		"extAuthZ deny should yield 403"
	);
	gw.kill().await;
	mcp_child.kill().await;
	authz.abort();
	let _ = authz.await;
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_extauth_allows_authorized() {
	if which("npx").is_err() && which("npx.cmd").is_err() {
		eprintln!("skipping MCP integration test: `npx` not found");
		return;
	}

	let npx_path = which("npx")
		.or_else(|_| which("npx.cmd"))
		.expect("`npx` should resolve after availability check");

	let gw_port = free_port();
	let authz_port = free_port();
	let authz_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), authz_port);
	let authz = spawn_allow_extauthz_on(authz_addr).await;
	wait_for_tcp(&format!("127.0.0.1:{authz_port}"), Duration::from_secs(10))
		.await
		.expect("extAuthz allow server ready");
	let (mut mcp_child, mcp_name, mcp_port) = spawn_mcp_everything(npx_path.as_path()).await;

	let policies = format!("extAuthz:\n  host: 127.0.0.1:{authz_port}",);
	let mut gw = spawn_agentgateway(gw_port, &mcp_name, mcp_port, &policies).await;

	let sse = format!("http://127.0.0.1:{gw_port}/sse");
	wait_http_ready(&sse, &[StatusCode::OK]).await;
	let ok = read_some_sse_bytes(&sse, None).await;
	assert!(ok, "expected to read some SSE bytes when authorized");
	gw.kill().await;
	mcp_child.kill().await;
	authz.abort();
	let _ = authz.await;
}

#[tokio::test(flavor = "multi_thread")]
async fn local_ratelimit_blocks_after_threshold() {
	if which("npx").is_err() && which("npx.cmd").is_err() {
		eprintln!("skipping MCP integration test: `npx` not found");
		return;
	}

	let npx_path = which("npx")
		.or_else(|_| which("npx.cmd"))
		.expect("`npx` should resolve after availability check");

	let gw_port = free_port();
	let (mut mcp_child, mcp_name, mcp_port) = spawn_mcp_everything(npx_path.as_path()).await;

	let policies =
		"localRateLimit:\n- maxTokens: 2\n  tokensPerFill: 0\n  fillInterval: 1h\n  type: requests";
	let mut gw = spawn_agentgateway(gw_port, &mcp_name, mcp_port, policies).await;

	let sse = format!("http://127.0.0.1:{gw_port}/sse");
	wait_http_ready(&sse, &[StatusCode::OK]).await;

	let client = reqwest::Client::new();
	assert_eq!(
		client
			.get(&sse)
			.header("accept", "text/event-stream")
			.send()
			.await
			.unwrap()
			.status(),
		StatusCode::OK
	);
	assert_eq!(
		client
			.get(&sse)
			.header("accept", "text/event-stream")
			.send()
			.await
			.unwrap()
			.status(),
		StatusCode::OK
	);
	assert_eq!(
		client
			.get(&sse)
			.header("accept", "text/event-stream")
			.send()
			.await
			.unwrap()
			.status(),
		StatusCode::TOO_MANY_REQUESTS
	);

	gw.kill().await;
	mcp_child.kill().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_ratelimit_blocks_after_threshold() {
	if which("npx").is_err() && which("npx.cmd").is_err() {
		eprintln!("skipping MCP integration test: `npx` not found");
		return;
	}

	let npx_path = which("npx")
		.or_else(|_| which("npx.cmd"))
		.expect("`npx` should resolve after availability check");

	let gw_port = free_port();
	let rls_port = free_port();
	let rls_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), rls_port);
	let rls = spawn_rls_on(rls_addr, 2).await;
	wait_for_tcp(&format!("127.0.0.1:{rls_port}"), Duration::from_secs(10))
		.await
		.expect("remote rate limit server ready");
	let (mut mcp_child, mcp_name, mcp_port) = spawn_mcp_everything(npx_path.as_path()).await;

	let policies = format!(concat!(
		"remoteRateLimit:\n",
		"  host: 127.0.0.1:{rls_port}\n",
		"  domain: test.mcp\n",
		"  descriptors:\n",
		"  - entries:\n",
		"    - key: constant\n",
		"      value: '\"mcp-sse\"'\n",
		"    - key: id\n",
		"      value: 'request.headers[\"x-id\"]'\n",
		"  type: requests"
	));
	let mut gw = spawn_agentgateway(gw_port, &mcp_name, mcp_port, &policies).await;

	let sse = format!("http://127.0.0.1:{gw_port}/sse");
	wait_http_ready(&sse, &[StatusCode::OK]).await;

	let client = reqwest::Client::new();
	let hdr = ("x-id", "abc");
	assert_eq!(
		client
			.get(&sse)
			.header(hdr.0, hdr.1)
			.header("accept", "text/event-stream")
			.send()
			.await
			.unwrap()
			.status(),
		StatusCode::OK
	);
	assert_eq!(
		client
			.get(&sse)
			.header(hdr.0, hdr.1)
			.header("accept", "text/event-stream")
			.send()
			.await
			.unwrap()
			.status(),
		StatusCode::OK
	);
	assert_eq!(
		client
			.get(&sse)
			.header(hdr.0, hdr.1)
			.header("accept", "text/event-stream")
			.send()
			.await
			.unwrap()
			.status(),
		StatusCode::TOO_MANY_REQUESTS
	);

	gw.kill().await;
	mcp_child.kill().await;
	rls.abort();
	let _ = rls.await;
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_ratelimit_isolated_by_descriptor_key() {
	if which("npx").is_err() && which("npx.cmd").is_err() {
		eprintln!("skipping MCP integration test: `npx` not found");
		return;
	}

	let npx_path = which("npx")
		.or_else(|_| which("npx.cmd"))
		.expect("`npx` should resolve after availability check");

	let gw_port = free_port();
	let rls_port = free_port();
	let rls_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), rls_port);
	let rls = spawn_rls_on(rls_addr, 2).await;
	wait_for_tcp(&format!("127.0.0.1:{rls_port}"), Duration::from_secs(10))
		.await
		.expect("remote rate limit server ready");
	let (mut mcp_child, mcp_name, mcp_port) = spawn_mcp_everything(npx_path.as_path()).await;

	let policies = format!(concat!(
		"remoteRateLimit:\n",
		"  host: 127.0.0.1:{rls_port}\n",
		"  domain: test.mcp\n",
		"  descriptors:\n",
		"  - entries:\n",
		"    - key: constant\n",
		"      value: '\"mcp-sse\"'\n",
		"    - key: id\n",
		"      value: 'request.headers[\"x-id\"]'\n",
		"  type: requests"
	));
	let mut gw = spawn_agentgateway(gw_port, &mcp_name, mcp_port, &policies).await;

	let client = reqwest::Client::new();
	let sse = format!("http://127.0.0.1:{gw_port}/sse");
	wait_http_ready(&sse, &[StatusCode::OK]).await;

	// Key A consumes quota
	assert_eq!(
		client
			.get(&sse)
			.header("x-id", "A")
			.header("accept", "text/event-stream")
			.send()
			.await
			.unwrap()
			.status(),
		StatusCode::OK
	);
	assert_eq!(
		client
			.get(&sse)
			.header("x-id", "A")
			.header("accept", "text/event-stream")
			.send()
			.await
			.unwrap()
			.status(),
		StatusCode::OK
	);

	// Different key B unaffected
	assert_eq!(
		client
			.get(&sse)
			.header("x-id", "B")
			.header("accept", "text/event-stream")
			.send()
			.await
			.unwrap()
			.status(),
		StatusCode::OK
	);

	// Key A now over limit
	assert_eq!(
		client
			.get(&sse)
			.header("x-id", "A")
			.header("accept", "text/event-stream")
			.send()
			.await
			.unwrap()
			.status(),
		StatusCode::TOO_MANY_REQUESTS
	);

	gw.kill().await;
	mcp_child.kill().await;
	rls.abort();
	let _ = rls.await;
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_ratelimit_concurrent_requests_enforce_limit() {
	if which("npx").is_err() && which("npx.cmd").is_err() {
		eprintln!("skipping MCP integration test: `npx` not found");
		return;
	}

	let npx_path = which("npx")
		.or_else(|_| which("npx.cmd"))
		.expect("`npx` should resolve after availability check");

	let gw_port = free_port();
	let rls_port = free_port();
	let rls_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), rls_port);
	let rls = spawn_rls_on(rls_addr, 2).await;
	wait_for_tcp(&format!("127.0.0.1:{rls_port}"), Duration::from_secs(10))
		.await
		.expect("remote rate limit server ready");
	let (mut mcp_child, mcp_name, mcp_port) = spawn_mcp_everything(npx_path.as_path()).await;

	let policies = format!(concat!(
		"remoteRateLimit:\n",
		"  host: 127.0.0.1:{rls_port}\n",
		"  domain: test.mcp\n",
		"  descriptors:\n",
		"  - entries:\n",
		"    - key: constant\n",
		"      value: '\"mcp-sse\"'\n",
		"    - key: id\n",
		"      value: 'request.headers[\"x-id\"]'\n",
		"  type: requests"
	));
	let mut gw = spawn_agentgateway(gw_port, &mcp_name, mcp_port, &policies).await;

	let sse = format!("http://127.0.0.1:{gw_port}/sse");
	wait_http_ready(&sse, &[StatusCode::OK]).await;

	let client = reqwest::Client::new();
	let mut futs = Vec::new();
	for _ in 0..5 {
		futs.push(
			client
				.get(&sse)
				.header("x-id", "C")
				.header("accept", "text/event-stream")
				.send(),
		);
	}
	let results = join_all(futs).await;
	let mut ok = 0usize;
	let mut over = 0usize;
	for r in results {
		let s = r.unwrap().status();
		if s == StatusCode::OK {
			ok += 1;
		}
		if s == StatusCode::TOO_MANY_REQUESTS {
			over += 1;
		}
	}
	assert_eq!(ok, 2, "only first two should be allowed");
	assert_eq!(over, 3, "remaining should be 429");

	gw.kill().await;
	mcp_child.kill().await;
	rls.abort();
	let _ = rls.await;
}

// --------- Opt-in outage semantics (skipped unless env vars are set) ---------
//
// Set AGW_EXPECT_EXTAUTH_OUTAGE_MODE=allow|deny to assert behavior when
// extAuthZ host is unreachable. Similarly, set AGW_EXPECT_RLS_OUTAGE_MODE=allow|deny
// for remote rate-limit outage behavior. If unset, tests skip.

#[tokio::test(flavor = "multi_thread")]
async fn extauth_outage_semantics_opt_in() {
	if which("npx").is_err() && which("npx.cmd").is_err() {
		eprintln!("skipping MCP integration test: `npx` not found");
		return;
	}
	let mode = std::env::var("AGW_EXPECT_EXTAUTH_OUTAGE_MODE").ok();
	if mode.is_none() {
		eprintln!("SKIP: set AGW_EXPECT_EXTAUTH_OUTAGE_MODE=allow|deny to run");
		return;
	}

	let npx_path = which("npx")
		.or_else(|_| which("npx.cmd"))
		.expect("`npx` should resolve after availability check");

	let gw_port = free_port();
	let dead_port = free_port(); // no server on this port
	let (mut mcp_child, mcp_name, mcp_port) = spawn_mcp_everything(npx_path.as_path()).await;

	let policies = format!("extAuthz:\n  host: 127.0.0.1:{dead_port}");
	let mut gw = spawn_agentgateway(gw_port, &mcp_name, mcp_port, &policies).await;
	let sse = format!("http://127.0.0.1:{gw_port}/sse");
	wait_http_ready(
		&sse,
		&[
			StatusCode::OK,
			StatusCode::SERVICE_UNAVAILABLE,
			StatusCode::FORBIDDEN,
		],
	)
	.await;

	let resp = reqwest::Client::new()
		.get(&sse)
		.header("accept", "text/event-stream")
		.send()
		.await
		.unwrap();
	match mode.as_deref() {
		Some("allow") => assert_eq!(resp.status(), StatusCode::OK, "expected fail-open"),
		Some("deny") => assert!(
			resp.status().is_client_error() || resp.status().is_server_error(),
			"expected non-200"
		),
		_ => eprintln!("Unknown mode; skipping assertion"),
	}
	gw.kill().await;
	mcp_child.kill().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn rls_outage_semantics_opt_in() {
	if which("npx").is_err() && which("npx.cmd").is_err() {
		eprintln!("skipping MCP integration test: `npx` not found");
		return;
	}
	let mode = std::env::var("AGW_EXPECT_RLS_OUTAGE_MODE").ok();
	if mode.is_none() {
		eprintln!("SKIP: set AGW_EXPECT_RLS_OUTAGE_MODE=allow|deny to run");
		return;
	}

	let npx_path = which("npx")
		.or_else(|_| which("npx.cmd"))
		.expect("`npx` should resolve after availability check");

	let gw_port = free_port();
	let dead_port = free_port();
	let (mut mcp_child, mcp_name, mcp_port) = spawn_mcp_everything(npx_path.as_path()).await;

	let policies = format!(concat!(
		"remoteRateLimit:\n",
		"  host: 127.0.0.1:{dead_port}\n",
		"  domain: test.mcp\n",
		"  descriptors:\n",
		"  - entries:\n",
		"    - key: constant\n",
		"      value: '\"mcp-sse\"'\n",
		"    - key: id\n",
		"      value: 'request.headers[\"x-id\"]'\n",
		"  type: requests"
	));
	let mut gw = spawn_agentgateway(gw_port, &mcp_name, mcp_port, &policies).await;
	let sse = format!("http://127.0.0.1:{gw_port}/sse");
	wait_http_ready(
		&sse,
		&[
			StatusCode::OK,
			StatusCode::SERVICE_UNAVAILABLE,
			StatusCode::TOO_MANY_REQUESTS,
		],
	)
	.await;

	let resp = reqwest::Client::new()
		.get(&sse)
		.header("accept", "text/event-stream")
		.send()
		.await
		.unwrap();
	match mode.as_deref() {
		Some("allow") => assert_eq!(resp.status(), StatusCode::OK, "expected fail-open"),
		Some("deny") => assert!(
			resp.status().is_client_error() || resp.status().is_server_error(),
			"expected non-200"
		),
		_ => eprintln!("Unknown mode; skipping assertion"),
	}
	gw.kill().await;
	mcp_child.kill().await;
}
