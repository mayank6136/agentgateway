use std::collections::{HashMap, VecDeque};
use std::fmt::{self, Debug, Formatter};
use std::sync::{Arc, Mutex};

use agent_core::prelude::*;
use futures_util::TryFutureExt;
use rmcp::model::{
	ClientJsonRpcMessage, ClientNotification, ClientRequest, JsonRpcMessage, JsonRpcRequest,
	RequestId, ServerJsonRpcMessage,
};
use rmcp::transport::{TokioChildProcess, Transport};
use serde::Serialize;
use serde_json::to_string;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::ChildStderr;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, warn};

use crate::mcp::mergestream::Messages;
use crate::mcp::upstream::{IncomingRequestContext, UpstreamError};

pub(crate) const STDIO_LOG_CAPACITY: usize = 50;

type PendingRequestMap = Arc<Mutex<HashMap<RequestId, oneshot::Sender<ServerJsonRpcMessage>>>>;

#[derive(Debug)]
struct ProcessLogInner {
	stdout: VecDeque<String>,
	stderr: VecDeque<String>,
	capacity: usize,
}

impl ProcessLogInner {
	fn new(capacity: usize) -> Self {
		Self {
			stdout: VecDeque::with_capacity(capacity),
			stderr: VecDeque::with_capacity(capacity),
			capacity,
		}
	}

	fn push(queue: &mut VecDeque<String>, capacity: usize, line: String) {
		if queue.len() == capacity {
			queue.pop_front();
		}
		queue.push_back(line);
	}
}

#[derive(Debug)]
pub(crate) struct ProcessLogs {
	inner: Mutex<ProcessLogInner>,
}

impl ProcessLogs {
	pub(crate) fn new(capacity: usize) -> Self {
		Self {
			inner: Mutex::new(ProcessLogInner::new(capacity)),
		}
	}

	pub(crate) fn record_stdout_line(&self, line: impl Into<String>) {
		let raw = line.into();
		let sanitized = Self::sanitize_line(&raw);
		if sanitized.is_empty() {
			return;
		}
		let mut inner = self.inner.lock().unwrap();
		let capacity = inner.capacity;
		ProcessLogInner::push(&mut inner.stdout, capacity, sanitized);
	}

	pub(crate) fn record_stderr_line(&self, line: impl AsRef<str>) {
		let sanitized = Self::sanitize_line(line.as_ref());
		if sanitized.is_empty() {
			return;
		}
		let mut inner = self.inner.lock().unwrap();
		let capacity = inner.capacity;
		ProcessLogInner::push(&mut inner.stderr, capacity, sanitized);
	}

	pub(crate) fn record_serialized<T: Serialize>(&self, prefix: &str, value: &T) {
		match to_string(value) {
			Ok(serialized) => self.record_stdout_line(format!("{prefix} {serialized}")),
			Err(err) => warn!(?err, "failed to serialize stdio message for buffering"),
		}
	}

	pub(crate) fn spawn_stderr_reader(self: &Arc<Self>, stderr: ChildStderr) {
		let logs = Arc::clone(self);
		tokio::spawn(async move {
			let mut reader = BufReader::new(stderr);
			let mut line = String::new();
			loop {
				line.clear();
				match reader.read_line(&mut line).await {
					Ok(0) => break,
					Ok(_) => logs.record_stderr_line(&line),
					Err(err) => {
						warn!(?err, "failed reading stdio stderr for buffering");
						break;
					},
				}
			}
		});
	}

	pub(crate) fn formatted(&self) -> String {
		let inner = self.inner.lock().unwrap();
		if inner.stdout.is_empty() && inner.stderr.is_empty() {
			return "no stdout/stderr captured yet".to_string();
		}
		let mut sections = Vec::new();
		if !inner.stderr.is_empty() {
			sections.push(format!(
				"stderr:\n{}",
				inner
					.stderr
					.iter()
					.map(String::as_str)
					.collect::<Vec<_>>()
					.join("\n")
			));
		}
		if !inner.stdout.is_empty() {
			sections.push(format!(
				"stdout:\n{}",
				inner
					.stdout
					.iter()
					.map(String::as_str)
					.collect::<Vec<_>>()
					.join("\n")
			));
		}
		sections.join("\n")
	}

	fn sanitize_line(line: &str) -> String {
		line.trim_end_matches(['\r', '\n']).to_string()
	}
}

pub struct Process {
	sender: mpsc::Sender<(ClientJsonRpcMessage, IncomingRequestContext)>,
	shutdown_tx: agent_core::responsechannel::Sender<(), Option<UpstreamError>>,
	event_stream: AtomicOption<mpsc::Sender<ServerJsonRpcMessage>>,
	pending_requests: PendingRequestMap,
	logs: Arc<ProcessLogs>,
	command_label: Arc<str>,
}

impl Process {
	pub async fn stop(&self) -> Result<(), UpstreamError> {
		let res = self
			.shutdown_tx
			.send_and_wait(())
			.await
			.map_err(|_| UpstreamError::Send)?;
		if let Some(err) = res {
			Err(err)
		} else {
			Ok(())
		}
	}

	pub async fn send_message(
		&self,
		req: JsonRpcRequest<ClientRequest>,
		ctx: &IncomingRequestContext,
	) -> Result<ServerJsonRpcMessage, UpstreamError> {
		let req_id = req.id.clone();
		let is_initialize = matches!(&req.request, ClientRequest::InitializeRequest(_));
		self.logs.record_serialized(">>>", &req);

		let (sender, receiver) = oneshot::channel();

		self
			.pending_requests
			.lock()
			.unwrap()
			.insert(req_id.clone(), sender);

		if self
			.sender
			.send((JsonRpcMessage::Request(req), ctx.clone()))
			.await
			.is_err()
		{
			self.pending_requests.lock().unwrap().remove(&req_id);
			return Err(self.handle_initialize_error(UpstreamError::Send, is_initialize));
		}

		match receiver.await {
			Ok(response) => Ok(response),
			Err(_) => {
				self.pending_requests.lock().unwrap().remove(&req_id);
				Err(self.handle_initialize_error(UpstreamError::Recv, is_initialize))
			},
		}
	}

	pub async fn get_event_stream(&self) -> Messages {
		let (tx, rx) = tokio::sync::mpsc::channel(10);
		self.event_stream.store(Some(Arc::new(tx)));
		Messages::from(rx)
	}

	pub async fn send_notification(
		&self,
		req: ClientNotification,
		ctx: &IncomingRequestContext,
	) -> Result<(), UpstreamError> {
		self
			.logs
			.record_serialized(">>>", &ClientJsonRpcMessage::notification(req.clone()));
		self
			.sender
			.send((JsonRpcMessage::notification(req), ctx.clone()))
			.await
			.map_err(|_| UpstreamError::Send)?;
		Ok(())
	}

	fn handle_initialize_error(&self, err: UpstreamError, is_initialize: bool) -> UpstreamError {
		if !is_initialize {
			return err;
		}
		let err_msg = err.to_string();
		let snippet = self.logs.formatted();
		warn!(
						command = %self.command_label,
						error = %err_msg,
						output = %snippet,
						"MCP stdio initialize failed"
		);
		UpstreamError::InitializeFailed {
			details: format!(
				"failed to initialize stdio command '{}': {err_msg}. Recent MCP stdio output:\n{snippet}",
				self.command_label
			),
		}
	}
}

impl Process {
	pub fn new(proc: impl MCPTransport) -> Self {
		Self::new_with_logging(
			proc,
			Arc::new(ProcessLogs::new(STDIO_LOG_CAPACITY)),
			Arc::<str>::from("mcp transport"),
		)
	}

	pub fn new_with_logging(
		mut proc: impl MCPTransport,
		logs: Arc<ProcessLogs>,
		command_label: Arc<str>,
	) -> Self {
		let (sender_tx, mut sender_rx) =
			mpsc::channel::<(ClientJsonRpcMessage, IncomingRequestContext)>(10);
		let (shutdown_tx, mut shutdown_rx) =
			agent_core::responsechannel::new::<(), Option<UpstreamError>>(10);
		let pending_requests: PendingRequestMap = Arc::new(Mutex::new(HashMap::new()));
		let pending_requests_for_loop = Arc::clone(&pending_requests);
		let event_stream: AtomicOption<Sender<ServerJsonRpcMessage>> = Default::default();
		let event_stream_send: AtomicOption<Sender<ServerJsonRpcMessage>> = event_stream.clone();
		let logs_for_receive = Arc::clone(&logs);
		let command_label_for_logs = Arc::clone(&command_label);

		tokio::spawn(async move {
			loop {
				tokio::select! {
								Some((msg, ctx)) = sender_rx.recv() => {
												if let Err(err) = proc.send(msg, &ctx).await {
																logs_for_receive.record_stdout_line(format!("transport send error: {err}"));
																error!(command = %command_label_for_logs, ?err, "Error sending message to stdio process");
																clear_pending_requests(&pending_requests_for_loop);
																break;
												}
								},
								msg = proc.receive() => {
												match msg {
																Some(message) => {
																				logs_for_receive.record_serialized("<<<", &message);
																				match message {
																								JsonRpcMessage::Response(res) => {
																												let req_id = res.id.clone();
																												if let Some(sender) = pending_requests_for_loop.lock().unwrap().remove(&req_id) {
																																let _ = sender.send(ServerJsonRpcMessage::Response(res));
																												}
																								},
																								other => {
																												if let Some(sender) = event_stream_send.load().as_ref() {
																																let _ = sender.send(other).await;
																												}
																								},
																				}
																},
																None => {
																				logs_for_receive.record_stdout_line("transport receive returned none");
																				clear_pending_requests(&pending_requests_for_loop);
																				break;
																},
												}
								},
								Some((_, resp)) = shutdown_rx.recv() => {
												let err = proc.close().await;
												if let Err(e) = &err {
																logs_for_receive.record_stdout_line(format!("transport close error: {e}"));
																warn!(command = %command_label_for_logs, ?e, "Error shutting down stdio process");
												}
												clear_pending_requests(&pending_requests_for_loop);
												let _ = resp.send(err.err());
												return;
								},
								else => {
												let err = proc.close().await;
												if let Err(e) = err {
																logs_for_receive.record_stdout_line(format!("transport close error: {e}"));
																warn!(command = %command_label_for_logs, ?e, "Error shutting down stdio process");
												}
												clear_pending_requests(&pending_requests_for_loop);
												return;
								},
				}
			}

			clear_pending_requests(&pending_requests_for_loop);
		});

		Self {
			sender: sender_tx,
			shutdown_tx,
			event_stream,
			pending_requests,
			logs,
			command_label,
		}
	}
}

impl Debug for Process {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		f.debug_struct("Process").finish()
	}
}

pub trait MCPTransport: Send + 'static {
	/// Send a message to the transport
	///
	/// Notice that the future returned by this function should be `Send` and `'static`.
	/// It's because the sending message could be executed concurrently.
	fn send(
		&mut self,
		item: ClientJsonRpcMessage,
		user_headers: &IncomingRequestContext,
	) -> impl Future<Output = Result<(), UpstreamError>> + Send + 'static;

	/// Receive a message from the transport, this operation is sequential.
	fn receive(&mut self) -> impl Future<Output = Option<ServerJsonRpcMessage>> + Send;

	/// Close the transport
	fn close(&mut self) -> impl Future<Output = Result<(), UpstreamError>> + Send;
}

impl MCPTransport for TokioChildProcess {
	fn send(
		&mut self,
		item: ClientJsonRpcMessage,
		_: &IncomingRequestContext,
	) -> impl Future<Output = Result<(), UpstreamError>> + Send + 'static {
		Transport::send(self, item).map_err(Into::into)
	}

	fn receive(&mut self) -> impl Future<Output = Option<ServerJsonRpcMessage>> + Send {
		Transport::receive(self)
	}

	fn close(&mut self) -> impl Future<Output = Result<(), UpstreamError>> + Send {
		Transport::close(self).map_err(Into::into)
	}
}

fn clear_pending_requests(pending: &PendingRequestMap) {
	pending.lock().unwrap().clear();
}

#[cfg(test)]
mod tests {
	use super::*;
	use rmcp::model::{
		ClientJsonRpcMessage, InitializeRequest, InitializeRequestParam, JsonRpcMessage, RequestId,
	};

	#[cfg(unix)]
	#[tokio::test]
	async fn initialize_failure_includes_command_name() {
		let mut command = tokio::process::Command::new("bash");
		command.args(["-lc", "nonexistent-cmd"]);
		let (proc, stderr) = TokioChildProcess::builder(command)
			.stderr(std::process::Stdio::piped())
			.spawn()
			.expect("failed to spawn bash");
		let logs = Arc::new(ProcessLogs::new(5));
		if let Some(stderr) = stderr {
			logs.spawn_stderr_reader(stderr);
		}
		let label: Arc<str> = Arc::from("bash -lc nonexistent-cmd");
		let process = Process::new_with_logging(proc, logs, Arc::clone(&label));
		tokio::time::sleep(std::time::Duration::from_millis(50)).await;

		let init = InitializeRequest::new(InitializeRequestParam::default());
		let message = ClientJsonRpcMessage::request(init.into(), RequestId::Number(0));
		let JsonRpcMessage::Request(init_request) = message else {
			panic!("expected request");
		};
		let ctx = IncomingRequestContext::empty();
		let err = tokio::time::timeout(
			std::time::Duration::from_secs(5),
			process.send_message(init_request, &ctx),
		)
		.await
		.expect("initialize call timed out")
		.expect_err("expected initialization failure");
		let err_string = err.to_string();
		assert!(err_string.contains("bash"));
		assert!(err_string.contains("nonexistent-cmd"));
	}
}
