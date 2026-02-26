//! Tier 3 LLM integration for deep graph interpretation.
//!
//! Receives [`Tier2Result`]s from a bounded crossbeam channel,
//! serializes graph paths into natural language prompts, and calls
//! an [`LlmClient`] for interpretation. The bounded channel (capacity: 1000)
//! never blocks the engine -- excess results are dropped via `try_send`.
//!
//! # Architecture
//!
//! The [`Tier3Worker`] runs as a background Tokio task (or dedicated thread).
//! The engine sends [`Tier2Result`]s through a [`Tier3Sender`] using
//! non-blocking `try_send`. If the channel is full, results are silently
//! dropped rather than blocking the engine (TIER3-04).
//!
//! The [`LlmClient`] trait abstracts the LLM backend. Production
//! implementations can use `tokio::task::spawn_blocking` for async HTTP;
//! the [`MockLlmClient`] provides synchronous responses for testing.
//!
//! # Example
//!
//! ```
//! use krabnet::tier3::{Tier3Worker, MockLlmClient, Tier2Result};
//! use krabnet::types::{NodeId, Epoch};
//!
//! let mock = MockLlmClient::new(vec!["pattern detected".to_string()]);
//! let (worker, sender) = Tier3Worker::new(Box::new(mock));
//!
//! let result = Tier2Result {
//!     frame_id: 1,
//!     anchor: NodeId(10),
//!     paths: vec![vec![NodeId(10), NodeId(20)]],
//!     epoch: Epoch(5),
//!     tier2_summary: "hop complete".to_string(),
//! };
//!
//! assert!(sender.try_send(result));
//! drop(sender);
//! worker.run();
//! assert_eq!(worker.interpretations().len(), 1);
//! ```

use std::sync::{Arc, Mutex};

use crate::types::{Epoch, NodeId};

/// A Tier 2 interpretation result ready for LLM analysis.
///
/// Contains the frame context and materialized paths that the
/// [`Tier3Worker`] will serialize into a natural language prompt
/// for LLM interpretation.
#[derive(Debug, Clone)]
pub struct Tier2Result {
    /// The frame that triggered this interpretation.
    pub frame_id: u64,
    /// The anchor node of the frame.
    pub anchor: NodeId,
    /// The materialized paths at the time of interpretation.
    pub paths: Vec<Vec<NodeId>>,
    /// The epoch at which this result was generated.
    pub epoch: Epoch,
    /// The Tier 2 structural analysis summary (hop completions, breakages).
    pub tier2_summary: String,
}

/// Trait for LLM backends. Implement for production (e.g., Anthropic API)
/// or testing ([`MockLlmClient`]).
///
/// Uses a synchronous `interpret()` method since the [`Tier3Worker`] runs
/// in its own dedicated task. Production implementations can use
/// `tokio::task::spawn_blocking` for async HTTP internally.
pub trait LlmClient: Send + Sync {
    /// Interpret a graph-aware prompt and return the LLM's analysis.
    ///
    /// Returns `Ok(analysis)` on success or `Err(message)` on failure.
    fn interpret(&self, prompt: &str) -> Result<String, String>;
}

/// Mock LLM client for testing. Records prompts and returns configurable responses.
///
/// If configured responses are exhausted, returns `"mock-analysis"` as default.
pub struct MockLlmClient {
    responses: Mutex<Vec<String>>,
    received_prompts: Mutex<Vec<String>>,
}

impl MockLlmClient {
    /// Create a new mock client with a queue of responses.
    ///
    /// Responses are returned in order; when exhausted, `"mock-analysis"` is returned.
    pub fn new(responses: Vec<String>) -> Self {
        Self {
            responses: Mutex::new(responses),
            received_prompts: Mutex::new(Vec::new()),
        }
    }

    /// Retrieve all prompts that were sent to this mock client.
    pub fn received_prompts(&self) -> Vec<String> {
        self.received_prompts.lock().unwrap().clone()
    }
}

impl LlmClient for MockLlmClient {
    fn interpret(&self, prompt: &str) -> Result<String, String> {
        self.received_prompts.lock().unwrap().push(prompt.to_string());
        let mut responses = self.responses.lock().unwrap();
        if responses.is_empty() {
            Ok("mock-analysis".to_string())
        } else {
            Ok(responses.remove(0))
        }
    }
}

/// Anthropic API client implementing [`LlmClient`].
///
/// Sends prompts to the Anthropic Messages API and extracts the text
/// response from the first content block. Uses `ureq` for synchronous
/// HTTP to avoid native-tls/windows-sys conflicts on GNU toolchains.
///
/// # Example
///
/// ```no_run
/// use krabnet::tier3::{AnthropicClient, LlmClient};
///
/// let client = AnthropicClient::new(
///     "sk-ant-...".to_string(),
///     "claude-sonnet-4-6".to_string(),
///     1024,
/// );
/// let result = client.interpret("Analyze this pattern...");
/// ```
pub struct AnthropicClient {
    agent: ureq::Agent,
    api_key: String,
    model: String,
    max_tokens: u32,
}

impl AnthropicClient {
    /// Create a new Anthropic API client.
    ///
    /// - `api_key`: Anthropic API key (e.g., `sk-ant-...`)
    /// - `model`: Model ID (e.g., `claude-sonnet-4-6`)
    /// - `max_tokens`: Maximum tokens in the response
    pub fn new(api_key: String, model: String, max_tokens: u32) -> Self {
        Self {
            agent: ureq::Agent::new(),
            api_key,
            model,
            max_tokens,
        }
    }
}

impl LlmClient for AnthropicClient {
    fn interpret(&self, prompt: &str) -> Result<String, String> {
        let body = serde_json::json!({
            "model": self.model,
            "max_tokens": self.max_tokens,
            "messages": [{
                "role": "user",
                "content": prompt
            }]
        });

        let response = self
            .agent
            .post("https://api.anthropic.com/v1/messages")
            .set("x-api-key", &self.api_key)
            .set("anthropic-version", "2023-06-01")
            .set("content-type", "application/json")
            .send_json(&body);

        match response {
            Ok(resp) => {
                let parsed: serde_json::Value = resp
                    .into_json()
                    .map_err(|e| format!("Failed to parse response JSON: {}", e))?;

                parsed["content"]
                    .as_array()
                    .and_then(|arr| arr.first())
                    .and_then(|block| block["text"].as_str())
                    .map(|s| s.to_string())
                    .ok_or_else(|| "No text content in Anthropic response".to_string())
            }
            Err(ureq::Error::Status(code, resp)) => {
                let body_text = resp.into_string().unwrap_or_default();
                Err(format!("Anthropic API error ({}): {}", code, body_text))
            }
            Err(e) => Err(format!("HTTP request failed: {}", e)),
        }
    }
}

/// Serialize frame paths into a natural language prompt for LLM interpretation.
///
/// Converts materialized paths into a structured prompt with causal chain
/// descriptions. The prompt includes:
/// - Frame and anchor node identifiers
/// - Tier 2 structural analysis summary
/// - Each materialized path as a causal chain with hop count
/// - A focused analysis request for structural patterns
///
/// # Example
///
/// ```
/// use krabnet::tier3::{serialize_prompt, Tier2Result};
/// use krabnet::types::{NodeId, Epoch};
///
/// let result = Tier2Result {
///     frame_id: 1,
///     anchor: NodeId(10),
///     paths: vec![vec![NodeId(10), NodeId(20), NodeId(30)]],
///     epoch: Epoch(5),
///     tier2_summary: "hop 1 complete".to_string(),
/// };
///
/// let prompt = serialize_prompt(&result);
/// assert!(prompt.contains("frame 1"));
/// assert!(prompt.contains("Node(10) -> Node(20) -> Node(30)"));
/// ```
pub fn serialize_prompt(result: &Tier2Result) -> String {
    let mut prompt = String::new();
    prompt.push_str(&format!(
        "Analyze the following graph pattern for frame {} (anchor node {}):\n\n",
        result.frame_id, result.anchor.0
    ));

    prompt.push_str(&format!("Tier 2 analysis: {}\n\n", result.tier2_summary));

    prompt.push_str("Materialized paths (causal chains):\n");
    for (i, path) in result.paths.iter().enumerate() {
        let chain: Vec<String> = path.iter().map(|n| format!("Node({})", n.0)).collect();
        let hops = if path.is_empty() { 0 } else { path.len() - 1 };
        prompt.push_str(&format!(
            "  Path {}: {} (length: {} hops)\n",
            i + 1,
            chain.join(" -> "),
            hops
        ));
    }

    prompt.push_str(&format!(
        "\nContext: {} total paths, epoch {}. ",
        result.paths.len(),
        result.epoch.0
    ));
    prompt.push_str(
        "What patterns, anomalies, or insights do you detect in these causal chains? ",
    );
    prompt.push_str(
        "Focus on structural significance: convergence points, fan-out patterns, and temporal ordering.",
    );

    prompt
}

/// An LLM interpretation result produced by the [`Tier3Worker`].
///
/// Stores the frame context alongside the LLM's analysis text.
#[derive(Debug, Clone)]
pub struct Tier3Interpretation {
    /// The frame that was analyzed.
    pub frame_id: u64,
    /// The epoch at which the original Tier 2 result was generated.
    pub epoch: Epoch,
    /// The LLM's analysis of the graph pattern.
    pub analysis: String,
}

/// Handle for sending [`Tier2Result`]s to the [`Tier3Worker`].
///
/// Used by the engine to submit results via [`try_send`](Tier3Sender::try_send)
/// (non-blocking). If the channel is full, the result is dropped rather than
/// blocking the engine.
pub struct Tier3Sender {
    sender: crossbeam::channel::Sender<Tier2Result>,
}

impl Tier3Sender {
    /// Send a Tier 2 result to the worker. Returns `true` if sent,
    /// `false` if the channel is full (result dropped, engine not blocked).
    pub fn try_send(&self, result: Tier2Result) -> bool {
        self.sender.try_send(result).is_ok()
    }
}

/// Background worker processing Tier 2 results through an LLM.
///
/// Receives results via a bounded crossbeam channel (capacity: 1000).
/// The engine sends via [`Tier3Sender::try_send`] -- if the channel is full,
/// results are dropped rather than blocking the engine (TIER3-04).
///
/// Call [`run`](Tier3Worker::run) in a dedicated thread or Tokio task.
/// The worker processes results until the channel is closed (all senders dropped).
pub struct Tier3Worker {
    receiver: crossbeam::channel::Receiver<Tier2Result>,
    client: Box<dyn LlmClient>,
    /// LLM interpretation results stored for retrieval.
    results: Arc<Mutex<Vec<Tier3Interpretation>>>,
}

impl Tier3Worker {
    /// Create a new Tier3Worker with its sender handle.
    ///
    /// Channel capacity is 1000 (TIER3-01). Returns the worker and a
    /// [`Tier3Sender`] for submitting results from the engine.
    pub fn new(client: Box<dyn LlmClient>) -> (Self, Tier3Sender) {
        let (sender, receiver) = crossbeam::channel::bounded(1000);
        let results = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                receiver,
                client,
                results,
            },
            Tier3Sender { sender },
        )
    }

    /// Run the worker loop. Call this in a dedicated thread or Tokio task.
    ///
    /// Processes [`Tier2Result`]s until the channel is closed (all senders
    /// dropped). Each result is serialized into a prompt via
    /// [`serialize_prompt`] and sent to the [`LlmClient`].
    pub fn run(&self) {
        while let Ok(tier2_result) = self.receiver.recv() {
            let prompt = serialize_prompt(&tier2_result);
            match self.client.interpret(&prompt) {
                Ok(analysis) => {
                    self.results.lock().unwrap().push(Tier3Interpretation {
                        frame_id: tier2_result.frame_id,
                        epoch: tier2_result.epoch,
                        analysis,
                    });
                }
                Err(e) => {
                    eprintln!(
                        "Tier 3 LLM error for frame {}: {}",
                        tier2_result.frame_id, e
                    );
                }
            }
        }
    }

    /// Retrieve all stored interpretations (for testing/inspection).
    pub fn interpretations(&self) -> Vec<Tier3Interpretation> {
        self.results.lock().unwrap().clone()
    }

    /// Get a shared handle to the results (for the server binary to expose).
    pub fn results_handle(&self) -> Arc<Mutex<Vec<Tier3Interpretation>>> {
        Arc::clone(&self.results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Epoch, NodeId};

    #[test]
    fn test_tier3_with_mock_llm() {
        // TEST-20: Tier 2 results through channel, mock LLM called, result stored
        let mock = MockLlmClient::new(vec!["detected convergence pattern".to_string()]);
        let (worker, sender) = Tier3Worker::new(Box::new(mock));

        // Send a Tier2Result
        let result = Tier2Result {
            frame_id: 42,
            anchor: NodeId(1),
            paths: vec![vec![NodeId(1), NodeId(2), NodeId(3)]],
            epoch: Epoch(100),
            tier2_summary: "hop 1 complete, hop 2 complete".to_string(),
        };

        assert!(sender.try_send(result));
        drop(sender); // Close the channel so worker loop exits

        // Run worker (will process one message then exit)
        worker.run();

        let interpretations = worker.interpretations();
        assert_eq!(interpretations.len(), 1);
        assert_eq!(interpretations[0].frame_id, 42);
        assert_eq!(interpretations[0].analysis, "detected convergence pattern");
    }

    #[test]
    fn test_tier3_channel_backpressure() {
        // TEST-21: Fill channel, engine never blocks, excess dropped
        let mock = MockLlmClient::new(vec![]);
        let (_worker, sender) = Tier3Worker::new(Box::new(mock));
        // Don't start the worker -- channel will fill up

        let base_result = Tier2Result {
            frame_id: 0,
            anchor: NodeId(1),
            paths: vec![vec![NodeId(1), NodeId(2)]],
            epoch: Epoch(1),
            tier2_summary: "test".to_string(),
        };

        // Fill channel to capacity (1000)
        let mut sent = 0;
        for i in 0..1100 {
            let mut r = base_result.clone();
            r.frame_id = i;
            if sender.try_send(r) {
                sent += 1;
            }
        }

        // Should have sent exactly 1000 (channel capacity)
        assert_eq!(sent, 1000);
        // The remaining 100 were dropped -- engine never blocked
    }

    #[test]
    fn test_anthropic_client_implements_llm_client() {
        // DEBT-01: AnthropicClient implements LlmClient trait (compile-time proof)
        // DEBT-07: AnthropicClient accessible via crate re-export (use crate::AnthropicClient)
        use crate::AnthropicClient;

        let client = AnthropicClient::new(
            "test-key".to_string(),
            "test-model".to_string(),
            100,
        );

        // Compile-time proof that AnthropicClient satisfies the LlmClient trait bound.
        // If this compiles, the trait is implemented.
        let _: Box<dyn LlmClient> = Box::new(client);
    }

    #[test]
    fn test_anthropic_client_env_var_pattern() {
        // DEBT-02: Structural verification that AnthropicClient accepts the same
        // parameter types that std::env::var returns (String), and is Send+Sync
        // (required for the binary's Box<dyn LlmClient> usage across threads).
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AnthropicClient>();

        // Verify construction with String params (matching env var output)
        let key: String = "sk-ant-test-key".to_string();
        let model: String = "claude-sonnet-4-6".to_string();
        let _client = AnthropicClient::new(key, model, 1024);
    }

    #[test]
    fn test_serialize_prompt_format() {
        let result = Tier2Result {
            frame_id: 7,
            anchor: NodeId(10),
            paths: vec![
                vec![NodeId(10), NodeId(20), NodeId(30)],
                vec![NodeId(10), NodeId(40)],
            ],
            epoch: Epoch(50),
            tier2_summary: "hop 1 complete".to_string(),
        };

        let prompt = serialize_prompt(&result);
        assert!(prompt.contains("frame 7"));
        assert!(prompt.contains("anchor node 10"));
        assert!(prompt.contains("Tier 2 analysis: hop 1 complete"));
        assert!(prompt.contains("Node(10) -> Node(20) -> Node(30)"));
        assert!(prompt.contains("length: 2 hops"));
        assert!(prompt.contains("Node(10) -> Node(40)"));
        assert!(prompt.contains("length: 1 hops"));
        assert!(prompt.contains("2 total paths"));
        assert!(prompt.contains("epoch 50"));
        assert!(prompt.contains("convergence points"));
    }
}
