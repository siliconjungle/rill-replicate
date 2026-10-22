//! Experimental replication of a complete immutable dataset per scope.
//!
//! Sessions must be unique across connection, scope and sender incarnations.
//! Authenticate envelopes externally. ACK only after successful application.
//! Codecs must reconstruct exactly and must not mutate retained input states.
//! History/cache bounds are logical counts/encoded bytes, not a total heap bound.
#![forbid(unsafe_code)]

pub mod bytes;

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

/// Application-allocated unique connection + scope + sender incarnation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Session(pub u128);

/// Exact attempt identity. Transport framing must carry all three fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Receipt {
    pub session: Session,
    pub attempt: u64,
    pub revision: u64,
}

/// Codec errors must leave inputs unchanged. Outputs must decode exactly.
pub trait Codec<T> {
    type Error;
    fn encode_full(&self, state: &T) -> Result<Vec<u8>, Self::Error>;
    fn encode_delta(&self, base: &T, state: &T) -> Result<Vec<u8>, Self::Error>;
    fn decode_full(&self, bytes: &[u8]) -> Result<T, Self::Error>;
    fn decode_delta(&self, base: &T, bytes: &[u8]) -> Result<T, Self::Error>;
}

/// Fixed policy; all delta policies fall back when the baseline expires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Strategy {
    Full,
    SharedFull,
    PerRecipientDelta,
    SharedDelta,
    SharedAdaptive,
}

impl Strategy {
    fn shared(self) -> bool {
        matches!(
            self,
            Self::SharedFull | Self::SharedDelta | Self::SharedAdaptive
        )
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub history: usize,
    pub peers: usize,
    pub attempts_per_peer: usize,
    pub cache_entries: usize,
    pub cache_bytes: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            history: 16,
            peers: 20_000,
            attempts_per_peer: 8,
            cache_entries: 64,
            cache_bytes: 8 * 1024 * 1024,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Error<E> {
    InvalidConfig,
    UnknownSession,
    DuplicateSession,
    PeerLimit,
    Exhausted,
    WrongSession,
    MissingBaseline(u64),
    InvalidEnvelope,
    Codec(E),
}

#[derive(Debug, Clone)]
pub struct Envelope {
    pub receipt: Receipt,
    /// None means full replacement, Some means exact retained baseline revision.
    pub baseline: Option<u64>,
    pub body: Arc<[u8]>,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Stats {
    pub full_encodes: u64,
    pub delta_encodes: u64,
    pub cache_hits: u64,
    pub baseline_fallbacks: u64,
    pub encoded_bytes: u64,
    pub expired_attempts: u64,
}

struct Peer {
    acknowledged: Option<u64>,
    next_attempt: u64,
    pending: VecDeque<Receipt>,
}

#[derive(Clone)]
struct Encoded {
    baseline: Option<u64>,
    body: Arc<[u8]>,
}

/// Single-owner sender. Payload and encoded bodies can be shared across workers.
pub struct Sender<T, C> {
    codec: C,
    config: Config,
    strategy: Strategy,
    revision: u64,
    history: VecDeque<(u64, Arc<T>)>,
    peers: HashMap<Session, Peer>,
    // Key is requested baseline; value may be full when adaptive rejects delta.
    cache: HashMap<Option<u64>, Encoded>,
    cache_bytes: usize,
    stats: Stats,
}

impl<T, C: Codec<T>> Sender<T, C> {
    pub fn new(
        initial: T,
        codec: C,
        config: Config,
        strategy: Strategy,
    ) -> Result<Self, Error<C::Error>> {
        if config.history == 0 || config.peers == 0 || config.attempts_per_peer == 0 {
            return Err(Error::InvalidConfig);
        }
        Ok(Self {
            codec,
            config,
            strategy,
            revision: 0,
            history: VecDeque::from([(0, Arc::new(initial))]),
            peers: HashMap::new(),
            cache: HashMap::new(),
            cache_bytes: 0,
            stats: Stats::default(),
        })
    }

    pub fn add_peer(&mut self, session: Session) -> Result<(), Error<C::Error>> {
        if self.peers.contains_key(&session) {
            return Err(Error::DuplicateSession);
        }
        if self.peers.len() == self.config.peers {
            return Err(Error::PeerLimit);
        }
        self.peers.insert(
            session,
            Peer {
                acknowledged: None,
                next_attempt: 0,
                pending: VecDeque::new(),
            },
        );
        Ok(())
    }

    /// Caller must never reuse a removed Session, even across sender restarts.
    pub fn remove_peer(&mut self, session: Session) -> bool {
        self.peers.remove(&session).is_some()
    }

    pub fn publish(&mut self, state: T) -> Result<u64, Error<C::Error>> {
        let next = self.revision.checked_add(1).ok_or(Error::Exhausted)?;
        self.history.push_back((next, Arc::new(state)));
        self.revision = next;
        self.cache.clear();
        self.cache_bytes = 0;
        while self.history.len() > self.config.history {
            self.history.pop_front();
        }
        Ok(next)
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn current(&self) -> &T {
        &self.history.back().expect("nonempty history").1
    }
    pub fn stats(&self) -> Stats {
        self.stats
    }
    pub fn retained_revisions(&self) -> usize {
        self.history.len()
    }
    pub fn cached_bytes(&self) -> usize {
        self.cache_bytes
    }
    pub fn cached_entries(&self) -> usize {
        self.cache.len()
    }
    pub fn pending_attempts(&self) -> usize {
        self.peers.values().map(|p| p.pending.len()).sum()
    }
    pub fn acknowledged(&self, session: Session) -> Option<u64> {
        self.peers.get(&session).and_then(|p| p.acknowledged)
    }

    fn full(&mut self) -> Result<Encoded, Error<C::Error>> {
        let body = self
            .codec
            .encode_full(self.current())
            .map_err(Error::Codec)?;
        self.stats.full_encodes += 1;
        self.stats.encoded_bytes += body.len() as u64;
        Ok(Encoded {
            baseline: None,
            body: body.into(),
        })
    }

    fn cached_full(&mut self) -> Result<Encoded, Error<C::Error>> {
        if let Some(encoded) = self.cache.get(&None) {
            self.stats.cache_hits += 1;
            return Ok(encoded.clone());
        }
        let encoded = self.full()?;
        self.cache_insert(None, encoded.clone());
        Ok(encoded)
    }

    fn cache_insert(&mut self, key: Option<u64>, encoded: Encoded) {
        // Charge every entry's body even if adaptive entries share the same Arc.
        if self.cache.len() < self.config.cache_entries
            && encoded.body.len() <= self.config.cache_bytes.saturating_sub(self.cache_bytes)
        {
            self.cache_bytes += encoded.body.len();
            self.cache.insert(key, encoded);
        }
    }

    /// Builds a retransmittable envelope and records its exact ACK identity.
    /// Oldest attempts expire on overflow; late ACKs for them have no effect.
    /// Sending/transport failure never acknowledges anything automatically.
    pub fn prepare(&mut self, session: Session) -> Result<Envelope, Error<C::Error>> {
        let peer = self.peers.get(&session).ok_or(Error::UnknownSession)?;
        let attempt = peer.next_attempt.checked_add(1).ok_or(Error::Exhausted)?;
        let acknowledged = peer.acknowledged;
        let base = acknowledged.and_then(|r| self.history.iter().find(|(rev, _)| *rev == r));
        let expired_base = acknowledged.is_some() && base.is_none();
        let full_only = matches!(self.strategy, Strategy::Full | Strategy::SharedFull);
        let requested = if full_only {
            None
        } else {
            base.map(|(rev, _)| *rev)
        };
        let encoded = if self.strategy.shared() && self.cache.contains_key(&requested) {
            self.stats.cache_hits += 1;
            self.cache[&requested].clone()
        } else if let Some(rev) = requested {
            let base = &self
                .history
                .iter()
                .find(|(r, _)| *r == rev)
                .expect("retained")
                .1;
            let bytes = self
                .codec
                .encode_delta(base, self.current())
                .map_err(Error::Codec)?;
            self.stats.delta_encodes += 1;
            self.stats.encoded_bytes += bytes.len() as u64;
            let mut encoded = Encoded {
                baseline: Some(rev),
                body: bytes.into(),
            };
            if self.strategy == Strategy::SharedAdaptive {
                let full = self.cached_full()?;
                if full.body.len() <= encoded.body.len() {
                    encoded = full;
                }
            }
            if self.strategy.shared() {
                self.cache_insert(requested, encoded.clone());
            }
            encoded
        } else if self.strategy.shared() {
            self.cached_full()?
        } else {
            self.full()?
        };
        if expired_base && !full_only {
            self.stats.baseline_fallbacks += 1;
        }
        let receipt = Receipt {
            session,
            attempt,
            revision: self.revision,
        };
        let peer = self.peers.get_mut(&session).expect("validated session");
        peer.next_attempt = attempt;
        if peer.pending.len() == self.config.attempts_per_peer {
            peer.pending.pop_front();
            self.stats.expired_attempts += 1;
        }
        peer.pending.push_back(receipt);
        Ok(Envelope {
            receipt,
            baseline: encoded.baseline,
            body: encoded.body,
        })
    }

    /// Only a matching live attempt advances the confirmed baseline.
    pub fn acknowledge(&mut self, receipt: Receipt) -> bool {
        let Some(peer) = self.peers.get_mut(&receipt.session) else {
            return false;
        };
        let Some(index) = peer.pending.iter().position(|r| *r == receipt) else {
            return false;
        };
        peer.pending.remove(index);
        peer.acknowledged = Some(
            peer.acknowledged
                .map_or(receipt.revision, |r| r.max(receipt.revision)),
        );
        true
    }

    /// Application-driven response to a receiver MissingBaseline error. Already
    /// emitted envelopes may still arrive; the receiver's freshness check applies.
    pub fn resynchronize(&mut self, session: Session) -> Result<(), Error<C::Error>> {
        let peer = self.peers.get_mut(&session).ok_or(Error::UnknownSession)?;
        peer.acknowledged = None;
        peer.pending.clear();
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Applied {
    Updated,
    Stale,
}

/// Bounded exact receiver history. Decoding errors leave revision/state unchanged.
pub struct Receiver<T> {
    session: Session,
    capacity: usize,
    history: VecDeque<(u64, Arc<T>)>,
}

impl<T> Receiver<T> {
    pub fn new(session: Session, capacity: usize) -> Option<Self> {
        (capacity != 0).then(|| Self {
            session,
            capacity,
            history: VecDeque::new(),
        })
    }
    pub fn revision(&self) -> Option<u64> {
        self.history.back().map(|(r, _)| *r)
    }
    pub fn current(&self) -> Option<&T> {
        self.history.back().map(|(_, v)| v.as_ref())
    }
    pub fn retained_revisions(&self) -> usize {
        self.history.len()
    }

    pub fn apply<C: Codec<T>>(
        &mut self,
        envelope: &Envelope,
        codec: &C,
    ) -> Result<Applied, Error<C::Error>> {
        if envelope.receipt.session != self.session {
            return Err(Error::WrongSession);
        }
        let revision = envelope.receipt.revision;
        if envelope.baseline.is_some_and(|r| r > revision) {
            return Err(Error::InvalidEnvelope);
        }
        if self.revision().is_some_and(|r| revision <= r) {
            return Ok(Applied::Stale);
        }
        let state = match envelope.baseline {
            None => codec.decode_full(&envelope.body).map_err(Error::Codec)?,
            Some(base) => {
                let (_, state) = self
                    .history
                    .iter()
                    .find(|(r, _)| *r == base)
                    .ok_or(Error::MissingBaseline(base))?;
                codec
                    .decode_delta(state, &envelope.body)
                    .map_err(Error::Codec)?
            }
        };
        self.history.push_back((revision, Arc::new(state)));
        while self.history.len() > self.capacity {
            self.history.pop_front();
        }
        Ok(Applied::Updated)
    }
}
