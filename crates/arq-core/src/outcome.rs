//! Outcome proposals returned by Python `dispatch`.

use crate::json::Json;

/// A dictionary Rust is willing to commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutcomeProposal {
    Ok,
    Skipped,
    Retry { delay_ms: u64 },
    Dead { reason: String, error: Option<String> },
}

/// The dispatch boundary did not produce a committable proposal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutcomeFault {
    /// Missing `outcome`, unknown string, wrong shape, or negative `delay_ms`.
    InvalidOutcome,
    /// `dispatch` raised. This is not an implicit retry.
    DispatchError,
}

impl OutcomeFault {
    pub const fn reason(self) -> &'static str {
        match self {
            Self::InvalidOutcome => "invalid_outcome",
            Self::DispatchError => "dispatch_error",
        }
    }

    pub const fn implies_retry(self) -> bool {
        false
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParsedOutcome {
    Proposal(OutcomeProposal),
    Invalid(OutcomeFault),
}

impl ParsedOutcome {
    pub fn implies_retry(&self) -> bool {
        match self {
            Self::Proposal(OutcomeProposal::Retry { .. }) => true,
            Self::Proposal(_) | Self::Invalid(_) => false,
        }
    }

    pub fn fault(&self) -> Option<OutcomeFault> {
        match self {
            Self::Invalid(fault) => Some(*fault),
            Self::Proposal(_) => None,
        }
    }
}

/// Parse a JSON value. This function does not turn a dispatch exception into an outcome.
pub fn parse_outcome(value: &Json) -> ParsedOutcome {
    let Json::Object(pairs) = value else {
        return invalid();
    };
    let Some(outcome) = pairs.iter().find(|(key, _)| key == "outcome").map(|(_, v)| v) else {
        return invalid();
    };
    let Json::String(kind) = outcome else {
        return invalid();
    };
    if let Some(delay) = lookup(pairs, "delay_ms") {
        if delay.as_u64().is_none() {
            return invalid();
        }
    }
    if let Some(reason) = lookup(pairs, "reason") {
        match reason {
            Json::String(text) if !text.is_empty() => {}
            _ => return invalid(),
        }
    }
    if let Some(error) = lookup(pairs, "error") {
        match error {
            Json::String(_) | Json::Null => {}
            _ => return invalid(),
        }
    }
    match kind.as_str() {
        "ok" => ParsedOutcome::Proposal(OutcomeProposal::Ok),
        "skipped" => ParsedOutcome::Proposal(OutcomeProposal::Skipped),
        "retry" => match lookup(pairs, "delay_ms").and_then(Json::as_u64) {
            Some(delay_ms) => ParsedOutcome::Proposal(OutcomeProposal::Retry { delay_ms }),
            None => invalid(),
        },
        "dead" => {
            let Some(Json::String(reason)) = lookup(pairs, "reason") else {
                return invalid();
            };
            let error = match lookup(pairs, "error") {
                Some(Json::String(text)) => Some(text.clone()),
                Some(Json::Null) => None,
                _ => return invalid(),
            };
            ParsedOutcome::Proposal(OutcomeProposal::Dead {
                reason: reason.clone(),
                error,
            })
        }
        _ => invalid(),
    }
}

/// An exception escaped `dispatch`, including outer middleware. Not a retry.
pub fn dispatch_exception() -> ParsedOutcome {
    ParsedOutcome::Invalid(OutcomeFault::DispatchError)
}

fn invalid() -> ParsedOutcome {
    ParsedOutcome::Invalid(OutcomeFault::InvalidOutcome)
}

fn lookup<'a>(pairs: &'a [(String, Json)], key: &str) -> Option<&'a Json> {
    pairs.iter().find(|(name, _)| name == key).map(|(_, value)| value)
}
