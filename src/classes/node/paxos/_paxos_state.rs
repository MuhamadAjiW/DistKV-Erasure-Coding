use std::fmt;

// ---PaxosState---
#[derive(PartialEq, Clone, Copy, Debug)]
pub enum PaxosState {
    Follower,
    Candidate,
    Leader,
}

impl fmt::Display for PaxosState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PaxosState::Follower => write!(f, "Follower"),
            PaxosState::Candidate => write!(f, "Candidate"),
            PaxosState::Leader => write!(f, "Leader"),
        }
    }
}
