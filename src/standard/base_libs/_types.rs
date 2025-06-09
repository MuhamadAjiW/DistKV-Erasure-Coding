use omnipaxos::{messages::Message, ClusterConfig, OmniPaxos};
use omnipaxos_storage::persistent_storage::PersistentStorage;

use crate::standard::classes::_entry::KeyValue;

pub type OmniPaxosKV = OmniPaxos<KeyValue, PersistentStorage<KeyValue, ClusterConfig>>;
pub type OmniPaxosMessage = Message<KeyValue, ClusterConfig>;
