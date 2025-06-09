use omnipaxos::{messages::Message, ClusterConfigEC, OmniPaxosEC};
use omnipaxos_storage::persistent_storage::PersistentStorage;

use crate::standard::classes::_entry::ECKeyValue;

pub type OmniPaxosECKV = OmniPaxosEC<ECKeyValue, PersistentStorage<ECKeyValue, ClusterConfigEC>>;
pub type OmniPaxosECMessage = Message<ECKeyValue, ClusterConfigEC>;
