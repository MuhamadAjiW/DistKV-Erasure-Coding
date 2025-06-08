use omnipaxos::{messages::Message, ClusterConfigEC, OmniPaxosEC};
use omnipaxos_storage::persistent_storage::PersistentStorage;

use crate::base_libs::_ec::ECKeyValue;

pub type OmniPaxosECKV = OmniPaxosEC<ECKeyValue, PersistentStorage<ECKeyValue, ClusterConfigEC>>;
pub type OmniPaxosECMessage = Message<ECKeyValue, ClusterConfigEC>;
