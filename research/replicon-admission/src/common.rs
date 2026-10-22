use bevy::prelude::*;
use bevy_replicon::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Component, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct Dataset {
    pub(crate) revision: u64,
    pub(crate) bytes: Vec<u8>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Change {
    pub(crate) revision: u64,
    pub(crate) edits: Vec<(usize, u8)>,
}
impl Diffable for Dataset {
    type Diff = Change;
    const HISTORY_LEN: usize = 16;
    fn apply_diff(&mut self, change: &Change) -> Result<()> {
        if change.revision != self.revision + 1 {
            return Err("noncontiguous application diff".into());
        }
        for &(index, value) in &change.edits {
            self.bytes[index] = value;
        }
        self.revision = change.revision;
        Ok(())
    }
}
