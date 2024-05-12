#![cfg(feature = "stats")]

////////////////////////////////////////////////////////////////////////////////
// SqlStats
////////////////////////////////////////////////////////////////////////////////

use crate::stats::PerfStats;

#[derive(Debug, Clone, Default)]
pub struct SqlStats {
    stats: Vec<PerfStats>,
}

impl SqlStats {
    pub(crate) fn new_empty() -> Self {
        SqlStats { stats: vec![] }
    }

    pub(crate) fn close(&mut self, mut s: PerfStats) {
        s.close();
        self.stats.push(s);
    }

    pub fn print(&self) {
        PerfStats::print_vec(&self.stats);
    }

    pub fn total(&self) -> PerfStats {
        self.stats.last().unwrap().clone()
    }
}


