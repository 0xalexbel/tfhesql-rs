#![cfg(feature = "stats")]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

static BOOL_AND_COUNT: AtomicUsize = AtomicUsize::new(0);
static BOOL_OR_COUNT: AtomicUsize = AtomicUsize::new(0);
static BOOL_NOT_COUNT: AtomicUsize = AtomicUsize::new(0);

static U8_AND_COUNT: AtomicUsize = AtomicUsize::new(0);
static U8_OR_COUNT: AtomicUsize = AtomicUsize::new(0);
static U8_NOT_COUNT: AtomicUsize = AtomicUsize::new(0);

static U8_IF_THEN_ELSE_COUNT: AtomicUsize = AtomicUsize::new(0);

pub fn inc_bool_and() {
    BOOL_AND_COUNT.fetch_add(1, Ordering::Relaxed);
}
pub fn inc_bool_or() {
    BOOL_OR_COUNT.fetch_add(1, Ordering::Relaxed);
}
pub fn inc_bool_not() {
    BOOL_NOT_COUNT.fetch_add(1, Ordering::Relaxed);
}

pub fn inc_u8_and() {
    U8_AND_COUNT.fetch_add(1, Ordering::Relaxed);
}
pub fn inc_u8_or() {
    U8_OR_COUNT.fetch_add(1, Ordering::Relaxed);
}
pub fn inc_u8_not() {
    U8_NOT_COUNT.fetch_add(1, Ordering::Relaxed);
}
pub fn inc_u8_if_then_else() {
    U8_IF_THEN_ELSE_COUNT.fetch_add(1, Ordering::Relaxed);
}

#[derive(Debug, Clone)]
pub struct PerfStats {
    closed: bool,
    name: String,
    time: Instant,
    duration: Duration,
    bool_and: usize,
    bool_or: usize,
    bool_not: usize,
    u8_and: usize,
    u8_or: usize,
    u8_not: usize,
    u8_if_then_else: usize,
}

impl Default for PerfStats {
    fn default() -> Self {
        Self {
            closed: Default::default(),
            name: Default::default(),
            time: Instant::now(),
            duration: Default::default(),
            bool_and: Default::default(),
            bool_or: Default::default(),
            bool_not: Default::default(),
            u8_and: Default::default(),
            u8_or: Default::default(),
            u8_not: Default::default(),
            u8_if_then_else: Default::default(),
        }
    }
}

impl PerfStats {
    pub fn close(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        let s = PerfStats::new("");
        self.duration = self.time.elapsed();
        self.bool_or = s.bool_or - self.bool_or;
        self.bool_and = s.bool_and - self.bool_and;
        self.bool_not = s.bool_not - self.bool_not;
        self.u8_and = s.u8_and - self.u8_and;
        self.u8_or = s.u8_or - self.u8_or;
        self.u8_not = s.u8_not - self.u8_not;
        self.u8_if_then_else = s.u8_if_then_else - self.u8_if_then_else;
    }

    pub fn new(name: &str) -> PerfStats {
        let bool_and = BOOL_AND_COUNT.load(std::sync::atomic::Ordering::Relaxed);
        let bool_or = BOOL_OR_COUNT.load(std::sync::atomic::Ordering::Relaxed);
        let bool_not = BOOL_NOT_COUNT.load(std::sync::atomic::Ordering::Relaxed);
        let u8_and = U8_AND_COUNT.load(std::sync::atomic::Ordering::Relaxed);
        let u8_or = U8_OR_COUNT.load(std::sync::atomic::Ordering::Relaxed);
        let u8_not = U8_NOT_COUNT.load(std::sync::atomic::Ordering::Relaxed);
        let u8_if_then_else = U8_IF_THEN_ELSE_COUNT.load(std::sync::atomic::Ordering::Relaxed);
        PerfStats {
            closed: false,
            name: name.to_string(),
            time: Instant::now(),
            duration: Duration::default(),
            bool_and,
            bool_or,
            bool_not,
            u8_and,
            u8_or,
            u8_not,
            u8_if_then_else,
        }
    }

    pub fn print(&self) {
        Self::print_sep();
        self.print_title("|  ");
        Self::print_sep();
        self.print_details("|  ");
        Self::print_sep();
    }

    fn print_title(&self, prefix: &str) {
        println!(
            "{}{}: ({:.2}s)",
            prefix,
            self.name,
            self.duration.as_secs_f32()
        );
    }
    fn print_sep() {
        println!("+---------------------------------------------+");
    }
    fn print_details(&self, prefix: &str) {
        println!(
            "{}OR : bool:{:?} / u8:{:?}",
            prefix, self.bool_or, self.u8_or
        );
        println!(
            "{}AND: bool:{:?} / u8:{:?}",
            prefix, self.bool_and, self.u8_and
        );
        println!(
            "{}NOT: bool:{:?} / u8:{:?}",
            prefix, self.bool_not, self.u8_not
        );
        println!("{}IF : {:?}", prefix, self.u8_if_then_else);
    }
    pub fn print_vec(vec: &[PerfStats]) {
        if vec.is_empty() {
            return;
        }
        Self::print_sep();
        vec.iter().enumerate().for_each(|(i, s)| {
            if i > 0 {
                println!("|");
            }
            let title_prefix = format!("|  {}.", i + 1);
            s.print_title(&title_prefix);
            println!("|");
            s.print_details("|    ");
        });
        Self::print_sep();
    }
}
