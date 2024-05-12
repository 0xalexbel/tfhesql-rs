use std::{
    error::Error, fs::read_to_string, io::BufWriter, path::{Path, PathBuf}, str::FromStr, time::Instant
};

use clap::Parser;
use tfhe::{set_server_key, ClientKey, ConfigBuilder};
use tfhesql::test_util::broadcast_set_server_key;
use tfhesql::FheSqlError;
use tfhesql::*;

////////////////////////////////////////////////////////////////////////////////

#[derive(clap::ValueEnum, Clone, Default, Debug)]
pub enum RunMode {
    Clear,
    Trivial,
    #[default]
    Encrypted,
}

impl FromStr for RunMode {
    type Err = ();

    fn from_str(input: &str) -> Result<RunMode, Self::Err> {
        match input.to_ascii_lowercase().as_str() {
            "clear" => Ok(RunMode::Clear),
            "trivial" => Ok(RunMode::Trivial),
            "encrypted" => Ok(RunMode::Encrypted),
            _ => Err(()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    input_db: String,

    #[arg(short, long)]
    query_file: String,

    #[clap(short, long, default_value_t, value_enum)]
    mode: RunMode,
}

////////////////////////////////////////////////////////////////////////////////

pub fn absolute_path(path: impl AsRef<Path>) -> Result<PathBuf, Box<dyn Error>> {
    let path = path.as_ref();

    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        let cd = std::env::current_dir()?;
        Ok(cd.join(path))
    }
}

pub fn parse_args() -> (String, PathBuf, RunMode) {
    let args = Args::parse();

    let abs_db_dir = match absolute_path(&args.input_db) {
        Ok(a) => a,
        Err(_) => {
            eprintln!("Unable to read locate db directory '{}'", args.input_db);
            std::process::exit(1);
        }
    };

    if !abs_db_dir.is_dir() {
        eprintln!("Unable to read locate db directory '{}'", args.input_db);
        std::process::exit(1);
    }

    let query = match read_to_string(&args.query_file) {
        Ok(s) => s,
        Err(err) => {
            eprintln!("Unable to read query file '{}': {err}", args.query_file);
            std::process::exit(1);
        }
    };
    (query, abs_db_dir, args.mode)
}

////////////////////////////////////////////////////////////////////////////////

fn print_duration(start: Instant) {
    println!("Runtime: {:.2} s", start.elapsed().as_secs_f32());
}

fn print_csv(csv: &str) {
    println!("Clear DB query result: {}", csv);
}

fn print_enc_result(_enc_sql_result: &FheSqlResult) {
    println!("Encrypted DB query result:");

    let buf = BufWriter::new(std::io::stdout());
    _enc_sql_result.to_json(buf).unwrap_or_default();
}

fn print_clear_result(_clear_sql_result: &ClearSqlResult) {
    println!("Encrypted DB query result:");

    let buf = BufWriter::new(std::io::stdout());
    _clear_sql_result.to_json(buf).unwrap_or_default();
}

////////////////////////////////////////////////////////////////////////////////

fn run_clear(
    sql: &str,
    sql_client: &FheSqlClient,
    server_tables: &OrderedTables,
    start_time: Instant,
) -> Result<(), FheSqlError> {
    let clear_sql_query = sql_client.clear_sql(sql, SqlResultOptions::best())?;
    let clear_sql_result = FheSqlServer::run(&clear_sql_query, server_tables)?;
    let csv_result = clear_sql_result.clone().into_csv().unwrap_or_default();

    print_duration(start_time);
    print_csv(&csv_result);
    print_clear_result(&clear_sql_result);
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////

fn run_trivial(
    sql: &str,
    sql_client: &FheSqlClient,
    server_tables: &OrderedTables,
    start_time: Instant,
) -> Result<(), FheSqlError> {
    let triv_sql_query = sql_client.trivial_encrypt_sql(sql, SqlResultOptions::best())?;
    let triv_sql_result = FheSqlServer::run(&triv_sql_query, server_tables)?;
    let csv_result = triv_sql_result
        .try_decrypt_trivial_csv()
        .unwrap_or_default();

    print_duration(start_time);
    print_csv(&csv_result);
    print_enc_result(&triv_sql_result);
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////

fn run_enc(
    sql: &str,
    sql_client: &FheSqlClient,
    server_tables: &OrderedTables,
    start_time: Instant,
    client_key: &ClientKey,
) -> Result<(), FheSqlError> {
    let enc_sql_query = sql_client.encrypt_sql(sql, client_key, SqlResultOptions::best())?;
    let enc_sql_result = FheSqlServer::run(&enc_sql_query, server_tables)?;
    let csv_result = enc_sql_result.decrypt_csv(client_key).unwrap_or_default();

    print_duration(start_time);
    print_csv(&csv_result);
    print_enc_result(&enc_sql_result);
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////

fn run<P: AsRef<Path>>(sql: &str, csv_dir: P, run_mode: RunMode) -> Result<(), FheSqlError> {
    // Perf measure
    let start_time = Instant::now();

    // Load the SAME schemas in the SAME order as the server.
    let client_ordered_schemas = OrderedSchemas::load_from_directory(&csv_dir)?;

    // Creates a new FheSqlClient instance
    let sql_client = FheSqlClient::new(client_ordered_schemas.clone())?;

    // load tables
    let server_tables = OrderedTables::load_from_directory(&csv_dir)?;

    match run_mode {
        RunMode::Trivial | RunMode::Encrypted => {
            // Generate a new key
            let config = ConfigBuilder::default().build();
            let ck = ClientKey::generate(config);
            let sk = ck.generate_server_key();

            // Setup server keys
            broadcast_set_server_key(&sk);
            set_server_key(sk.clone());

            if matches!(run_mode, RunMode::Trivial) {
                run_trivial(sql, &sql_client, &server_tables, start_time)
            } else {
                run_enc(sql, &sql_client, &server_tables, start_time, &ck)
            }
        }
        RunMode::Clear => {
            run_clear(sql, &sql_client, &server_tables, start_time)
        }
    }
}

/// CLI Examples:
/// cargo run --release -- --input-db ../tfhesql/test/data/tiny --query-file ../tfhesql/test/queries/query-eq.txt --mode clear
/// cargo run --release -- --input-db ../tfhesql/test/data/numbers --query-file ../tfhesql/test/queries/query-eq-u8.txt --mode clear

fn main() {
    let (query, abs_db_dir, run_mode) = parse_args();
    let res = run(&query, abs_db_dir, run_mode);
    match res {
        Ok(_) => (),
        Err(e) => {
            eprintln!("{}", e);
            std::process::exit(1);
        }
    };
}
