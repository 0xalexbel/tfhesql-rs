use crate::{
    test_util::broadcast_set_server_key, FheRunSqlQuery,
    FheSqlClient, FheSqlQuery, FheSqlResult, FheSqlServer, OrderedSchemas, OrderedTables,
    SqlResultOptions,
};
use std::{cell::RefCell, io::BufWriter};
use tfhe::{set_server_key, shortint::PBSParameters, ClientKey};

thread_local! {
    static INTERNAL_BOUNTY_PARAMS: RefCell<Option<OrderedSchemas>> = const { RefCell::new(None) };
}

pub fn set_ordered_schemas(ordered_schemas: OrderedSchemas) {
    INTERNAL_BOUNTY_PARAMS.with(|os| os.replace_with(|_old| Some(ordered_schemas)));
}

pub struct Tables(OrderedTables);

#[derive(serde::Deserialize, serde::Serialize)]
pub struct EncrypedResult(FheSqlResult);

#[derive(serde::Deserialize, serde::Serialize)]
pub struct EncryptedQuery(FheSqlQuery);

impl EncrypedResult {
    /// Serializes the encrypted result as a JSON string. Returns an empty string if failed
    pub fn to_json(&self) -> String {
        let mut buf = BufWriter::new(Vec::new());
        self.0.to_json(&mut buf).unwrap_or_default();

        let bytes = buf.into_inner().unwrap_or_default();
        String::from_utf8(bytes).unwrap_or_default()
    }

    /// Write the encrypted result as a JSON string directly to the standard output
    pub fn print_json(&self) {
        let mut buf = BufWriter::new(std::io::stdout());        
        self.0.to_json(&mut buf).unwrap_or_default();
    }
}

/// Returns parameters and the device (CudaGpu/Cpu) on which the function /// should be ran
pub fn default_parameters() -> (PBSParameters, tfhe::Device) {
    let pbs_params: PBSParameters =
        tfhe::shortint::parameters::PARAM_MESSAGE_2_CARRY_2_KS_PBS.into();
    (pbs_params, tfhe::Device::Cpu)
}

/// Loads a directory with a structure described above in a format
/// that works for your implementation of the encryted query
pub fn load_tables<P: AsRef<std::path::Path>>(path: P) -> Tables {
    let ot = OrderedTables::load_from_directory(path).unwrap();
    set_ordered_schemas(ot.ordered_schemas().clone());
    Tables(ot)
}

/// Returns the encrypted query
pub fn encrypt_query(query: &str, client_key: &ClientKey) -> EncryptedQuery {
    INTERNAL_BOUNTY_PARAMS.with(|os| {
        let cell = os.borrow();
        if cell.as_ref().is_none() {
            panic!("Please call load_tables first!");
        }
        let ordered_schemas = cell.as_ref().unwrap();
        let sql_client = FheSqlClient::new(ordered_schemas.clone()).unwrap();
        let enc_sql_query = sql_client
            .encrypt_sql(query, client_key, SqlResultOptions::best())
            .unwrap();
        EncryptedQuery(enc_sql_query)
    })
}

/// # Inputs:
/// - sks: The server key to use
/// - input: your EncryptedQuery
/// - tables: the plain data you run the query on
///
/// # Output
/// - EncryptedResult
pub fn run_fhe_query(
    sks: &tfhe::ServerKey,
    input: &EncryptedQuery,
    data: &Tables,
) -> EncrypedResult {
    broadcast_set_server_key(sks);
    set_server_key(sks.clone());

    let sql_result = FheSqlServer::run(&input.0, &data.0).unwrap();
    EncrypedResult(sql_result)
}

/// The output of this function should be a string using the CSV format
/// You should provide a way to compare this string with the output of
/// the clear DB system you use for comparison
/// Returns empty string if failed
pub fn decrypt_result(client_key: &ClientKey, result: &EncrypedResult) -> String {
    result.0.decrypt_csv(client_key).unwrap_or_default()
}
