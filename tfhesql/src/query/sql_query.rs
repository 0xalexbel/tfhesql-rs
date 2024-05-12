use super::sql_query_binops::SqlQueryBinaryOp;
use super::sql_query_tree::ClearSqlQueryTree;
use super::sql_query_tree::SqlQueryTree;
use crate::default_into::*;
use crate::encrypt::*;
use crate::encrypt::traits::*;
use crate::types::BooleanType;
use crate::uint::mask::BoolMask;
use crate::FheSqlError;
use crate::OrderedSchemas;
use crate::SqlResultOptions;
use std::sync::Arc;
use tfhe::ClientKey;
use tfhe::CompactFheBool;
use tfhe::CompactPublicKey;
use tfhe::CompressedFheBool;
use tfhe::FheBool;

////////////////////////////////////////////////////////////////////////////////
// TableBoolMaskHeader
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug, serde::Deserialize, serde::Serialize)]
pub struct TableBoolMaskHeader<B> {
    pub table_mask: BoolMask<B>,
    pub field_mask: BoolMask<B>,
    pub not_field_mask: BoolMask<B>,
}

derive3_encrypt_decrypt! { TableBoolMaskHeader<B> {table_mask: BoolMask<B>, field_mask: BoolMask<B>, not_field_mask: BoolMask<B>} }

pub type ClearTableBoolMaskHeader = TableBoolMaskHeader<bool>;

impl<B> TableBoolMaskHeader<B> {
    pub fn new_empty() -> Self {
        TableBoolMaskHeader::<B> {
            table_mask: BoolMask::<B>::new_empty(),
            field_mask: BoolMask::<B>::new_empty(),
            not_field_mask: BoolMask::<B>::new_empty(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// SqlQuery
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug, serde::Deserialize, serde::Serialize)]
pub struct SqlQuery<B> {
    // The crypted part of the query.
    // ------------------------------

    enc: EncryptedSqlQuery<B>,

    // The clear part of the query.
    // ----------------------------

    // parameters provided by the client to tell the server how to export the result
    options: SqlResultOptions,

    // the ordered Schemas is a shared information between
    // the client and the server. It is included in the query, so that the server
    // can check if the tables it is manipulating are in sync with the client schemas
    ordered_schemas: OrderedSchemas,
}

pub type SqlQueryRef<B> = Arc<SqlQuery<B>>;
pub type ClearSqlQuery = SqlQuery<bool>;
pub type FheSqlQuery = SqlQuery<FheBool>;
pub type CompressedFheSqlQuery = SqlQuery<CompressedFheBool>;
pub type CompactFheSqlQuery = SqlQuery<CompactFheBool>;

#[derive(Clone, PartialEq, Eq, Debug, serde::Deserialize, serde::Serialize)]
struct EncryptedSqlQuery<B> {
    header: TableBoolMaskHeader<B>,
    is_distinct: B,
    where_tree: SqlQueryTree<B>,
}

derive3_encrypt_decrypt! { EncryptedSqlQuery<B> {header: TableBoolMaskHeader<B>, is_distinct: B, where_tree: SqlQueryTree<B>} }

type ClearEncryptedSqlQuery = EncryptedSqlQuery<bool>;

////////////////////////////////////////////////////////////////////////////////
// THFE public traits impl
////////////////////////////////////////////////////////////////////////////////

impl tfhe::prelude::FheDecrypt<ClearSqlQuery> for FheSqlQuery {
    /// Implements the [FheDecrypt] trait
    #[inline]
    fn decrypt(&self, key: &ClientKey) -> ClearSqlQuery {
        ClearSqlQuery {
            enc: self.enc.decrypt(key),
            options: self.options,
            ordered_schemas: self.ordered_schemas.clone(),
        }
    }
}

impl tfhe::prelude::FheTryEncrypt<ClearSqlQuery, ClientKey> for FheSqlQuery {
    type Error = FheSqlError;

    /// Implements the [FheTryEncrypt] trait
    #[inline]
    fn try_encrypt(value: ClearSqlQuery, key: &ClientKey) -> Result<Self, Self::Error> {
        Ok(FheSqlQuery::encrypt_ref(&value, key))
    }
}

impl tfhe::prelude::FheTryEncrypt<ClearSqlQuery, ClientKey> for CompressedFheSqlQuery {
    type Error = FheSqlError;

    /// Implements the [FheTryEncrypt] for Compressed Fhe types trait
    #[inline]
    fn try_encrypt(value: ClearSqlQuery, key: &ClientKey) -> Result<Self, Self::Error> {
        Ok(CompressedFheSqlQuery::encrypt_ref(&value, key))
    }
}

impl tfhe::prelude::FheTryEncrypt<ClearSqlQuery, CompactPublicKey> for CompactFheSqlQuery {
    type Error = FheSqlError;

    #[inline]
    /// Implements the [FheTryEncrypt] for [CompactPublicKey]
    fn try_encrypt(value: ClearSqlQuery, key: &CompactPublicKey) -> Result<Self, Self::Error> {
        Ok(CompactFheSqlQuery::encrypt_ref(&value, key))
    }
}

impl tfhe::prelude::FheTryTrivialEncrypt<ClearSqlQuery> for FheSqlQuery {
    type Error = FheSqlError;

    #[inline]
    /// Implements the [FheTryTrivialEncrypt] trait
    fn try_encrypt_trivial(value: ClearSqlQuery) -> Result<Self, Self::Error> {
        Ok(FheSqlQuery::encrypt_trivial_ref(&value))
    }
}

impl FheSqlQuery {
    #[inline]
    pub fn try_decrypt_trivial(
        &self,
    ) -> Result<ClearSqlQuery, tfhe::shortint::ciphertext::NotTrivialCiphertextError> {
        Ok(ClearSqlQuery {
            enc: self.enc.try_decrypt_trivial()?,
            options: self.options,
            ordered_schemas: self.ordered_schemas.clone(),
        })
    }
}

impl CompressedFheSqlQuery {
    /// Decompresses itself into a [FheSqlQuery]
    #[inline]
    pub fn decompress(&self) -> FheSqlQuery {
        SqlQuery {
            options: self.options,
            ordered_schemas: self.ordered_schemas.clone(),
            enc: self.enc.decompress(),
        }
    }
}

impl CompactFheSqlQuery {
    /// Expands itself into a [FheSqlQuery]
    #[inline]
    pub fn expand(&self) -> FheSqlQuery {
        SqlQuery {
            options: self.options,
            ordered_schemas: self.ordered_schemas.clone(),
            enc: self.enc.expand(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

impl<B> SqlQuery<B>
where
    B: BooleanType,
{
    pub(crate) fn new_empty(ordered_schemas: OrderedSchemas, options: SqlResultOptions) -> Self {
        SqlQuery {
            options,
            ordered_schemas,
            enc: EncryptedSqlQuery::<B> {
                header: TableBoolMaskHeader::<B>::new_empty(),
                is_distinct: B::get_false(),
                where_tree: SqlQueryTree::<B>::new_empty(),
            },
        }
    }
}

impl<B> SqlQuery<B> {
    #[inline]
    pub(crate) fn options(&self) -> &SqlResultOptions {
        &self.options
    }

    #[inline]
    pub(crate) fn ordered_schemas(&self) -> &OrderedSchemas {
        &self.ordered_schemas
    }

    #[inline]
    pub(crate) fn header(&self) -> &TableBoolMaskHeader<B> {
        &self.enc.header
    }

    #[inline]
    pub(crate) fn distinct(&self) -> &B {
        &self.enc.is_distinct
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.header().table_mask.len() == 0
    }

    #[inline]
    pub(crate) fn num_binary_ops(&self) -> usize {
        self.where_tree().compare_ops.len()
    }

    #[inline]
    pub(crate) fn binary_op_at(&self, index: usize) -> &SqlQueryBinaryOp<B> {
        self.where_tree().compare_ops.get(index)
    }

    #[inline]
    pub(crate) fn is_where_empty(&self) -> bool {
        self.where_tree().is_empty()
    }

    #[inline]
    pub(crate) fn where_tree(&self) -> &SqlQueryTree<B> {
        &self.enc.where_tree
    }
}

////////////////////////////////////////////////////////////////////////////////
// ClearSqlQuery
////////////////////////////////////////////////////////////////////////////////

impl ClearSqlQuery {
    pub(crate) fn new(
        header: ClearTableBoolMaskHeader,
        is_distinct: bool,
        where_tree: ClearSqlQueryTree,
        ordered_schemas: OrderedSchemas,
        options: SqlResultOptions,
    ) -> Self {
        ClearSqlQuery {
            enc: ClearEncryptedSqlQuery::new(header, is_distinct, where_tree),
            options,
            ordered_schemas,
        }
    }
}

impl ClearEncryptedSqlQuery {
    pub fn new(
        header: ClearTableBoolMaskHeader,
        is_distinct: bool,
        where_tree: ClearSqlQueryTree,
    ) -> Self {
        ClearEncryptedSqlQuery {
            header,
            is_distinct,
            where_tree,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// SqlQuery (Private impls)
////////////////////////////////////////////////////////////////////////////////

impl<E, C, Key> EncryptRef<SqlQuery<C>, Key> for SqlQuery<E>
where
    C: DefaultIntoWithKey<E, Key> + Sync,
    E: EncryptRef<C, Key> + Clone + Send,
    Key: Sync,
{
    #[inline]
    fn encrypt_ref(value: &SqlQuery<C>, key: &Key) -> Self {
        SqlQuery::<E> {
            enc: value.enc.encrypt_into(key),
            options: value.options,
            ordered_schemas: value.ordered_schemas.clone(),
        }
    }
}

impl<C, E> TrivialEncryptRef<SqlQuery<C>> for SqlQuery<E>
where
    C: DefaultInto<E> + Sync,
    E: TrivialEncryptRef<C> + Clone + Send,
{
    #[inline]
    fn encrypt_trivial_ref(value: &SqlQuery<C>) -> Self {
        SqlQuery::<E> {
            enc: value.enc.encrypt_trivial_into(),
            options: value.options,
            ordered_schemas: value.ordered_schemas.clone(),
        }
    }
}

impl<C, E> DefaultInto<SqlQuery<E>> for SqlQuery<C>
where
    C: DefaultInto<E>,
{
    #[inline]
    fn default_into() -> SqlQuery<E> {
        SqlQuery::<E> {
            enc: EncryptedSqlQuery::<C>::default_into(),
            options: SqlResultOptions::default(),
            ordered_schemas: OrderedSchemas::new_empty(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    use super::*;
    use crate::test;
    use crate::test_util::try_load_or_gen_test_keys;
    use crate::test_util::broadcast_set_server_key;
    use tfhe::prelude::FheDecrypt;
    use tfhe::set_server_key;

    #[test]
    fn test_encrypt() {
        let (ck, sk) = try_load_or_gen_test_keys(false);

        broadcast_set_server_key(&sk);
        set_server_key(sk);

        let options = SqlResultOptions::default();
        let sql_client = test::sql_client_i16x1("table1", "ProductID", vec![1]);
        let enc_q: FheSqlQuery = ClearSqlQuery::default_into();
        let clear_q = enc_q.decrypt(&ck);
        let expected_clear_q = ClearSqlQuery::default_into();
        assert_eq!(clear_q, expected_clear_q);

        let sql = "SELECT DISTINCT * FROM table1 WHERE ProductID = 1";
        let expected_clear_q: ClearSqlQuery = sql_client.clear_sql(sql, options).unwrap();

        let enc_q = sql_client.encrypt_sql(sql, &ck, options).unwrap();
        let clear_q = enc_q.decrypt(&ck);
        assert_eq!(clear_q, expected_clear_q);

        let triv_q = sql_client.trivial_encrypt_sql(sql, options).unwrap();
        let clear_q = triv_q.try_decrypt_trivial().unwrap();
        assert_eq!(clear_q, expected_clear_q);
    }
}
