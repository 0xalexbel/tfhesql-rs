use arrow_array::RecordBatch;
use super::simple_batch::RecordBatchBuilder;

#[derive(Clone)]
pub struct SqlInOut<T>(pub String, pub Vec<T>, pub Vec<T>);

pub trait InOutBatchResult {
    fn input_batch(&self) -> RecordBatch;
    fn output_batch(&self) -> RecordBatch;
}

impl<T> SqlInOut<T> {
    pub fn sql(&self) -> &String {
        &self.0
    }
    pub fn input(&self) -> &Vec<T> {
        &self.1
    }
    pub fn output(&self) -> &Vec<T> {
        &self.2
    }
}

impl SqlInOut<String> {
    fn from(sql: String, input: Vec<&str>, output: Vec<&str>) -> Self {
        SqlInOut(
            sql,
            input.iter().map(|s| s.to_string()).collect(),
            output.iter().map(|s| s.to_string()).collect(),
        )
    }
}

impl InOutBatchResult for SqlInOut<String> {
    fn input_batch(&self) -> RecordBatch {
        RecordBatchBuilder::one_string(self.input().clone())
    }

    fn output_batch(&self) -> RecordBatch {
        RecordBatchBuilder::one_string(self.output().clone())
    }
}

impl SqlInOut<bool> {
    fn from(sql: String, input: Vec<bool>, output: Vec<bool>) -> Self {
        SqlInOut(sql, input, output)
    }
}

impl InOutBatchResult for SqlInOut<bool> {
    fn input_batch(&self) -> RecordBatch {
        RecordBatchBuilder::one_bool(self.input().clone())
    }

    fn output_batch(&self) -> RecordBatch {
        RecordBatchBuilder::one_bool(self.output().clone())
    }
}

pub fn simple_str_queries(
    projection: &str,
    table_name: &str,
    column_name: &str,
) -> Vec<SqlInOut<String>> {
    [
        SqlInOut::<String>::from(
            format!(
                "SELECT DISTINCT {} FROM {} WHERE {} = 'a'",
                projection, table_name, column_name
            ),
            vec!["a"],
            vec!["a"],
        ),
        SqlInOut::<String>::from(
            format!(
                "SELECT DISTINCT {} FROM {} WHERE {} = 'ab'",
                projection, table_name, column_name
            ),
            vec!["ab"],
            vec!["ab"],
        ),
        SqlInOut::<String>::from(
            format!(
                "SELECT DISTINCT {} FROM {} WHERE {} = 'abc'",
                projection, table_name, column_name
            ),
            vec!["abc"],
            vec!["abc"],
        ),
        SqlInOut::<String>::from(
            format!(
                "SELECT DISTINCT {} FROM {} WHERE {} = 'abcd'",
                projection, table_name, column_name
            ),
            vec!["abcd"],
            vec!["abcd"],
        ),
        SqlInOut::<String>::from(
            format!(
                "SELECT DISTINCT {} FROM {} WHERE {} = 'abcde'",
                projection, table_name, column_name
            ),
            vec!["abcde"],
            vec!["abcde"],
        ),
        SqlInOut::<String>::from(
            format!(
                "SELECT DISTINCT {} FROM {} WHERE {} = 'abcdef'",
                projection, table_name, column_name
            ),
            vec!["abcdef"],
            vec!["abcdef"],
        ),
        SqlInOut::<String>::from(
            format!(
                "SELECT DISTINCT {} FROM {} WHERE {} = 'abcdefg'",
                projection, table_name, column_name
            ),
            vec!["abcdefg"],
            vec!["abcdefg"],
        ),
        SqlInOut::<String>::from(
            format!(
                "SELECT DISTINCT {} FROM {} WHERE {} = 'abcdefgh'",
                projection, table_name, column_name
            ),
            vec!["abcdefgh"],
            vec!["abcdefgh"],
        ),
        SqlInOut::<String>::from(
            format!(
                "SELECT DISTINCT {} FROM {} WHERE {} = 'abcdefghijklmnopqrstuvwxy'",
                projection, table_name, column_name
            ),
            vec!["abcdefghijklmnopqrstuvwxy"],
            vec!["abcdefghijklmnopqrstuvwxy"],
        ),
        SqlInOut::<String>::from(
            format!(
                "SELECT DISTINCT {} FROM {} WHERE {} = 'abcdefghijklmnopqrstuvwxyz'",
                projection, table_name, column_name
            ),
            vec!["abcdefghijklmnopqrstuvwxyz"],
            vec!["abcdefghijklmnopqrstuvwxyz"],
        ),
        SqlInOut::<String>::from(
            format!(
                "SELECT DISTINCT {} FROM {} WHERE {} = 'abcdefghijklmnopqrstuvwxyz01234'",
                projection, table_name, column_name
            ),
            vec!["abcdefghijklmnopqrstuvwxyz01234"],
            vec!["abcdefghijklmnopqrstuvwxyz01234"],
        ),
        SqlInOut::<String>::from(
            format!(
                "SELECT DISTINCT {} FROM {} WHERE {} = 'abcdefghijklmnopqrstuvwxyz012345'",
                projection, table_name, column_name
            ),
            vec!["abcdefghijklmnopqrstuvwxyz012345"],
            vec!["abcdefghijklmnopqrstuvwxyz012345"],
        ),
        SqlInOut::<String>::from(
            format!(
                "SELECT DISTINCT {} FROM {} WHERE {} = 'ab'",
                projection, table_name, column_name
            ),
            vec!["ab", "ab"],
            vec!["ab"],
        ),
        SqlInOut::<String>::from(
            format!(
                "SELECT {} FROM {} WHERE {} = 'ab'",
                projection, table_name, column_name
            ),
            vec!["ab", "ab"],
            vec!["ab", "ab"],
        ),
        SqlInOut::<String>::from(
            format!(
                "SELECT DISTINCT {} FROM {} WHERE {} = 'ab'",
                projection, table_name, column_name
            ),
            vec!["ab", "cd"],
            vec!["ab"],
        ),
        SqlInOut::<String>::from(
            format!(
                "SELECT DISTINCT {} FROM {} WHERE {} = 'ab'",
                projection, table_name, column_name
            ),
            vec!["cd", "ab"],
            vec!["ab"],
        ),
        SqlInOut::<String>::from(
            format!(
                "SELECT DISTINCT {} FROM {} WHERE {} = 'ab'",
                projection, table_name, column_name
            ),
            vec!["cd"],
            vec![],
        ),
        SqlInOut::<String>::from(
            format!(
                "SELECT DISTINCT {} FROM {} WHERE {} = 'ab' OR {} = 'jj'",
                projection, table_name, column_name, column_name
            ),
            vec!["ab", "cd"],
            vec!["ab"],
        ),
        SqlInOut::<String>::from(
            format!(
                "SELECT DISTINCT {} FROM {} WHERE {} = 'ab' OR {} = 'cd'",
                projection, table_name, column_name, column_name
            ),
            vec!["ab", "cd"],
            vec!["ab", "cd"],
        ),
        SqlInOut::<String>::from(
            format!(
                "SELECT DISTINCT {} FROM {} WHERE {} = 'ab' AND {} = 'cd'",
                projection, table_name, column_name, column_name
            ),
            vec!["ab", "cd"],
            vec![],
        ),
        SqlInOut::<String>::from(
            format!(
                "SELECT DISTINCT {} FROM {} WHERE {} <> 'ab'",
                projection, table_name, column_name
            ),
            vec!["ab", "cd"],
            vec!["cd"],
        ),
        SqlInOut::<String>::from(
            format!(
                "SELECT DISTINCT {} FROM {} WHERE {} <> 'ab'",
                projection, table_name, column_name
            ),
            vec!["ab", "cd", "cd"],
            vec!["cd"],
        ),
        SqlInOut::<String>::from(
            format!(
                "SELECT {} FROM {} WHERE {} <> 'ab'",
                projection, table_name, column_name
            ),
            vec!["ab", "cd", "cd"],
            vec!["cd", "cd"],
        ),
        SqlInOut::<String>::from(
            format!(
                "SELECT {} FROM {} WHERE {} <> 'ab' OR {} <> 'cd'",
                projection, table_name, column_name, column_name
            ),
            vec!["ab", "cd", "cd"],
            vec!["ab", "cd", "cd"],
        ),
    ]
    .to_vec()
}

pub fn simple_bool_queries(
    projection: &str,
    table_name: &str,
    column_name: &str,
) -> Vec<SqlInOut<bool>> {
    [SqlInOut::<bool>::from(
        format!(
            "SELECT DISTINCT {} FROM {} WHERE {} = true",
            projection, table_name, column_name
        ),
        vec![true],
        vec![true],
    )]
    .to_vec()
}
