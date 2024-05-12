////////////////////////////////////////////////////////////////////////////////
// SqlResultFormat
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum SqlResultFormat {
    /// Default: a list of Row bytes, one for each row of the final dataset batch result,
    /// the bool value indicates if padding is enabled or not (default=true). 
    RowBytes(bool),
    /// a single array of bytes equals to the serial concatenation of 
    /// all the rows of the final dataset batch result
    TableBytesInRowOrder,
    /// a single array of bytes equals to the serial concatenation of 
    /// all the columns of the final dataset batch result
    TableBytesInColumnOrder,
}

impl Default for SqlResultFormat {
    fn default() -> Self {
        SqlResultFormat::RowBytes(true)
    }
}

////////////////////////////////////////////////////////////////////////////////
// SqlResultOptions
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct SqlResultOptions {
    /// True if the SqlResult if dataset is compressed prior to encryption by the server
    compress: bool,
    /// dataset byte format
    format: SqlResultFormat,
}

impl Default for SqlResultOptions {
    fn default() -> Self {
        Self {
            compress: true,
            format: Default::default(),
        }
    }
}

impl SqlResultOptions {
    /// Use the fastest dataset encryption format
    pub fn best() -> Self {
        SqlResultOptions {
            compress:true,
            format: SqlResultFormat::TableBytesInColumnOrder,
        }
    }
    /// Enable/disable server-side dataset compression (default=`true`)
    pub fn with_compress(mut self, compression: bool) -> Self {
        self.compress = compression;
        self
    }

    /// Specify result format (default=`SqlResultFormat::ByRow(true)`),
    /// that is the way the server will arrange the resulting bytes 
    pub fn with_format(mut self, format: SqlResultFormat) -> Self {
        self.format = format;
        self
    }
}

////////////////////////////////////////////////////////////////////////////////

impl SqlResultOptions {
    pub(crate) fn compress(&self) -> bool {
        self.compress
    }

    pub(crate) fn format(&self) -> SqlResultFormat {
        self.format
    }

    pub(crate) fn in_row_order(&self) -> bool {
        matches!(self.format, SqlResultFormat::TableBytesInRowOrder)
    }
}

////////////////////////////////////////////////////////////////////////////////
