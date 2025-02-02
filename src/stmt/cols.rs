use crate::{Result, oci::{self, *}, types::{date, number, raw, varchar}};
use libc::c_void;
use std::{collections::HashMap, ptr};

pub(crate) const DEFAULT_LONG_BUFFER_SIZE: u32 = 32768;

/// Column data type.
#[derive(Debug, PartialEq)]
pub enum ColumnType {
    /// Less common type for which data type decoder has not been implemented (yet).
    Unknown,
    Char,
    NChar,
    Varchar,
    NVarchar,
    Clob,
    NClob,
    Long,
    Raw,
    LongRaw,
    Blob,
    Number,
    BinaryFloat,
    BinaryDouble,
    Date,
    Timestamp,
    TimestampWithTimeZone,
    TimestampWithLocalTimeZone,
    IntervalYearToMonth,
    IntervalDayToSecond,
    RowID,
    Cursor,
}

impl std::fmt::Display for ColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ColumnType::Unknown => write!(f, "UNKNOWN"),
            ColumnType::Char => write!(f, "CHAR"),
            ColumnType::NChar => write!(f, "NCHAR"),
            ColumnType::Varchar => write!(f, "VARCHAR2"),
            ColumnType::NVarchar => write!(f, "NVARCHAR2"),
            ColumnType::Clob => write!(f, "CLOB"),
            ColumnType::NClob => write!(f, "NCLOB"),
            ColumnType::Long => write!(f, "LONG"),
            ColumnType::Raw => write!(f, "RAW"),
            ColumnType::LongRaw => write!(f, "LONG RAW"),
            ColumnType::Blob => write!(f, "BLOB"),
            ColumnType::Number => write!(f, "NUMBER"),
            ColumnType::BinaryFloat => write!(f, "BINARY_FLOAT"),
            ColumnType::BinaryDouble => write!(f, "BINARY_DOUBLE"),
            ColumnType::Date => write!(f, "DATE"),
            ColumnType::Timestamp => write!(f, "TIMESTAMP"),
            ColumnType::TimestampWithTimeZone => write!(f, "TIMESTAMP WITH TIME ZONE"),
            ColumnType::TimestampWithLocalTimeZone => write!(f, "TIMESTAMP WITH LOCAL TIME ZONE"),
            ColumnType::IntervalYearToMonth => write!(f, "INTERVAL YEAR TO MONTH"),
            ColumnType::IntervalDayToSecond => write!(f, "INTERVAL DAY TO SECOND"),
            ColumnType::RowID => write!(f, "ROWID"),
            ColumnType::Cursor => write!(f, "SYS_REFCURSOR"),
        }
    }
}

/// Provides access to the column metadata.
pub struct ColumnInfo<'a> {
    desc: Ptr<OCIParam>,
    err:  &'a OCIError,
}

impl<'a> ColumnInfo<'a> {
    pub(crate) fn new(desc: Ptr<OCIParam>, err: &'a OCIError) -> Self {
        Self { desc, err }
    }

    fn get_attr<T: attr::AttrGet>(&self, attr: u32) -> Result<T> {
        attr::get(attr, OCI_DTYPE_PARAM, self.desc.as_ref(), self.err)
    }

    /// Returns `true` if a column is visible
    pub fn is_visible(&self) -> Result<bool> {
        let invisible: u8 = self.get_attr(OCI_ATTR_INVISIBLE_COL)?;
        Ok(invisible == 0)
    }

    /// Returns `true` if NULLs are permitted in the column.
    ///
    /// Does not return a correct value for a CUBE or ROLLUP operation.
    pub fn is_null(&self) -> Result<bool> {
        let is_null: u8 = self.get_attr(OCI_ATTR_IS_NULL)?;
        Ok(is_null != 0)
    }

    /// Returns `true` if column is an identity column.
    pub fn is_identity(&self) -> Result<bool> {
        let col_props: u8 = self.get_attr(OCI_ATTR_COL_PROPERTIES)?;
        Ok(col_props & OCI_ATTR_COL_PROPERTY_IS_IDENTITY != 0)
    }

    /// Returns `true` if column value is GENERATED ALWAYS.
    /// `false` means that the value is GENERATED BY DEFAULT.
    pub fn is_generated_always(&self) -> Result<bool> {
        let col_props: u8 = self.get_attr(OCI_ATTR_COL_PROPERTIES)?;
        Ok(col_props & OCI_ATTR_COL_PROPERTY_IS_GEN_ALWAYS != 0)
    }

    /// Returns true if column was declared as GENERATED BY DEFAULT ON NULL.
    pub fn is_generated_on_null(&self) -> Result<bool> {
        let col_props: u8 = self.get_attr(OCI_ATTR_COL_PROPERTIES)?;
        Ok(col_props & OCI_ATTR_COL_PROPERTY_IS_GEN_BY_DEF_ON_NULL != 0)
    }

    /// Returns the column name
    pub fn name(&self) -> Result<&str> {
        self.get_attr::<&str>(OCI_ATTR_NAME)
    }

    /// Returns the maximum size of the column in bytes.
    /// For example, it returns 22 for NUMBERs.
    pub fn size(&self) -> Result<usize> {
        let size = self.get_attr::<u16>(OCI_ATTR_DATA_SIZE)? as usize;
        Ok(size)
    }

    /// Returns the column character length that is the number of characters allowed in the column.
    ///
    /// It is the counterpart of `size`, which gets the byte length.
    pub fn char_size(&self) -> Result<usize> {
        let size = self.get_attr::<u16>(OCI_ATTR_CHAR_SIZE)? as usize;
        Ok(size)
    }

    /// The precision of numeric columns.
    ///
    /// If the precision is nonzero and scale is -127, then it is a FLOAT; otherwise, it is a NUMBER(precision, scale).
    /// When precision is 0, NUMBER(precision, scale) can be represented simply as NUMBER.
    pub fn precision(&self) -> Result<i16> {
        self.get_attr::<i16>(OCI_ATTR_PRECISION)
    }

    /// The scale of numeric columns.
    ///
    /// If the precision is nonzero and scale is -127, then it is a FLOAT; otherwise, it is a NUMBER(precision, scale).
    /// When precision is 0, NUMBER(precision, scale) can be represented simply as NUMBER.
    pub fn scale(&self) -> Result<i8> {
        self.get_attr::<i8>(OCI_ATTR_SCALE)
    }

    /// Returns column data type.
    pub fn data_type(&self) -> Result<ColumnType> {
        let col_type = match self.get_attr::<u16>(OCI_ATTR_DATA_TYPE)? {
            SQLT_RDD => ColumnType::RowID,
            SQLT_CHR => match self.get_attr::<u8>(OCI_ATTR_CHARSET_FORM)? {
                SQLCS_NCHAR => ColumnType::NVarchar,
                _ => ColumnType::Varchar,
            },
            SQLT_AFC => match self.get_attr::<u8>(OCI_ATTR_CHARSET_FORM)? {
                SQLCS_NCHAR => ColumnType::NChar,
                _ => ColumnType::Char,
            },
            SQLT_CLOB => match self.get_attr::<u8>(OCI_ATTR_CHARSET_FORM)? {
                SQLCS_NCHAR => ColumnType::NClob,
                _ => ColumnType::Clob,
            },
            SQLT_LNG => ColumnType::Long,
            SQLT_BIN => ColumnType::Raw,
            SQLT_LBI => ColumnType::LongRaw,
            SQLT_BLOB => ColumnType::Blob,
            SQLT_NUM => ColumnType::Number,
            SQLT_DAT => ColumnType::Date,
            SQLT_TIMESTAMP => ColumnType::Timestamp,
            SQLT_TIMESTAMP_TZ => ColumnType::TimestampWithTimeZone,
            SQLT_TIMESTAMP_LTZ => ColumnType::TimestampWithLocalTimeZone,
            SQLT_INTERVAL_YM => ColumnType::IntervalYearToMonth,
            SQLT_INTERVAL_DS => ColumnType::IntervalDayToSecond,
            SQLT_IBFLOAT => ColumnType::BinaryFloat,
            SQLT_IBDOUBLE => ColumnType::BinaryDouble,
            SQLT_RSET => ColumnType::Cursor,
            _ => ColumnType::Unknown,
        };
        Ok(col_type)
    }

    /// Returns the column type name:
    /// - If the data type is SQLT_NTY, the name of the named data type's type is returned.
    /// - If the data type is SQLT_REF, the type name of the named data type pointed to by the REF is returned.
    /// - If the data type is anything other than SQLT_NTY or SQLT_REF, an empty string is returned.
    pub fn type_name(&self) -> Result<&str> {
        self.get_attr::<&str>(OCI_ATTR_TYPE_NAME)
    }

    /// Returns the schema name under which the type has been created.
    pub fn schema_name(&self) -> Result<&str> {
        self.get_attr::<&str>(OCI_ATTR_SCHEMA_NAME)
    }
}

/// Public face of the private column buffer
// pub struct ColumnData {
//     pub(crate) buf: ColumnBuffer
// }

/// Column output buffer
pub enum ColumnBuffer {
    Text(Ptr<OCIString>),
    CLOB(Descriptor<OCICLobLocator>),
    Binary(Ptr<OCIRaw>),
    BLOB(Descriptor<OCIBLobLocator>),
    BFile(Descriptor<OCIBFileLocator>),
    Number(Box<OCINumber>),
    Date(OCIDate),
    Timestamp(Descriptor<OCITimestamp>),
    TimestampTZ(Descriptor<OCITimestampTZ>),
    TimestampLTZ(Descriptor<OCITimestampLTZ>),
    IntervalYM(Descriptor<OCIIntervalYearToMonth>),
    IntervalDS(Descriptor<OCIIntervalDayToSecond>),
    Float(f32),
    Double(f64),
    Rowid(Descriptor<OCIRowid>),
    Cursor(Handle<OCIStmt>),
}

impl ColumnBuffer {
    fn new(data_type: u16, data_size: u32, env: &impl AsRef<OCIEnv>, err: &impl AsRef<OCIError>) -> Result<Self> {
        let val = match data_type {
            SQLT_DAT => ColumnBuffer::Date(date::new()),
            SQLT_TIMESTAMP => ColumnBuffer::Timestamp(Descriptor::<OCITimestamp>::new(env)?),
            SQLT_TIMESTAMP_TZ => ColumnBuffer::TimestampTZ(Descriptor::<OCITimestampTZ>::new(env)?),
            SQLT_TIMESTAMP_LTZ => {
                ColumnBuffer::TimestampLTZ(Descriptor::<OCITimestampLTZ>::new(env)?)
            }
            SQLT_INTERVAL_YM => {
                ColumnBuffer::IntervalYM(Descriptor::<OCIIntervalYearToMonth>::new(env)?)
            }
            SQLT_INTERVAL_DS => {
                ColumnBuffer::IntervalDS(Descriptor::<OCIIntervalDayToSecond>::new(env)?)
            }
            SQLT_NUM => ColumnBuffer::Number(Box::new(number::new())),
            SQLT_IBFLOAT => ColumnBuffer::Float(0f32),
            SQLT_IBDOUBLE => ColumnBuffer::Double(0f64),
            SQLT_BIN | SQLT_LBI => ColumnBuffer::Binary(raw::new(data_size, env.as_ref(), err.as_ref())?),
            SQLT_CLOB => ColumnBuffer::CLOB(Descriptor::<OCICLobLocator>::new(env)?),
            SQLT_BLOB => ColumnBuffer::BLOB(Descriptor::<OCIBLobLocator>::new(env)?),
            SQLT_BFILE => ColumnBuffer::BFile(Descriptor::<OCIBFileLocator>::new(env)?),
            SQLT_RDD => ColumnBuffer::Rowid(Descriptor::<OCIRowid>::new(env)?),
            SQLT_RSET => ColumnBuffer::Cursor(Handle::<OCIStmt>::new(env)?),
            _ => ColumnBuffer::Text(varchar::new(data_size, env.as_ref(), err.as_ref())?),
        };
        Ok(val)
    }

    fn drop(&mut self, env: &OCIEnv, err: &OCIError) {
        match self {
            ColumnBuffer::Text(oci_str_ptr) => {
                varchar::free(oci_str_ptr, env, err);
            }
            ColumnBuffer::Binary(oci_raw_ptr) => {
                raw::free(oci_raw_ptr, env, err);
            }
            _ => {}
        }
    }

    // Returns (output type, pointer to the output buffer, buffer size)
    fn get_output_buffer_def(&mut self, col_size: usize) -> (u16, *mut c_void, usize) {
        use std::mem::size_of;
        match self {
            ColumnBuffer::Text(oci_str_ptr)   => (SQLT_LVC, oci_str_ptr.get() as *mut c_void, col_size + size_of::<u32>()),
            ColumnBuffer::Binary(oci_raw_ptr) => (SQLT_LVB, oci_raw_ptr.get() as *mut c_void, col_size + size_of::<u32>()),
            ColumnBuffer::Number(oci_num_box) => (SQLT_VNU, oci_num_box.as_mut() as *mut OCINumber as *mut c_void, size_of::<OCINumber>()),
            ColumnBuffer::Date(oci_date)      => (SQLT_ODT, oci_date as *mut OCIDate as *mut c_void, size_of::<OCIDate>()),
            ColumnBuffer::Timestamp(ts)       => (SQLT_TIMESTAMP, ts.as_ptr() as *mut c_void, size_of::<*mut OCIDateTime>()),
            ColumnBuffer::TimestampTZ(ts)     => (SQLT_TIMESTAMP_TZ, ts.as_ptr() as *mut c_void, size_of::<*mut OCIDateTime>()),
            ColumnBuffer::TimestampLTZ(ts)    => (SQLT_TIMESTAMP_LTZ, ts.as_ptr() as *mut c_void, size_of::<*mut OCIDateTime>()),
            ColumnBuffer::IntervalYM(int)     => (SQLT_INTERVAL_YM, int.as_ptr() as *mut c_void, size_of::<*mut OCIInterval>()),
            ColumnBuffer::IntervalDS(int)     => (SQLT_INTERVAL_DS, int.as_ptr() as *mut c_void, size_of::<*mut OCIInterval>()),
            ColumnBuffer::Float(val)          => (SQLT_BFLOAT, val as *mut f32 as *mut c_void, size_of::<f32>()),
            ColumnBuffer::Double(val)         => (SQLT_BDOUBLE, val as *mut f64 as *mut c_void, size_of::<f64>()),
            ColumnBuffer::CLOB(lob)           => (SQLT_CLOB, lob.as_ptr() as *mut c_void, size_of::<*mut OCILobLocator>()),
            ColumnBuffer::BLOB(lob)           => (SQLT_BLOB, lob.as_ptr() as *mut c_void, size_of::<*mut OCILobLocator>()),
            ColumnBuffer::BFile(lob)          => (SQLT_BFILE, lob.as_ptr() as *mut c_void, size_of::<*mut OCILobLocator>()),
            ColumnBuffer::Rowid(rowid)        => (SQLT_RDD, rowid.as_ptr() as *mut c_void, size_of::<*mut OCIRowid>()),
            ColumnBuffer::Cursor(handle)      => (SQLT_RSET, handle.as_ptr() as *mut c_void, 0),
        }
    }
}

/// Internal representation of a column from a SELECT projection
#[allow(dead_code)]
pub struct Column {
    buf: ColumnBuffer,
    inf: Descriptor<OCIParam>,
    def: Ptr<OCIDefine>,
    /// Length of data fetched
    len: u32,
    /// Output "indicator":
    /// * -2  : The length of the item is greater than the length of the output variable; the item has been truncated.
    ///         Unline the case of indicators that are > 0, the original length is longer than the maximum data length
    ///         that can be returned in the i16 indicator variable.
    /// * -1  : The selected value is null, and the value of the output variable is unchanged.
    /// *  0  : Oracle Database assigned an intact value to the host variable
    /// * \>0 : The length of the item is greater than the length of the output variable; the item has been truncated.
    ///         The positive value returned in the indicator variable is the actual length before truncation.
    ind: i16
}

impl Column {
    fn new(buf: ColumnBuffer, inf: Descriptor<OCIParam>) -> Self {
        Self {
            buf,
            inf,
            def: Ptr::<OCIDefine>::null(),
            len: 0,
            ind: 0
        }
    }

    pub fn is_null(&self) -> bool {
        self.ind == OCI_IND_NULL
    }

    pub fn data(&mut self) -> &mut ColumnBuffer {
        &mut self.buf
    }

    pub(crate) fn name(&self, err: &OCIError) -> Result<&str> {
        self.inf.get_attr(OCI_ATTR_NAME, err)
    }
}

/// Internal representation of columns from a SELECT projection
pub struct Columns {
    names: HashMap<&'static str, usize>,
    cols: Vec<Column>,
    env:  Ptr<OCIEnv>,
    err:  Ptr<OCIError>,
}

impl Drop for Columns {
    fn drop(&mut self) {
        for col in self.cols.iter_mut() {
            col.buf.drop(&self.env, &self.err);
        }
    }
}

impl Columns {
    pub(crate) fn new(stmt: Ptr<OCIStmt>, env: Ptr<OCIEnv>, err: Ptr<OCIError>, max_long_fetch_size: u32) -> Result<Self> {
        let num_columns : u32 = attr::get(OCI_ATTR_PARAM_COUNT, OCI_HTYPE_STMT, stmt.as_ref(), err.as_ref())?;
        let num_columns = num_columns as usize;

        let mut names = HashMap::with_capacity(num_columns);
        let mut cols  = Vec::with_capacity(num_columns);

        let utf8_factor = std::env::var("ORACLE_UTF8_CONV_FACTOR").ok().and_then(|val| val.parse::<u32>().ok()).unwrap_or(2);
        for i in 0..num_columns {
            let col_info = param::get((i + 1) as u32, OCI_HTYPE_STMT, stmt.as_ref(), err.as_ref())?;
            let data_type = col_info.get_attr::<u16>(OCI_ATTR_DATA_TYPE, err.as_ref())?;
            let data_size = match data_type {
                SQLT_LNG | SQLT_LBI => max_long_fetch_size,
                _ => col_info.get_attr::<u16>(OCI_ATTR_DATA_SIZE, err.as_ref())? as u32 * utf8_factor,
            };
            cols.push(Column::new(ColumnBuffer::new(data_type, data_size, &env, &err)?, col_info));

            // Now, that columns buffers are in the vector and thus their locations in memory are fixed,
            // define the output buffers in OCI

            let (output_type, output_buff_ptr, output_buff_size) = cols[i].buf.get_output_buffer_def(data_size as usize);
            oci::define_by_pos(
                stmt.as_ref(), cols[i].def.as_mut_ptr(), err.as_ref(),
                (i + 1) as u32,
                output_buff_ptr, output_buff_size as i64, output_type,
                &mut cols[i].ind,
                &mut cols[i].len,
                ptr::null_mut::<u16>(),
                OCI_DEFAULT
            )?;

            let name : &str = cols[i].inf.get_attr(OCI_ATTR_NAME, err.as_ref())?;
            names.insert(name, i);
        }
        Ok(Self { names, cols, env, err })
    }

    pub(crate) fn col_index(&self, name: &str) -> Option<usize> {
        self.names.get(name).map(|ix| *ix)
    }

    /// Returns Column at the specified index or None if column index is out of bounds.
    pub(crate) fn col(&self, index: usize) -> Option<&Column> {
        self.cols.get(index)
    }

    /// Returns mutable Column at the specified index or None if column index is out of bounds.
    pub(crate) fn col_mut(&mut self, index: usize) -> Option<&mut Column> {
        self.cols.get_mut(index)
    }

    /// Returns `true` if the last value fetched was NULL.
    pub(crate) fn is_null(&self, index: usize) -> bool {
        self.col(index).map_or(true, |col| col.is_null())
    }

    pub(crate) fn column_param<'a>(&'a self, index: usize) -> Option<Ptr<OCIParam>> {
        self.col(index).map(|col| col.inf.get_ptr())
    }
}
