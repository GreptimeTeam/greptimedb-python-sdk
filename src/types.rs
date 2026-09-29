use std::time::Duration;

use pyo3::prelude::*;

use crate::error::raise;
use greptimedb_ingester::api::v1::ColumnDataType as ProtoType;
use greptimedb_ingester::{CompressionType, GrpcCompression};

#[pyclass(eq, eq_int, frozen, name = "SemanticType")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SemanticType {
    #[pyo3(name = "TAG")]
    Tag = 0,
    #[pyo3(name = "FIELD")]
    Field = 1,
    #[pyo3(name = "TIMESTAMP")]
    Timestamp = 2,
}

#[pymethods]
impl SemanticType {
    fn __repr__(&self) -> String {
        format!("SemanticType.{self:?}")
            .replace("Tag", "TAG")
            .replace("Field", "FIELD")
            .replace("Timestamp", "TIMESTAMP")
    }
}

#[pyclass(eq, eq_int, frozen, name = "ColumnDataType")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColumnDataType {
    #[pyo3(name = "BOOLEAN")]
    Boolean = 0,
    #[pyo3(name = "INT8")]
    Int8 = 1,
    #[pyo3(name = "INT16")]
    Int16 = 2,
    #[pyo3(name = "INT32")]
    Int32 = 3,
    #[pyo3(name = "INT64")]
    Int64 = 4,
    #[pyo3(name = "UINT8")]
    Uint8 = 5,
    #[pyo3(name = "UINT16")]
    Uint16 = 6,
    #[pyo3(name = "UINT32")]
    Uint32 = 7,
    #[pyo3(name = "UINT64")]
    Uint64 = 8,
    #[pyo3(name = "FLOAT32")]
    Float32 = 9,
    #[pyo3(name = "FLOAT64")]
    Float64 = 10,
    #[pyo3(name = "BINARY")]
    Binary = 11,
    #[pyo3(name = "STRING")]
    String = 12,
    #[pyo3(name = "DATE")]
    Date = 13,
    #[pyo3(name = "TIMESTAMP_SECOND")]
    TimestampSecond = 15,
    #[pyo3(name = "TIMESTAMP_MILLISECOND")]
    TimestampMillisecond = 16,
    #[pyo3(name = "TIMESTAMP_MICROSECOND")]
    TimestampMicrosecond = 17,
    #[pyo3(name = "TIMESTAMP_NANOSECOND")]
    TimestampNanosecond = 18,
    #[pyo3(name = "TIME_SECOND")]
    TimeSecond = 19,
    #[pyo3(name = "TIME_MILLISECOND")]
    TimeMillisecond = 20,
    #[pyo3(name = "TIME_MICROSECOND")]
    TimeMicrosecond = 21,
    #[pyo3(name = "TIME_NANOSECOND")]
    TimeNanosecond = 22,
    #[pyo3(name = "DECIMAL128")]
    Decimal128 = 30,
    #[pyo3(name = "JSON")]
    Json = 31,
}

#[pymethods]
impl ColumnDataType {
    fn __repr__(&self) -> String {
        let name = match self {
            Self::Boolean => "BOOLEAN",
            Self::Int8 => "INT8",
            Self::Int16 => "INT16",
            Self::Int32 => "INT32",
            Self::Int64 => "INT64",
            Self::Uint8 => "UINT8",
            Self::Uint16 => "UINT16",
            Self::Uint32 => "UINT32",
            Self::Uint64 => "UINT64",
            Self::Float32 => "FLOAT32",
            Self::Float64 => "FLOAT64",
            Self::Binary => "BINARY",
            Self::String => "STRING",
            Self::Date => "DATE",
            Self::TimestampSecond => "TIMESTAMP_SECOND",
            Self::TimestampMillisecond => "TIMESTAMP_MILLISECOND",
            Self::TimestampMicrosecond => "TIMESTAMP_MICROSECOND",
            Self::TimestampNanosecond => "TIMESTAMP_NANOSECOND",
            Self::TimeSecond => "TIME_SECOND",
            Self::TimeMillisecond => "TIME_MILLISECOND",
            Self::TimeMicrosecond => "TIME_MICROSECOND",
            Self::TimeNanosecond => "TIME_NANOSECOND",
            Self::Decimal128 => "DECIMAL128",
            Self::Json => "JSON",
        };
        format!("ColumnDataType.{name}")
    }
}

impl ColumnDataType {
    pub fn proto(self) -> ProtoType {
        match self {
            Self::Boolean => ProtoType::Boolean,
            Self::Int8 => ProtoType::Int8,
            Self::Int16 => ProtoType::Int16,
            Self::Int32 => ProtoType::Int32,
            Self::Int64 => ProtoType::Int64,
            Self::Uint8 => ProtoType::Uint8,
            Self::Uint16 => ProtoType::Uint16,
            Self::Uint32 => ProtoType::Uint32,
            Self::Uint64 => ProtoType::Uint64,
            Self::Float32 => ProtoType::Float32,
            Self::Float64 => ProtoType::Float64,
            Self::Binary => ProtoType::Binary,
            Self::String => ProtoType::String,
            Self::Date => ProtoType::Date,
            Self::TimestampSecond => ProtoType::TimestampSecond,
            Self::TimestampMillisecond => ProtoType::TimestampMillisecond,
            Self::TimestampMicrosecond => ProtoType::TimestampMicrosecond,
            Self::TimestampNanosecond => ProtoType::TimestampNanosecond,
            Self::TimeSecond => ProtoType::TimeSecond,
            Self::TimeMillisecond => ProtoType::TimeMillisecond,
            Self::TimeMicrosecond => ProtoType::TimeMicrosecond,
            Self::TimeNanosecond => ProtoType::TimeNanosecond,
            Self::Decimal128 => ProtoType::Decimal128,
            Self::Json => ProtoType::Json,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Boolean => "boolean",
            Self::Int8 => "int8",
            Self::Int16 => "int16",
            Self::Int32 => "int32",
            Self::Int64 => "int64",
            Self::Uint8 => "uint8",
            Self::Uint16 => "uint16",
            Self::Uint32 => "uint32",
            Self::Uint64 => "uint64",
            Self::Float32 => "float32",
            Self::Float64 => "float64",
            Self::Binary => "binary",
            Self::String => "string",
            Self::Date => "date",
            Self::TimestampSecond => "timestamp_second",
            Self::TimestampMillisecond => "timestamp_millisecond",
            Self::TimestampMicrosecond => "timestamp_microsecond",
            Self::TimestampNanosecond => "timestamp_nanosecond",
            Self::TimeSecond => "time_second",
            Self::TimeMillisecond => "time_millisecond",
            Self::TimeMicrosecond => "time_microsecond",
            Self::TimeNanosecond => "time_nanosecond",
            Self::Decimal128 => "decimal128",
            Self::Json => "json",
        }
    }
}

#[pyclass(name = "Column")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Column {
    pub(crate) name: String,
    pub(crate) data_type: ColumnDataType,
    pub(crate) semantic_type: SemanticType,
    pub(crate) precision: Option<u8>,
    pub(crate) scale: Option<i8>,
}

#[pymethods]
impl Column {
    #[new]
    #[pyo3(signature = (name, data_type, semantic_type, precision=None, scale=None))]
    fn new(
        name: String,
        data_type: ColumnDataType,
        semantic_type: SemanticType,
        precision: Option<u8>,
        scale: Option<i8>,
    ) -> PyResult<Self> {
        if name.is_empty() {
            return Err(raise("column name must not be empty", false));
        }
        match (data_type, precision, scale) {
            (ColumnDataType::Decimal128, Some(precision), Some(scale)) => {
                if precision == 0 || precision > 38 {
                    return Err(raise(
                        "decimal128 precision must be between 1 and 38",
                        false,
                    ));
                }
                if scale < 0 || i32::from(scale) > i32::from(precision) {
                    return Err(raise(
                        "decimal128 scale must be between 0 and precision",
                        false,
                    ));
                }
            }
            (ColumnDataType::Decimal128, _, _) => {
                return Err(raise(
                    "decimal128 columns require precision and scale",
                    false,
                ));
            }
            (_, Some(_), _) | (_, _, Some(_)) => {
                return Err(raise(
                    "precision and scale are only valid for decimal128 columns",
                    false,
                ));
            }
            _ => {}
        }
        Ok(Self {
            name,
            data_type,
            semantic_type,
            precision,
            scale,
        })
    }

    #[getter]
    fn name(&self) -> &str {
        &self.name
    }

    #[getter]
    fn data_type(&self) -> ColumnDataType {
        self.data_type
    }

    #[getter]
    fn semantic_type(&self) -> SemanticType {
        self.semantic_type
    }

    #[getter]
    fn precision(&self) -> Option<u8> {
        self.precision
    }

    #[getter]
    fn scale(&self) -> Option<i8> {
        self.scale
    }

    fn __repr__(&self) -> String {
        match (self.precision, self.scale) {
            (Some(precision), Some(scale)) => format!(
                "Column({:?}, {}, {}, precision={}, scale={})",
                self.name,
                self.data_type.__repr__(),
                self.semantic_type.__repr__(),
                precision,
                scale
            ),
            _ => format!(
                "Column({:?}, {}, {})",
                self.name,
                self.data_type.__repr__(),
                self.semantic_type.__repr__()
            ),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BulkCompression {
    None,
    Lz4,
    Zstd,
}

impl BulkCompression {
    pub fn parse(value: &str) -> PyResult<Self> {
        match value.to_ascii_lowercase().as_str() {
            "none" => Ok(Self::None),
            "lz4" => Ok(Self::Lz4),
            "zstd" => Ok(Self::Zstd),
            _ => Err(raise("compression must be 'none', 'lz4', or 'zstd'", false)),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Lz4 => "lz4",
            Self::Zstd => "zstd",
        }
    }

    pub fn into_ingester(self) -> CompressionType {
        match self {
            Self::None => CompressionType::None,
            Self::Lz4 => CompressionType::Lz4,
            Self::Zstd => CompressionType::Zstd,
        }
    }
}

pub fn parse_grpc_compression(value: &str) -> PyResult<GrpcCompression> {
    match value.to_ascii_lowercase().as_str() {
        "gzip" => Ok(GrpcCompression::Gzip),
        "zstd" => Ok(GrpcCompression::Zstd),
        _ => Err(raise("gRPC compression must be 'gzip' or 'zstd'", false)),
    }
}

#[pyclass(name = "WriteOptions")]
#[derive(Clone, Debug)]
pub struct WriteOptions {
    pub(crate) compression: BulkCompression,
    pub(crate) timeout: Duration,
    pub(crate) parallelism: usize,
}

#[pymethods]
impl WriteOptions {
    #[new]
    #[pyo3(signature = (compression = "lz4", timeout_secs = 60.0, parallelism = 4))]
    fn new(compression: &str, timeout_secs: f64, parallelism: usize) -> PyResult<Self> {
        if timeout_secs < 0.0 {
            return Err(raise("timeout_secs must be >= 0", false));
        }
        if parallelism == 0 {
            return Err(raise("parallelism must be >= 1", false));
        }
        Ok(Self {
            compression: BulkCompression::parse(compression)?,
            timeout: Duration::from_secs_f64(timeout_secs),
            parallelism,
        })
    }

    #[getter]
    fn compression(&self) -> &'static str {
        self.compression.as_str()
    }

    #[getter]
    fn timeout_secs(&self) -> f64 {
        self.timeout.as_secs_f64()
    }

    #[getter]
    fn parallelism(&self) -> usize {
        self.parallelism
    }

    fn __repr__(&self) -> String {
        format!(
            "WriteOptions(compression={:?}, timeout_secs={}, parallelism={})",
            self.compression.as_str(),
            self.timeout.as_secs_f64(),
            self.parallelism
        )
    }
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            compression: BulkCompression::Lz4,
            timeout: Duration::from_secs(60),
            parallelism: 4,
        }
    }
}

#[pyclass(name = "BulkResponse")]
#[derive(Clone, Debug)]
pub struct BulkResponse {
    #[pyo3(get)]
    pub request_id: i64,
    #[pyo3(get)]
    pub affected_rows: u64,
}

#[pymethods]
impl BulkResponse {
    fn __repr__(&self) -> String {
        format!(
            "BulkResponse(request_id={}, affected_rows={})",
            self.request_id, self.affected_rows
        )
    }
}
