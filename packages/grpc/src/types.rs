use std::time::Duration;

use pyo3::prelude::*;

use crate::error::raise;
use greptimedb_ingester::api::v1::ColumnDataType as ProtoType;
use greptimedb_ingester::{CompressionType, GrpcCompression};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SemanticType {
    Tag = 0,
    Field = 1,
    Timestamp = 2,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColumnDataType {
    Boolean = 0,
    Int8 = 1,
    Int16 = 2,
    Int32 = 3,
    Int64 = 4,
    Uint8 = 5,
    Uint16 = 6,
    Uint32 = 7,
    Uint64 = 8,
    Float32 = 9,
    Float64 = 10,
    Binary = 11,
    String = 12,
    Date = 13,
    TimestampSecond = 15,
    TimestampMillisecond = 16,
    TimestampMicrosecond = 17,
    TimestampNanosecond = 18,
    TimeSecond = 19,
    TimeMillisecond = 20,
    TimeMicrosecond = 21,
    TimeNanosecond = 22,
    Decimal128 = 30,
    Json = 31,
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
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Column {
    pub(crate) name: String,
    pub(crate) data_type: ColumnDataType,
    pub(crate) semantic_type: SemanticType,
    pub(crate) precision: Option<u8>,
    pub(crate) scale: Option<i8>,
}

impl<'py> FromPyObject<'py> for Column {
    fn extract_bound(value: &Bound<'py, PyAny>) -> PyResult<Self> {
        let schema = value.py().import("greptimedb_ingester._types")?;
        if !value.is_instance(&schema.getattr("Column")?)? {
            return Err(raise("columns must contain Column objects", false));
        }
        let data_type = match value
            .getattr("data_type")?
            .getattr("value")?
            .extract::<u8>()?
        {
            0 => ColumnDataType::Boolean,
            1 => ColumnDataType::Int8,
            2 => ColumnDataType::Int16,
            3 => ColumnDataType::Int32,
            4 => ColumnDataType::Int64,
            5 => ColumnDataType::Uint8,
            6 => ColumnDataType::Uint16,
            7 => ColumnDataType::Uint32,
            8 => ColumnDataType::Uint64,
            9 => ColumnDataType::Float32,
            10 => ColumnDataType::Float64,
            11 => ColumnDataType::Binary,
            12 => ColumnDataType::String,
            13 => ColumnDataType::Date,
            15 => ColumnDataType::TimestampSecond,
            16 => ColumnDataType::TimestampMillisecond,
            17 => ColumnDataType::TimestampMicrosecond,
            18 => ColumnDataType::TimestampNanosecond,
            19 => ColumnDataType::TimeSecond,
            20 => ColumnDataType::TimeMillisecond,
            21 => ColumnDataType::TimeMicrosecond,
            22 => ColumnDataType::TimeNanosecond,
            30 => ColumnDataType::Decimal128,
            31 => ColumnDataType::Json,
            _ => return Err(raise("Invalid column data type", false)),
        };
        let semantic_type = match value
            .getattr("semantic_type")?
            .getattr("value")?
            .extract::<u8>()?
        {
            0 => SemanticType::Tag,
            1 => SemanticType::Field,
            2 => SemanticType::Timestamp,
            _ => return Err(raise("Invalid column semantic type", false)),
        };
        Ok(Self {
            name: value.getattr("name")?.extract()?,
            data_type,
            semantic_type,
            precision: value.getattr("precision")?.extract()?,
            scale: value.getattr("scale")?.extract()?,
        })
    }
}

impl Column {
    pub fn to_python(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let schema = py.import("greptimedb_ingester._types")?;
        let dtype = schema
            .getattr("ColumnDataType")?
            .call1((self.data_type as u8,))?;
        let semantic = schema
            .getattr("SemanticType")?
            .call1((self.semantic_type as u8,))?;
        Ok(schema
            .getattr("Column")?
            .call1((&self.name, dtype, semantic, self.precision, self.scale))?
            .unbind())
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
