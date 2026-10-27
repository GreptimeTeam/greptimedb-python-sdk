use pyo3::prelude::*;
use pyo3::types::{
    PyBool, PyByteArray, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple,
};

use crate::error::{raise, RemoteError};

#[derive(Clone, Debug)]
pub struct DecimalParts {
    pub negative: bool,
    pub digits: String,
    pub exponent: i32,
}

#[derive(Clone, Debug)]
pub enum Cell {
    Null,
    Bool(bool),
    Int(i128),
    Float(f64),
    Str(String),
    Bytes(Vec<u8>),
    Decimal(DecimalParts),
    /// Days since Unix epoch.
    DateDays(i32),
    /// Absolute timestamp. `secs` is the euclidean quotient of the instant.
    Timestamp {
        secs: i64,
        subsec_nanos: u32,
    },
    /// JSON text produced from a Python object, or supplied directly for JSON columns.
    Json(String),
}

impl Cell {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Bool(_) => "bool",
            Self::Int(_) => "int",
            Self::Float(_) => "float",
            Self::Str(_) => "string",
            Self::Bytes(_) => "bytes",
            Self::Decimal(_) => "Decimal",
            Self::DateDays(_) => "date",
            Self::Timestamp { .. } => "datetime",
            Self::Json(_) => "json",
        }
    }
}

pub fn cell_from_py(obj: &Bound<'_, PyAny>) -> PyResult<Cell> {
    if obj.is_none() {
        return Ok(Cell::Null);
    }
    if obj.is_instance_of::<PyBool>() {
        return Ok(Cell::Bool(obj.extract()?));
    }
    if obj.is_instance_of::<PyInt>() {
        let value = obj
            .extract::<i128>()
            .map_err(|_| raise("integer does not fit in i128", false))?;
        return Ok(Cell::Int(value));
    }
    if obj.is_instance_of::<PyFloat>() {
        let value: f64 = obj.extract()?;
        if !value.is_finite() {
            return Err(raise("float values must be finite", false));
        }
        return Ok(Cell::Float(value));
    }
    if obj.is_instance_of::<PyString>() {
        return Ok(Cell::Str(obj.extract()?));
    }
    if obj.is_instance_of::<PyBytes>() {
        return Ok(Cell::Bytes(obj.extract()?));
    }
    if obj.is_instance_of::<PyByteArray>() {
        let array = obj.downcast::<PyByteArray>()?;
        return Ok(Cell::Bytes(array.to_vec()));
    }

    let py = obj.py();
    let decimal_type = py.import("decimal")?.getattr("Decimal")?;
    if obj.is_instance(&decimal_type)? {
        return decimal_cell(obj);
    }

    let datetime_mod = py.import("datetime")?;
    let datetime_type = datetime_mod.getattr("datetime")?;
    let date_type = datetime_mod.getattr("date")?;
    if obj.is_instance(&datetime_type)? {
        return timestamp_cell(obj, &datetime_mod);
    }
    if obj.is_instance(&date_type)? {
        let ordinal: i32 = obj.call_method0("toordinal")?.extract()?;
        // `date(1970, 1, 1).toordinal()`
        return Ok(Cell::DateDays(ordinal - 719_163));
    }

    if obj.is_instance_of::<PyDict>()
        || obj.is_instance_of::<PyList>()
        || obj.is_instance_of::<PyTuple>()
    {
        let text: String = py
            .import("json")?
            .getattr("dumps")?
            .call1((obj,))?
            .extract()?;
        return Ok(Cell::Json(text));
    }

    Err(raise(
        format!("unsupported Python value {}", obj.get_type().name()?),
        false,
    ))
}

fn decimal_cell(obj: &Bound<'_, PyAny>) -> PyResult<Cell> {
    if !obj.call_method0("is_finite")?.extract::<bool>()? {
        return Err(raise("Decimal values must be finite", false));
    }
    let parts = obj.call_method0("as_tuple")?;
    let sign: u8 = parts.getattr("sign")?.extract()?;
    let digits = parts.getattr("digits")?;
    let mut text = String::new();
    for digit in digits.try_iter()? {
        let digit: u8 = digit?.extract()?;
        text.push(char::from(b'0' + digit));
    }
    if text.is_empty() {
        text.push('0');
    }
    let exponent: i32 = parts
        .getattr("exponent")?
        .extract()
        .map_err(|_| raise("Decimal exponent is not an integer", false))?;
    Ok(Cell::Decimal(DecimalParts {
        negative: sign == 1,
        digits: text,
        exponent,
    }))
}

fn timestamp_cell(obj: &Bound<'_, PyAny>, datetime_mod: &Bound<'_, PyAny>) -> PyResult<Cell> {
    let py = obj.py();
    let utc = datetime_mod.getattr("timezone")?.getattr("utc")?;
    let aware = if obj.getattr("tzinfo")?.is_none() {
        let kwargs = PyDict::new(py);
        kwargs.set_item("tzinfo", utc.clone())?;
        obj.call_method("replace", (), Some(&kwargs))?
    } else {
        obj.clone()
    };
    let epoch = datetime_mod.getattr("datetime")?.call1((1970, 1, 1))?;
    let epoch_kwargs = PyDict::new(py);
    epoch_kwargs.set_item("tzinfo", utc)?;
    let epoch = epoch.call_method("replace", (), Some(&epoch_kwargs))?;
    let delta = aware.sub(&epoch)?;
    let days: i64 = delta.getattr("days")?.extract()?;
    let seconds: i64 = delta.getattr("seconds")?.extract()?;
    let micros: i64 = delta.getattr("microseconds")?.extract()?;
    let total_us = days
        .checked_mul(86_400_000_000)
        .and_then(|v| v.checked_add(seconds.checked_mul(1_000_000)?))
        .and_then(|v| v.checked_add(micros))
        .ok_or_else(|| raise("datetime is out of range", false))?;
    let secs = total_us.div_euclid(1_000_000);
    let subsec_nanos = u32::try_from(total_us.rem_euclid(1_000_000) * 1_000)
        .map_err(|_| raise("datetime is out of range", false))?;
    Ok(Cell::Timestamp { secs, subsec_nanos })
}

pub fn type_mismatch(expected: &str, cell: &Cell) -> RemoteError {
    RemoteError::new(format!("expected {expected}, got {}", cell.kind()), false)
}

pub fn fit_int<T>(value: i128, label: &str) -> Result<T, RemoteError>
where
    T: TryFrom<i128>,
{
    T::try_from(value)
        .map_err(|_| RemoteError::new(format!("{label} value is out of range"), false))
}
