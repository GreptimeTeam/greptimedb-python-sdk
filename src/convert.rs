use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use greptimedb_ingester::api::v1::column::Values;
use greptimedb_ingester::api::v1::value::ValueData;
use greptimedb_ingester::api::v1::Row as ProtoRow;
use greptimedb_ingester::api::v1::{
    column_data_type_extension::TypeExt, Column as ProtoColumn, ColumnDataTypeExtension,
    ColumnSchema, DecimalTypeExtension, DeleteRequest, DeleteRequests, RowInsertRequest,
    RowInsertRequests, Rows as ProtoRows,
};
use greptimedb_ingester::helpers::schema::{field, tag, timestamp};
use greptimedb_ingester::helpers::values::{
    binary_value, bool_value, date_value, decimal128_value, f32_value, f64_value, i16_value,
    i32_value, i64_value, i8_value, none_value,
    string_value, time_microsecond_value, time_millisecond_value, time_nanosecond_value,
    time_second_value, timestamp_microsecond_value, timestamp_millisecond_value,
    timestamp_nanosecond_value, timestamp_second_value, u16_value, u32_value, u64_value, u8_value,
};
use greptimedb_ingester::Value;

use crate::cell::{cell_from_py, fit_int, type_mismatch, Cell, DecimalParts};
use crate::error::RemoteError;
use crate::types::{Column, ColumnDataType, SemanticType};

#[derive(Clone, Debug)]
pub enum RowInput {
    Positional(Vec<Cell>),
    Named(Vec<(String, Cell)>),
}

pub fn parse_row(row: &Bound<'_, PyAny>) -> PyResult<RowInput> {
    if let Ok(dict) = row.downcast::<PyDict>() {
        let mut pairs = Vec::with_capacity(dict.len());
        for (key, value) in dict.iter() {
            let name: String = key
                .extract()
                .map_err(|_| crate::error::raise("row dict keys must be strings", false))?;
            pairs.push((name, cell_from_py(&value)?));
        }
        return Ok(RowInput::Named(pairs));
    }
    if row.is_instance_of::<PyList>() || row.is_instance_of::<PyTuple>() {
        let mut cells = Vec::new();
        for item in row.try_iter()? {
            cells.push(cell_from_py(&item?)?);
        }
        return Ok(RowInput::Positional(cells));
    }
    Err(crate::error::raise(
        "a row must be a list, tuple, or dict",
        false,
    ))
}

pub fn align(columns: &[Column], input: &RowInput) -> Result<Vec<Cell>, RemoteError> {
    match input {
        RowInput::Positional(cells) => {
            if cells.len() != columns.len() {
                return Err(RemoteError::new(
                    format!(
                        "invalid column count: expected {}, got {}",
                        columns.len(),
                        cells.len()
                    ),
                    false,
                ));
            }
            Ok(cells.clone())
        }
        RowInput::Named(pairs) => {
            let mut slots: Vec<Option<Cell>> = vec![None; columns.len()];
            for (name, cell) in pairs {
                let index = columns
                    .iter()
                    .position(|column| column.name == *name)
                    .ok_or_else(|| RemoteError::new(format!("unknown column {name}"), false))?;
                if slots[index].is_some() {
                    return Err(RemoteError::new(format!("duplicate column {name}"), false));
                }
                slots[index] = Some(cell.clone());
            }
            let mut cells = Vec::with_capacity(columns.len());
            for (index, slot) in slots.into_iter().enumerate() {
                match slot {
                    Some(cell) => cells.push(cell),
                    None => {
                        return Err(RemoteError::new(
                            format!("missing column {}", columns[index].name),
                            false,
                        ));
                    }
                }
            }
            Ok(cells)
        }
    }
}

pub fn column_schema(column: &Column) -> Result<ColumnSchema, RemoteError> {
    let dtype = column.data_type.proto();
    let mut schema = match column.semantic_type {
        SemanticType::Tag => tag(&column.name, dtype),
        SemanticType::Timestamp => timestamp(&column.name, dtype),
        SemanticType::Field => field(&column.name, dtype),
    };
    if column.data_type == ColumnDataType::Decimal128 {
        let precision = i32::from(column.precision.expect("decimal precision"));
        let scale = i32::from(column.scale.expect("decimal scale"));
        schema.datatype_extension = Some(ColumnDataTypeExtension {
            type_ext: Some(TypeExt::DecimalType(DecimalTypeExtension {
                precision,
                scale,
            })),
        });
    }
    Ok(schema)
}

pub fn build_insert(
    table: &str,
    columns: &[Column],
    rows: &[Vec<Cell>],
) -> Result<RowInsertRequests, RemoteError> {
    if rows.is_empty() {
        return Err(RemoteError::new("cannot insert an empty row batch", false));
    }
    let schema = columns
        .iter()
        .map(column_schema)
        .collect::<Result<Vec<_>, _>>()?;
    let mut proto_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let mut values = Vec::with_capacity(columns.len());
        for (column, cell) in columns.iter().zip(row.iter()) {
            values.push(to_proto(column, cell)?);
        }
        proto_rows.push(ProtoRow { values });
    }
    Ok(RowInsertRequests {
        inserts: vec![RowInsertRequest {
            table_name: table.to_owned(),
            rows: Some(ProtoRows {
                schema,
                rows: proto_rows,
            }),
        }],
    })
}

pub fn build_delete(
    table: &str,
    columns: &[Column],
    rows: &[Vec<Cell>],
) -> Result<DeleteRequests, RemoteError> {
    if rows.is_empty() {
        return Err(RemoteError::new("cannot delete an empty row batch", false));
    }
    let row_count = u32::try_from(rows.len())
        .map_err(|_| RemoteError::new("too many rows to delete in one request", false))?;
    let mut key_columns = columns
        .iter()
        .map(empty_proto_column)
        .collect::<Result<Vec<_>, _>>()?;
    for (row_index, row) in rows.iter().enumerate() {
        for (column, (proto_column, cell)) in
            columns.iter().zip(key_columns.iter_mut().zip(row.iter()))
        {
            push_column_value(proto_column, row_index, to_proto(column, cell)?)?;
        }
    }
    Ok(DeleteRequests {
        deletes: vec![DeleteRequest {
            table_name: table.to_owned(),
            key_columns,
            row_count,
        }],
    })
}

fn empty_proto_column(column: &Column) -> Result<ProtoColumn, RemoteError> {
    let schema = column_schema(column)?;
    Ok(ProtoColumn {
        column_name: column.name.clone(),
        semantic_type: column.semantic_type as i32,
        values: Some(Values::default()),
        null_mask: Vec::new(),
        datatype: column.data_type.proto() as i32,
        datatype_extension: schema.datatype_extension,
        options: schema.options,
    })
}

fn push_column_value(
    column: &mut ProtoColumn,
    row_index: usize,
    value: greptimedb_ingester::api::v1::Value,
) -> Result<(), RemoteError> {
    let Some(value_data) = value.value_data else {
        let byte = row_index / 8;
        if column.null_mask.len() <= byte {
            column.null_mask.resize(byte + 1, 0);
        }
        column.null_mask[byte] |= 1 << (row_index % 8);
        return Ok(());
    };
    let values = column.values.get_or_insert_with(Values::default);
    match value_data {
        ValueData::I8Value(v) => values.i8_values.push(v),
        ValueData::I16Value(v) => values.i16_values.push(v),
        ValueData::I32Value(v) => values.i32_values.push(v),
        ValueData::I64Value(v) => values.i64_values.push(v),
        ValueData::U8Value(v) => values.u8_values.push(v),
        ValueData::U16Value(v) => values.u16_values.push(v),
        ValueData::U32Value(v) => values.u32_values.push(v),
        ValueData::U64Value(v) => values.u64_values.push(v),
        ValueData::F32Value(v) => values.f32_values.push(v),
        ValueData::F64Value(v) => values.f64_values.push(v),
        ValueData::BoolValue(v) => values.bool_values.push(v),
        ValueData::BinaryValue(v) => values.binary_values.push(v),
        ValueData::StringValue(v) => values.string_values.push(v),
        ValueData::DateValue(v) => values.date_values.push(v),
        ValueData::TimestampSecondValue(v) => values.timestamp_second_values.push(v),
        ValueData::TimestampMillisecondValue(v) => values.timestamp_millisecond_values.push(v),
        ValueData::TimestampMicrosecondValue(v) => values.timestamp_microsecond_values.push(v),
        ValueData::TimestampNanosecondValue(v) => values.timestamp_nanosecond_values.push(v),
        ValueData::TimeSecondValue(v) => values.time_second_values.push(v),
        ValueData::TimeMillisecondValue(v) => values.time_millisecond_values.push(v),
        ValueData::TimeMicrosecondValue(v) => values.time_microsecond_values.push(v),
        ValueData::TimeNanosecondValue(v) => values.time_nanosecond_values.push(v),
        ValueData::Decimal128Value(v) => values.decimal128_values.push(v),
        ValueData::DatetimeValue(_)
        | ValueData::IntervalYearMonthValue(_)
        | ValueData::IntervalDayTimeValue(_)
        | ValueData::IntervalMonthDayNanoValue(_)
        | ValueData::JsonValue(_)
        | ValueData::ListValue(_)
        | ValueData::StructValue(_) => {
            return Err(RemoteError::new(
                "this value cannot be encoded in a delete request",
                false,
            ));
        }
    }
    Ok(())
}

pub fn to_proto(
    column: &Column,
    cell: &Cell,
) -> Result<greptimedb_ingester::api::v1::Value, RemoteError> {
    if matches!(cell, Cell::Null) {
        if column.semantic_type == SemanticType::Timestamp || column.data_type.is_timestamp() {
            return Err(RemoteError::new(
                format!("timestamp column {} cannot be null", column.name),
                false,
            ));
        }
        return Ok(none_value());
    }
    let value = match column.data_type {
        ColumnDataType::Boolean => bool_value(expect_bool(cell)?),
        ColumnDataType::Int8 => i8_value(expect_int(cell, "int8")?),
        ColumnDataType::Int16 => i16_value(expect_int(cell, "int16")?),
        ColumnDataType::Int32 => i32_value(expect_int(cell, "int32")?),
        ColumnDataType::Int64 => i64_value(expect_int(cell, "int64")?),
        ColumnDataType::Uint8 => u8_value(expect_int(cell, "uint8")?),
        ColumnDataType::Uint16 => u16_value(expect_int(cell, "uint16")?),
        ColumnDataType::Uint32 => u32_value(expect_int(cell, "uint32")?),
        ColumnDataType::Uint64 => u64_value(expect_int(cell, "uint64")?),
        ColumnDataType::Float32 => f32_value(expect_f32(cell)?),
        ColumnDataType::Float64 => f64_value(expect_f64(cell)?),
        ColumnDataType::Binary => binary_value(expect_bytes(cell)?),
        ColumnDataType::String => string_value(expect_str(cell)?.to_owned()),
        ColumnDataType::Date => date_value(expect_date(cell)?),
        ColumnDataType::TimestampSecond => {
            timestamp_second_value(expect_epoch(cell, 1_000_000_000)?)
        }
        ColumnDataType::TimestampMillisecond => {
            timestamp_millisecond_value(expect_epoch(cell, 1_000_000)?)
        }
        ColumnDataType::TimestampMicrosecond => {
            timestamp_microsecond_value(expect_epoch(cell, 1_000)?)
        }
        ColumnDataType::TimestampNanosecond => timestamp_nanosecond_value(expect_epoch(cell, 1)?),
        ColumnDataType::TimeSecond => time_second_value(expect_int(cell, "time_second")?),
        ColumnDataType::TimeMillisecond => {
            time_millisecond_value(expect_int(cell, "time_millisecond")?)
        }
        ColumnDataType::TimeMicrosecond => {
            time_microsecond_value(expect_int(cell, "time_microsecond")?)
        }
        ColumnDataType::TimeNanosecond => {
            time_nanosecond_value(expect_int(cell, "time_nanosecond")?)
        }
        ColumnDataType::Decimal128 => {
            decimal128_value(expect_decimal(cell, column.scale.expect("scale"))?)
        }
        ColumnDataType::Json => {
            let text = expect_json_text(cell)?;
            serde_json::from_str::<serde_json::Value>(&text)
                .map_err(|err| RemoteError::new(format!("invalid JSON: {err}"), false))?;
            string_value(text)
        }
    };
    Ok(value)
}

pub fn to_bulk(column: &Column, cell: &Cell) -> Result<Value, RemoteError> {
    if matches!(cell, Cell::Null) {
        if column.semantic_type == SemanticType::Timestamp || column.data_type.is_timestamp() {
            return Err(RemoteError::new(
                format!("timestamp column {} cannot be null", column.name),
                false,
            ));
        }
        return Ok(Value::Null);
    }
    let value = match column.data_type {
        ColumnDataType::Boolean => Value::Boolean(expect_bool(cell)?),
        ColumnDataType::Int8 => Value::Int8(expect_int(cell, "int8")?),
        ColumnDataType::Int16 => Value::Int16(expect_int(cell, "int16")?),
        ColumnDataType::Int32 => Value::Int32(expect_int(cell, "int32")?),
        ColumnDataType::Int64 => Value::Int64(expect_int(cell, "int64")?),
        ColumnDataType::Uint8 => Value::Uint8(expect_int(cell, "uint8")?),
        ColumnDataType::Uint16 => Value::Uint16(expect_int(cell, "uint16")?),
        ColumnDataType::Uint32 => Value::Uint32(expect_int(cell, "uint32")?),
        ColumnDataType::Uint64 => Value::Uint64(expect_int(cell, "uint64")?),
        ColumnDataType::Float32 => Value::Float32(expect_f32(cell)?),
        ColumnDataType::Float64 => Value::Float64(expect_f64(cell)?),
        ColumnDataType::Binary => Value::Binary(expect_bytes(cell)?),
        ColumnDataType::String => Value::String(expect_str(cell)?.to_owned()),
        ColumnDataType::Date => Value::Date(expect_date(cell)?),
        ColumnDataType::TimestampSecond => {
            Value::TimestampSecond(expect_epoch(cell, 1_000_000_000)?)
        }
        ColumnDataType::TimestampMillisecond => {
            Value::TimestampMillisecond(expect_epoch(cell, 1_000_000)?)
        }
        ColumnDataType::TimestampMicrosecond => {
            Value::TimestampMicrosecond(expect_epoch(cell, 1_000)?)
        }
        ColumnDataType::TimestampNanosecond => Value::TimestampNanosecond(expect_epoch(cell, 1)?),
        ColumnDataType::TimeSecond => Value::TimeSecond(expect_int(cell, "time_second")?),
        ColumnDataType::TimeMillisecond => {
            Value::TimeMillisecond(expect_int(cell, "time_millisecond")?)
        }
        ColumnDataType::TimeMicrosecond => {
            Value::TimeMicrosecond(expect_int(cell, "time_microsecond")?)
        }
        ColumnDataType::TimeNanosecond => {
            Value::TimeNanosecond(expect_int(cell, "time_nanosecond")?)
        }
        ColumnDataType::Decimal128 => {
            Value::Decimal128(expect_decimal(cell, column.scale.expect("scale"))?)
        }
        ColumnDataType::Json => {
            let text = expect_json_text(cell)?;
            serde_json::from_str::<serde_json::Value>(&text)
                .map_err(|err| RemoteError::new(format!("invalid JSON: {err}"), false))?;
            Value::Json(text)
        }
    };
    Ok(value)
}

pub fn format_proto(value: &greptimedb_ingester::api::v1::Value) -> String {
    match &value.value_data {
        None => "null".to_owned(),
        Some(ValueData::BoolValue(v)) => format!("bool:{v}"),
        Some(ValueData::I8Value(v)) => format!("i8:{v}"),
        Some(ValueData::I16Value(v)) => format!("i16:{v}"),
        Some(ValueData::I32Value(v)) => format!("i32:{v}"),
        Some(ValueData::I64Value(v)) => format!("i64:{v}"),
        Some(ValueData::U8Value(v)) => format!("u8:{v}"),
        Some(ValueData::U16Value(v)) => format!("u16:{v}"),
        Some(ValueData::U32Value(v)) => format!("u32:{v}"),
        Some(ValueData::U64Value(v)) => format!("u64:{v}"),
        Some(ValueData::F32Value(v)) => format!("f32:{v}"),
        Some(ValueData::F64Value(v)) => format!("f64:{v}"),
        Some(ValueData::BinaryValue(v)) => format!("binary:{}", hex(v)),
        Some(ValueData::StringValue(v)) => format!("string:{v}"),
        Some(ValueData::DateValue(v)) => format!("date:{v}"),
        Some(ValueData::TimestampSecondValue(v)) => format!("timestamp_second:{v}"),
        Some(ValueData::TimestampMillisecondValue(v)) => format!("timestamp_millisecond:{v}"),
        Some(ValueData::TimestampMicrosecondValue(v)) => format!("timestamp_microsecond:{v}"),
        Some(ValueData::TimestampNanosecondValue(v)) => format!("timestamp_nanosecond:{v}"),
        Some(ValueData::TimeSecondValue(v)) => format!("time_second:{v}"),
        Some(ValueData::TimeMillisecondValue(v)) => format!("time_millisecond:{v}"),
        Some(ValueData::TimeMicrosecondValue(v)) => format!("time_microsecond:{v}"),
        Some(ValueData::TimeNanosecondValue(v)) => format!("time_nanosecond:{v}"),
        Some(ValueData::Decimal128Value(v)) => {
            let coefficient = ((v.hi as i128) << 64) | (v.lo as u64 as i128);
            format!("decimal128:{coefficient}")
        }
        Some(ValueData::JsonValue(_)) => "json_value".to_owned(),
        Some(other) => format!("unsupported:{other:?}"),
    }
}

pub fn format_bulk(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::Boolean(v) => format!("bool:{v}"),
        Value::Int8(v) => format!("i8:{v}"),
        Value::Int16(v) => format!("i16:{v}"),
        Value::Int32(v) => format!("i32:{v}"),
        Value::Int64(v) => format!("i64:{v}"),
        Value::Uint8(v) => format!("u8:{v}"),
        Value::Uint16(v) => format!("u16:{v}"),
        Value::Uint32(v) => format!("u32:{v}"),
        Value::Uint64(v) => format!("u64:{v}"),
        Value::Float32(v) => format!("f32:{v}"),
        Value::Float64(v) => format!("f64:{v}"),
        Value::Binary(v) => format!("binary:{}", hex(v)),
        Value::String(v) => format!("string:{v}"),
        Value::Date(v) => format!("date:{v}"),
        Value::Datetime(v) => format!("datetime:{v}"),
        Value::TimestampSecond(v) => format!("timestamp_second:{v}"),
        Value::TimestampMillisecond(v) => format!("timestamp_millisecond:{v}"),
        Value::TimestampMicrosecond(v) => format!("timestamp_microsecond:{v}"),
        Value::TimestampNanosecond(v) => format!("timestamp_nanosecond:{v}"),
        Value::TimeSecond(v) => format!("time_second:{v}"),
        Value::TimeMillisecond(v) => format!("time_millisecond:{v}"),
        Value::TimeMicrosecond(v) => format!("time_microsecond:{v}"),
        Value::TimeNanosecond(v) => format!("time_nanosecond:{v}"),
        Value::Decimal128(v) => format!("decimal128:{v}"),
        Value::Json(v) => format!("json:{v}"),
    }
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    out
}

fn expect_bool(cell: &Cell) -> Result<bool, RemoteError> {
    match cell {
        Cell::Bool(value) => Ok(*value),
        other => Err(type_mismatch("boolean", other)),
    }
}

fn expect_int<T>(cell: &Cell, label: &str) -> Result<T, RemoteError>
where
    T: TryFrom<i128>,
{
    match cell {
        Cell::Int(value) => fit_int(*value, label),
        other => Err(type_mismatch(label, other)),
    }
}

fn expect_f64(cell: &Cell) -> Result<f64, RemoteError> {
    match cell {
        Cell::Float(value) => Ok(*value),
        Cell::Int(value) => {
            let value = *value as f64;
            if value.is_finite() {
                Ok(value)
            } else {
                Err(RemoteError::new("float64 value is out of range", false))
            }
        }
        other => Err(type_mismatch("float64", other)),
    }
}

fn expect_f32(cell: &Cell) -> Result<f32, RemoteError> {
    let value = expect_f64(cell).map_err(|err| {
        if err.message.contains("expected float64") {
            type_mismatch("float32", cell)
        } else {
            RemoteError::new(err.message.replace("float64", "float32"), false)
        }
    })?;
    let narrowed = value as f32;
    if narrowed.is_finite() {
        Ok(narrowed)
    } else {
        Err(RemoteError::new("float32 value is out of range", false))
    }
}

fn expect_bytes(cell: &Cell) -> Result<Vec<u8>, RemoteError> {
    match cell {
        Cell::Bytes(value) => Ok(value.clone()),
        other => Err(type_mismatch("binary", other)),
    }
}

fn expect_str(cell: &Cell) -> Result<&str, RemoteError> {
    match cell {
        Cell::Str(value) => Ok(value),
        other => Err(type_mismatch("string", other)),
    }
}

fn expect_json_text(cell: &Cell) -> Result<String, RemoteError> {
    match cell {
        Cell::Str(value) | Cell::Json(value) => Ok(value.clone()),
        other => Err(type_mismatch("json", other)),
    }
}

fn expect_date(cell: &Cell) -> Result<i32, RemoteError> {
    match cell {
        Cell::Int(value) => fit_int(*value, "date"),
        Cell::DateDays(value) => Ok(*value),
        other => Err(type_mismatch("date", other)),
    }
}

/// Integer cells are already in the column unit. Datetime cells are absolute instants.
fn expect_epoch(cell: &Cell, unit_nanos: i64) -> Result<i64, RemoteError> {
    match cell {
        Cell::Int(value) => fit_int(*value, "timestamp"),
        Cell::Timestamp { secs, subsec_nanos } => scale_timestamp(*secs, *subsec_nanos, unit_nanos),
        other => Err(type_mismatch("timestamp", other)),
    }
}

fn scale_timestamp(secs: i64, subsec_nanos: u32, unit_nanos: i64) -> Result<i64, RemoteError> {
    let total = secs
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(i64::from(subsec_nanos)))
        .ok_or_else(|| RemoteError::new("timestamp value is out of range", false))?;
    total
        .checked_div(unit_nanos)
        .ok_or_else(|| RemoteError::new("timestamp value is out of range", false))
}

fn expect_decimal(cell: &Cell, scale: i8) -> Result<i128, RemoteError> {
    match cell {
        Cell::Int(value) => Ok(*value),
        Cell::Decimal(parts) => scale_decimal(parts, scale),
        other => Err(type_mismatch("decimal128", other)),
    }
}

fn scale_decimal(parts: &DecimalParts, scale: i8) -> Result<i128, RemoteError> {
    let coefficient: i128 = parts
        .digits
        .parse()
        .map_err(|_| RemoteError::new("decimal128 value is out of range", false))?;
    let shift = parts.exponent + i32::from(scale);
    let scaled = if shift >= 0 {
        let factor = 10i128
            .checked_pow(shift as u32)
            .ok_or_else(|| RemoteError::new("decimal128 value is out of range", false))?;
        coefficient
            .checked_mul(factor)
            .ok_or_else(|| RemoteError::new("decimal128 value is out of range", false))?
    } else {
        return Err(RemoteError::new(
            format!("decimal has more fractional digits than scale {scale}"),
            false,
        ));
    };
    if parts.negative {
        scaled
            .checked_neg()
            .ok_or_else(|| RemoteError::new("decimal128 value is out of range", false))
    } else {
        Ok(scaled)
    }
}

#[pyfunction]
#[pyo3(signature = (columns, row, *, bulk = false))]
pub fn describe_row(
    columns: Vec<Column>,
    row: &Bound<'_, PyAny>,
    bulk: bool,
) -> PyResult<Vec<String>> {
    let input = parse_row(row)?;
    let cells = align(&columns, &input)?;
    let mut described = Vec::with_capacity(columns.len());
    for (column, cell) in columns.iter().zip(cells.iter()) {
        let text = if bulk {
            format_bulk(&to_bulk(column, cell)?)
        } else {
            format_proto(&to_proto(column, cell)?)
        };
        described.push(text);
    }
    Ok(described)
}
