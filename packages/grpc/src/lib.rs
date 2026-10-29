mod bulk;
mod cell;
mod client;
mod convert;
mod error;
mod types;

use std::sync::OnceLock;

use pyo3::prelude::*;

pub fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("greptimedb-ingester")
            .build()
            .expect("failed to start the greptimedb ingester runtime")
    })
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("GreptimeError", m.py().get_type::<error::GreptimeError>())?;
    m.add_class::<types::SemanticType>()?;
    m.add_class::<types::ColumnDataType>()?;
    m.add_class::<types::Column>()?;
    m.add_class::<types::WriteOptions>()?;
    m.add_class::<types::BulkResponse>()?;
    m.add_class::<client::Client>()?;
    m.add_class::<bulk::Rows>()?;
    m.add_class::<bulk::BulkWriter>()?;
    m.add_function(wrap_pyfunction!(convert::describe_row, m)?)?;
    Ok(())
}
