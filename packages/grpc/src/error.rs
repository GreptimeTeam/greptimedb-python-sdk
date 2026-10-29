use pyo3::prelude::*;

#[derive(Debug)]
pub struct RemoteError {
    pub message: String,
    pub retriable: bool,
}

impl RemoteError {
    pub fn new(message: impl Into<String>, retriable: bool) -> Self {
        Self {
            message: message.into(),
            retriable,
        }
    }
}

impl From<greptimedb_ingester::Error> for RemoteError {
    fn from(err: greptimedb_ingester::Error) -> Self {
        let retriable = err.is_retriable();
        Self {
            message: err.to_string(),
            retriable,
        }
    }
}

pub fn raise(message: impl Into<String>, retriable: bool) -> PyErr {
    let message = message.into();
    Python::with_gil(|py| {
        match py
            .import("greptimedb_ingester._errors")
            .and_then(|module| module.getattr("GreptimeError"))
            .and_then(|class| class.call1((message, retriable)))
        {
            Ok(instance) => PyErr::from_value(instance),
            Err(error) => error,
        }
    })
}

impl From<RemoteError> for PyErr {
    fn from(err: RemoteError) -> Self {
        raise(&err.message, err.retriable)
    }
}
