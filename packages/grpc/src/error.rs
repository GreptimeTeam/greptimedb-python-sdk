use pyo3::exceptions::PyException;
use pyo3::prelude::*;

pyo3::create_exception!(
    greptimedb_ingester,
    GreptimeError,
    PyException,
    "Error returned by the GreptimeDB ingester."
);

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
        let err = GreptimeError::new_err(message.clone());
        let instance = err.value(py);
        if instance.setattr("retriable", retriable).is_err() {
            return GreptimeError::new_err(format!("retriable={retriable}: {message}"));
        }
        err
    })
}

impl From<RemoteError> for PyErr {
    fn from(err: RemoteError) -> Self {
        raise(&err.message, err.retriable)
    }
}
