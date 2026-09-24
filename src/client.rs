use std::time::Duration;

use pyo3::prelude::*;

use greptimedb_ingester::api::v1::auth_header::AuthScheme;
use greptimedb_ingester::api::v1::Basic;
use greptimedb_ingester::client::Client as IngesterClient;
use greptimedb_ingester::database::Database;
use greptimedb_ingester::{ChannelConfig, ChannelManager, ClientTlsOption};

use crate::bulk::BulkWriter;
use crate::convert::{align, build_delete, build_insert, parse_row};
use crate::error::{raise, RemoteError};
use crate::runtime;
use crate::types::{parse_grpc_compression, Column, WriteOptions};

#[pyclass]
#[derive(Clone)]
pub struct Client {
    inner: IngesterClient,
    database: String,
    username: Option<String>,
    password: Option<String>,
}

#[pymethods]
impl Client {
    #[new]
    #[pyo3(signature = (
        urls,
        database = "public",
        username = None,
        password = None,
        timeout_secs = None,
        connect_timeout_secs = None,
        send_compression = None,
        accept_compression = None,
        tls_server_ca = None,
        tls_client_cert = None,
        tls_client_key = None,
    ))]
    fn new(
        urls: Vec<String>,
        database: &str,
        username: Option<String>,
        password: Option<String>,
        timeout_secs: Option<f64>,
        connect_timeout_secs: Option<f64>,
        send_compression: Option<String>,
        accept_compression: Option<String>,
        tls_server_ca: Option<String>,
        tls_client_cert: Option<String>,
        tls_client_key: Option<String>,
    ) -> PyResult<Self> {
        if urls.is_empty() || urls.iter().any(|url| url.is_empty()) {
            return Err(raise("urls must contain at least one gRPC endpoint", false));
        }
        if database.is_empty() {
            return Err(raise("database must not be empty", false));
        }
        let (username, password) = match (username, password) {
            (None, None) => (None, None),
            (Some(user), Some(pass)) => (Some(user), Some(pass)),
            _ => return Err(raise("username and password must both be set", false)),
        };
        let tls = match (tls_server_ca, tls_client_cert, tls_client_key) {
            (None, None, None) => None,
            (Some(ca), Some(cert), Some(key)) => Some((ca, cert, key)),
            _ => {
                return Err(raise(
                    "tls_server_ca, tls_client_cert, and tls_client_key must all be set",
                    false,
                ))
            }
        };
        let timeout = optional_duration(timeout_secs, "timeout_secs")?;
        let connect_timeout = optional_duration(connect_timeout_secs, "connect_timeout_secs")?;
        let send = send_compression
            .as_deref()
            .map(parse_grpc_compression)
            .transpose()?;
        let accept = accept_compression
            .as_deref()
            .map(parse_grpc_compression)
            .transpose()?;

        let inner = build_ingester_client(urls, timeout, connect_timeout, send, accept, tls)?;
        Ok(Self {
            inner,
            database: database.to_owned(),
            username,
            password,
        })
    }

    #[getter]
    fn database(&self) -> &str {
        &self.database
    }

    fn health_check(&self, py: Python<'_>) -> PyResult<()> {
        let client = self.inner.clone();
        py.allow_threads(|| runtime().block_on(client.health_check()))
            .map_err(RemoteError::from)?;
        Ok(())
    }

    #[pyo3(signature = (table, columns, rows, hints = None))]
    fn insert(
        &self,
        py: Python<'_>,
        table: String,
        columns: Vec<Column>,
        rows: &Bound<'_, PyAny>,
        hints: Option<Vec<(String, String)>>,
    ) -> PyResult<u32> {
        if table.is_empty() {
            return Err(raise("table name must not be empty", false));
        }
        let aligned = align_rows(&columns, rows)?;
        let hints = hints.unwrap_or_default();
        self.execute(py, move |database| async move {
            let request = build_insert(&table, &columns, &aligned)?;
            if hints.is_empty() {
                database.insert(request).await.map_err(RemoteError::from)
            } else {
                let refs: Vec<(&str, &str)> = hints
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_str()))
                    .collect();
                database
                    .insert_with_hints(request, &refs)
                    .await
                    .map_err(RemoteError::from)
            }
        })
    }

    #[pyo3(signature = (table, columns, rows))]
    fn delete(
        &self,
        py: Python<'_>,
        table: String,
        columns: Vec<Column>,
        rows: &Bound<'_, PyAny>,
    ) -> PyResult<u32> {
        if table.is_empty() {
            return Err(raise("table name must not be empty", false));
        }
        let aligned = align_rows(&columns, rows)?;
        self.execute(py, move |database| async move {
            let request = build_delete(&table, &columns, &aligned)?;
            database.delete(request).await.map_err(RemoteError::from)
        })
    }

    #[pyo3(signature = (table, columns, options = None, auto_create_table = false))]
    fn bulk_writer(
        &self,
        py: Python<'_>,
        table: String,
        columns: Vec<Column>,
        options: Option<WriteOptions>,
        auto_create_table: bool,
    ) -> PyResult<BulkWriter> {
        if table.is_empty() {
            return Err(raise("table name must not be empty", false));
        }
        if columns.is_empty() {
            return Err(raise("bulk writer requires at least one column", false));
        }
        BulkWriter::start(
            py,
            self.inner.clone(),
            self.database.clone(),
            self.username.clone(),
            self.password.clone(),
            table,
            columns,
            options.unwrap_or_default(),
            auto_create_table,
        )
    }

    fn __repr__(&self) -> String {
        format!("Client(database={:?})", self.database)
    }
}

impl Client {
    fn execute<F, Fut, T>(&self, py: Python<'_>, op: F) -> PyResult<T>
    where
        F: FnOnce(Database) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T, RemoteError>> + Send,
        T: Send,
    {
        let database = self.open_database();
        py.allow_threads(|| runtime().block_on(op(database)))
            .map_err(Into::into)
    }

    fn open_database(&self) -> Database {
        let mut database = Database::new_with_dbname(self.database.clone(), self.inner.clone());
        if let (Some(username), Some(password)) = (&self.username, &self.password) {
            database.set_auth(AuthScheme::Basic(Basic {
                username: username.clone(),
                password: password.clone(),
            }));
        }
        database
    }
}

fn align_rows(
    columns: &[Column],
    rows: &Bound<'_, PyAny>,
) -> PyResult<Vec<Vec<crate::cell::Cell>>> {
    if columns.is_empty() {
        return Err(raise("insert requires at least one column", false));
    }
    let mut aligned = Vec::new();
    for row in rows.try_iter()? {
        let input = parse_row(&row?)?;
        aligned.push(align(columns, &input)?);
    }
    if aligned.is_empty() {
        return Err(raise("cannot insert an empty row batch", false));
    }
    Ok(aligned)
}

fn optional_duration(value: Option<f64>, name: &str) -> PyResult<Option<Duration>> {
    match value {
        None => Ok(None),
        Some(secs) if secs >= 0.0 => Ok(Some(Duration::from_secs_f64(secs))),
        Some(_) => Err(raise(format!("{name} must be >= 0"), false)),
    }
}

fn build_ingester_client(
    urls: Vec<String>,
    timeout: Option<Duration>,
    connect_timeout: Option<Duration>,
    send_compression: Option<greptimedb_ingester::GrpcCompression>,
    accept_compression: Option<greptimedb_ingester::GrpcCompression>,
    tls: Option<(String, String, String)>,
) -> PyResult<IngesterClient> {
    let mut config = ChannelConfig::new();
    if let Some(timeout) = timeout {
        config = config.timeout(timeout);
    }
    if let Some(timeout) = connect_timeout {
        config = config.connect_timeout(timeout);
    }
    if let Some(compression) = send_compression {
        config = config.with_send_compression(compression);
    }
    if let Some(compression) = accept_compression {
        config = config.with_accept_compression(compression);
    }
    let client = if let Some((ca, cert, key)) = tls {
        config = config.client_tls_config(ClientTlsOption {
            server_ca_cert_path: ca,
            client_cert_path: cert,
            client_key_path: key,
        });
        let manager = ChannelManager::with_tls_config(config).map_err(RemoteError::from)?;
        IngesterClient::with_manager_and_urls(manager, urls)
    } else {
        let manager = ChannelManager::with_config(config);
        IngesterClient::with_manager_and_urls(manager, urls)
    };
    Ok(client)
}
