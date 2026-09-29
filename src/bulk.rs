use std::sync::Mutex;
use std::thread;

use pyo3::prelude::*;
use tokio::sync::{mpsc, oneshot};

use greptimedb_ingester::api::v1::auth_header::AuthScheme;
use greptimedb_ingester::api::v1::Basic;
use greptimedb_ingester::client::Client as IngesterClient;
use greptimedb_ingester::{BulkInserter, BulkStreamWriter, BulkWriteOptions, Row};

use crate::cell::Cell;
use crate::convert::{align, parse_row, to_bulk};
use crate::error::{raise, RemoteError};
use crate::types::{BulkResponse, Column, WriteOptions};

enum Command {
    Write {
        rows: Vec<Row>,
        wait: bool,
        reply: oneshot::Sender<Result<WriteOutcome, RemoteError>>,
    },
    Wait {
        request_id: i64,
        reply: oneshot::Sender<Result<BulkResponse, RemoteError>>,
    },
    WaitAll {
        reply: oneshot::Sender<Result<Vec<BulkResponse>, RemoteError>>,
    },
    Flush {
        reply: oneshot::Sender<Result<Vec<BulkResponse>, RemoteError>>,
    },
    Finish {
        with_responses: bool,
        reply: oneshot::Sender<Result<Vec<BulkResponse>, RemoteError>>,
    },
}

enum WriteOutcome {
    Waited(BulkResponse),
    Ids(Vec<i64>),
}

#[pyclass]
pub struct Rows {
    columns: Vec<Column>,
    rows: Vec<Vec<Cell>>,
}

#[pymethods]
impl Rows {
    fn add_row(&mut self, row: &Bound<'_, PyAny>) -> PyResult<()> {
        let input = parse_row(row)?;
        self.rows.push(align(&self.columns, &input)?);
        Ok(())
    }

    /// Convert a batch of rows in one call. The GIL stays held while the
    /// iterable is consumed, so this avoids one Python-to-Rust crossing per row.
    fn add_rows(&mut self, rows: &Bound<'_, PyAny>) -> PyResult<usize> {
        let mut parsed = Vec::new();
        for row in rows.try_iter()? {
            let input = parse_row(&row?)?;
            parsed.push(align(&self.columns, &input)?);
        }
        let added = parsed.len();
        self.rows.append(&mut parsed);
        Ok(added)
    }

    fn __len__(&self) -> usize {
        self.rows.len()
    }
}

#[pyclass]
pub struct BulkWriter {
    columns: Vec<Column>,
    table: String,
    tx: Mutex<Option<mpsc::Sender<Command>>>,
}

impl BulkWriter {
    pub fn start(
        py: Python<'_>,
        client: IngesterClient,
        database: String,
        username: Option<String>,
        password: Option<String>,
        table: String,
        columns: Vec<Column>,
        options: WriteOptions,
    ) -> PyResult<Self> {
        let (ready_tx, ready_rx) = oneshot::channel();
        let (tx, rx) = mpsc::channel(16);
        let thread_columns = columns.clone();
        let thread_table = table.clone();
        thread::Builder::new()
            .name("greptimedb-bulk".to_owned())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("bulk writer runtime");
                runtime.block_on(actor(
                    client,
                    database,
                    username,
                    password,
                    thread_table,
                    thread_columns,
                    options,
                    ready_tx,
                    rx,
                ));
            })
            .map_err(|err| raise(format!("failed to start bulk writer thread: {err}"), false))?;

        py.allow_threads(|| ready_rx.blocking_recv())
            .map_err(|_| raise("bulk writer thread exited before it was ready", true))??;

        Ok(Self {
            columns,
            table,
            tx: Mutex::new(Some(tx)),
        })
    }
}

#[pymethods]
impl BulkWriter {
    #[getter]
    fn table_name(&self) -> &str {
        &self.table
    }

    #[getter]
    fn columns(&self) -> Vec<Column> {
        self.columns.clone()
    }

    fn alloc_rows(&self, py: Python<'_>, capacity: usize) -> PyResult<Py<Rows>> {
        Py::new(
            py,
            Rows {
                columns: self.columns.clone(),
                rows: Vec::with_capacity(capacity),
            },
        )
    }

    fn write(&self, py: Python<'_>, rows: PyRefMut<'_, Rows>) -> PyResult<BulkResponse> {
        match self.write_inner(py, rows, true)? {
            WriteOutcome::Waited(response) => Ok(response),
            WriteOutcome::Ids(_) => Err(raise("internal error: write did not wait", false)),
        }
    }

    fn write_async(&self, py: Python<'_>, rows: PyRefMut<'_, Rows>) -> PyResult<Vec<i64>> {
        match self.write_inner(py, rows, false)? {
            WriteOutcome::Ids(ids) => Ok(ids),
            WriteOutcome::Waited(_) => Err(raise("internal error: async write waited", false)),
        }
    }

    fn wait(&self, py: Python<'_>, request_id: i64) -> PyResult<BulkResponse> {
        self.call(py, |reply| Command::Wait { request_id, reply })
    }

    fn wait_all(&self, py: Python<'_>) -> PyResult<Vec<BulkResponse>> {
        self.call(py, |reply| Command::WaitAll { reply })
    }

    fn flush(&self, py: Python<'_>) -> PyResult<Vec<BulkResponse>> {
        self.call(py, |reply| Command::Flush { reply })
    }

    fn finish(&self, py: Python<'_>) -> PyResult<()> {
        self.finish_inner(py, false).map(|_| ())
    }

    fn finish_with_responses(&self, py: Python<'_>) -> PyResult<Vec<BulkResponse>> {
        self.finish_inner(py, true)
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &mut self,
        py: Python<'_>,
        exc_type: Option<&Bound<'_, PyAny>>,
        _exc: Option<&Bound<'_, PyAny>>,
        _tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        if exc_type.is_none() {
            self.finish(py)?;
        } else if let Err(err) = self.finish(py) {
            err.write_unraisable(py, None);
        }
        Ok(false)
    }

    fn __repr__(&self) -> String {
        format!("BulkWriter(table={:?})", self.table)
    }
}

impl BulkWriter {
    fn write_inner(
        &self,
        py: Python<'_>,
        mut rows: PyRefMut<'_, Rows>,
        wait: bool,
    ) -> PyResult<WriteOutcome> {
        if rows.columns != self.columns {
            return Err(raise(
                "rows were allocated by a different bulk writer",
                false,
            ));
        }
        if rows.rows.is_empty() {
            return Err(raise("cannot write an empty row batch", false));
        }
        let mut batch = Vec::with_capacity(rows.rows.len());
        for row in &rows.rows {
            let mut values = Vec::with_capacity(self.columns.len());
            for (column, cell) in self.columns.iter().zip(row.iter()) {
                values.push(to_bulk(column, cell)?);
            }
            batch.push(Row::from_values(values));
        }
        rows.rows.clear();
        self.call(py, |reply| Command::Write {
            rows: batch,
            wait,
            reply,
        })
    }

    fn finish_inner(&self, py: Python<'_>, with_responses: bool) -> PyResult<Vec<BulkResponse>> {
        let tx = self
            .tx
            .lock()
            .expect("bulk writer lock")
            .take()
            .ok_or_else(|| raise("bulk writer is already finished", false))?;
        let (reply_tx, reply_rx) = oneshot::channel();
        py.allow_threads(|| {
            tx.blocking_send(Command::Finish {
                with_responses,
                reply: reply_tx,
            })
            .map_err(|_| RemoteError::new("bulk writer thread is gone", true))?;
            reply_rx
                .blocking_recv()
                .map_err(|_| RemoteError::new("bulk writer thread dropped the response", true))?
        })
        .map_err(Into::into)
    }

    fn call<T>(
        &self,
        py: Python<'_>,
        make: impl FnOnce(oneshot::Sender<Result<T, RemoteError>>) -> Command,
    ) -> PyResult<T>
    where
        T: Send,
    {
        let tx = self
            .tx
            .lock()
            .expect("bulk writer lock")
            .clone()
            .ok_or_else(|| raise("bulk writer is already finished", false))?;
        let (reply_tx, reply_rx) = oneshot::channel();
        let command = make(reply_tx);
        py.allow_threads(|| {
            tx.blocking_send(command)
                .map_err(|_| RemoteError::new("bulk writer thread is gone", true))?;
            reply_rx
                .blocking_recv()
                .map_err(|_| RemoteError::new("bulk writer thread dropped the response", true))?
        })
        .map_err(Into::into)
    }
}

impl Drop for BulkWriter {
    fn drop(&mut self) {
        let tx = self.tx.lock().ok().and_then(|mut guard| guard.take());
        let Some(tx) = tx else {
            return;
        };
        let (reply_tx, reply_rx) = oneshot::channel();
        if tx
            .blocking_send(Command::Finish {
                with_responses: false,
                reply: reply_tx,
            })
            .is_ok()
        {
            let _ = reply_rx.blocking_recv();
        }
    }
}

async fn actor(
    client: IngesterClient,
    database: String,
    username: Option<String>,
    password: Option<String>,
    table: String,
    columns: Vec<Column>,
    options: WriteOptions,
    ready: oneshot::Sender<Result<(), RemoteError>>,
    mut rx: mpsc::Receiver<Command>,
) {
    let mut inserter = BulkInserter::new(client, &database);
    if let (Some(username), Some(password)) = (username, password) {
        inserter.set_auth(AuthScheme::Basic(Basic { username, password }));
    }
    let schema = match table_schema(&table, &columns) {
        Ok(schema) => schema,
        Err(err) => {
            let _ = ready.send(Err(err));
            return;
        }
    };
    let write_options = BulkWriteOptions::default()
        .with_compression(options.compression.into_ingester())
        .with_timeout(options.timeout)
        .with_parallelism(options.parallelism);
    let writer = inserter
        .create_bulk_stream_writer(&schema, Some(write_options))
        .await;
    let mut writer = match writer {
        Ok(writer) => {
            let _ = ready.send(Ok(()));
            writer
        }
        Err(err) => {
            let _ = ready.send(Err(RemoteError::from(err)));
            return;
        }
    };

    while let Some(command) = rx.recv().await {
        match command {
            Command::Write { rows, wait, reply } => {
                let result = write_rows(&mut writer, rows, wait).await;
                let _ = reply.send(result);
            }
            Command::Wait { request_id, reply } => {
                let result = writer
                    .wait_for_response(request_id)
                    .await
                    .map(response_from)
                    .map_err(RemoteError::from);
                let _ = reply.send(result);
            }
            Command::WaitAll { reply } => {
                let result = writer
                    .wait_for_all_pending()
                    .await
                    .map(|responses| responses.into_iter().map(response_from).collect())
                    .map_err(RemoteError::from);
                let _ = reply.send(result);
            }
            Command::Flush { reply } => {
                let responses = writer
                    .flush_completed_responses()
                    .into_iter()
                    .map(response_from)
                    .collect();
                let _ = reply.send(Ok(responses));
            }
            Command::Finish {
                with_responses,
                reply,
            } => {
                let result = if with_responses {
                    writer
                        .finish_with_responses()
                        .await
                        .map(|responses| responses.into_iter().map(response_from).collect())
                        .map_err(RemoteError::from)
                } else {
                    writer
                        .finish()
                        .await
                        .map(|_| Vec::new())
                        .map_err(RemoteError::from)
                };
                let _ = reply.send(result);
                break;
            }
        }
    }
}

fn table_schema(
    table: &str,
    columns: &[Column],
) -> Result<greptimedb_ingester::TableSchema, RemoteError> {
    let mut schema = greptimedb_ingester::TableSchema::builder()
        .name(table)
        .build()
        .map_err(|err| RemoteError::new(err.to_string(), false))?;
    for column in columns {
        let dtype = column.data_type.proto();
        schema = match column.semantic_type {
            crate::types::SemanticType::Tag => schema.add_tag(column.name.clone(), dtype),
            crate::types::SemanticType::Timestamp => {
                schema.add_timestamp(column.name.clone(), dtype)
            }
            crate::types::SemanticType::Field
                if column.data_type == crate::types::ColumnDataType::Decimal128 =>
            {
                schema.add_decimal128_field(
                    column.name.clone(),
                    column.precision.expect("precision"),
                    column.scale.expect("scale"),
                )
            }
            crate::types::SemanticType::Field => schema.add_field(column.name.clone(), dtype),
        };
    }
    Ok(schema)
}

async fn write_rows(
    writer: &mut BulkStreamWriter,
    rows: Vec<Row>,
    wait: bool,
) -> Result<WriteOutcome, RemoteError> {
    let mut buffer = writer
        .alloc_rows_buffer(rows.len())
        .map_err(RemoteError::from)?;
    for row in rows {
        buffer.add_row(row).map_err(RemoteError::from)?;
    }
    if wait {
        let response = writer.write_rows(buffer).await.map_err(RemoteError::from)?;
        Ok(WriteOutcome::Waited(response_from(response)))
    } else {
        let ids = writer
            .write_rows_async(buffer)
            .await
            .map_err(RemoteError::from)?;
        Ok(WriteOutcome::Ids(ids))
    }
}

fn response_from(response: greptimedb_ingester::flight::do_put::DoPutResponse) -> BulkResponse {
    BulkResponse {
        request_id: response.request_id(),
        affected_rows: response.affected_rows() as u64,
    }
}
