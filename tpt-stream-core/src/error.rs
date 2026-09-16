use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("CSV error: {0}")]
    Csv(#[from] csv::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid schema: {0}")]
    Schema(String),
    #[error("columnar format error: {0}")]
    Columnar(#[from] tpt_stream_columnar::format::FormatError),
    #[error("pipeline configuration: {0}")]
    Config(String),
    #[error("database error: {0}")]
    Database(String),
    #[error("cloud storage error: {0}")]
    Cloud(String),
    #[error("data quality: {0}")]
    DataQuality(String),
    #[error("{0}")]
    Other(String),
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PipelineStats {
    pub batches: u64,
    pub rows: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    /// Wall-clock time of the last `execute()` run.
    pub elapsed: std::time::Duration,
}
