
#[derive(thiserror::Error, Debug)]
pub enum POSError {
    #[error("ZODB.POSException.POSKeyError")]
    Key([u8;8]),
}
