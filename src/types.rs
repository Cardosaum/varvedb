use heed3::byteorder::BigEndian;
use heed3::types::Bytes;
use heed3::types::U64;
use heed3::EncryptedDatabase;

pub type SequenceKey = U64<BigEndian>;
pub type EventsDb = EncryptedDatabase<SequenceKey, Bytes>;