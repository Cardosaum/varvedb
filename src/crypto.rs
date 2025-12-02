use crate::storage::Storage;
use aes_gcm::{
    Aes256Gcm, Key, Nonce,
    aead::{Aead, KeyInit, Payload},
};
use rand::RngCore;

/// Manages encryption keys for streams.
///
/// Keys are stored in the `keystore` bucket, encrypted at rest (conceptually, though currently stored as raw bytes in this implementation).
///
/// # Example
///
/// ```rust
/// use varvedb::storage::{Storage, StorageConfig};
/// use varvedb::crypto::KeyManager;
/// use tempfile::tempdir;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let dir = tempdir()?;
/// let config = StorageConfig { path: dir.path().to_path_buf(), ..Default::default() };
/// let storage = Storage::open(config)?;
/// let key_manager = KeyManager::new(storage);
///
/// let key = key_manager.get_or_create_key(123)?;
/// println!("Key: {:?}", key);
/// # Ok(())
/// # }
/// ```
pub struct KeyManager {
    storage: Storage,
}

impl KeyManager {
    /// Creates a new `KeyManager` instance.
    pub fn new(storage: Storage) -> Self {
        Self { storage }
    }

    pub fn get_or_create_key(&self, stream_id: u128) -> crate::error::Result<[u8; 32]> {
        let mut txn = self.storage.env.write_txn()?;

        match self.storage.keystore.get(&txn, &stream_id)? {
            Some(key_bytes) => {
                let mut key = [0u8; 32];
                key.copy_from_slice(key_bytes);
                txn.commit()?;
                Ok(key)
            }
            None => {
                let mut key = [0u8; 32];
                rand::thread_rng().fill_bytes(&mut key);
                self.storage.keystore.put(&mut txn, &stream_id, &key)?;
                txn.commit()?;
                Ok(key)
            }
        }
    }

    pub fn delete_key(&self, stream_id: u128) -> crate::error::Result<()> {
        let mut txn = self.storage.env.write_txn()?;
        self.storage.keystore.delete(&mut txn, &stream_id)?;
        txn.commit()?;
        Ok(())
    }
}

/// Encrypts plaintext using AES-256-GCM.
///
/// Returns a vector containing the 12-byte nonce followed by the ciphertext.
pub fn encrypt(key: &[u8; 32], plaintext: &[u8], aad: &[u8]) -> crate::error::Result<Vec<u8>> {
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    let mut nonce_bytes = [0u8; 12];
    rand::thread_rng().fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);

    let payload = Payload {
        msg: plaintext,
        aad,
    };

    let mut ciphertext = cipher
        .encrypt(nonce, payload)
        .map_err(|e| crate::error::Error::Validation(format!("Encryption failed: {}", e)))?;

    // Prepend Nonce to ciphertext
    let mut result = nonce_bytes.to_vec();
    result.append(&mut ciphertext);
    Ok(result)
}

/// Decrypts ciphertext using AES-256-GCM.
///
/// Expects the input to start with a 12-byte nonce.
pub fn decrypt(
    key: &[u8; 32],
    ciphertext_with_nonce: &[u8],
    aad: &[u8],
) -> crate::error::Result<Vec<u8>> {
    if ciphertext_with_nonce.len() < 12 {
        return Err(crate::error::Error::Validation(
            "Invalid ciphertext length".to_string(),
        ));
    }

    let (nonce_bytes, ciphertext) = ciphertext_with_nonce.split_at(12);
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    let nonce = Nonce::from_slice(nonce_bytes);

    let payload = Payload {
        msg: ciphertext,
        aad,
    };

    cipher
        .decrypt(nonce, payload)
        .map_err(|e| crate::error::Error::Validation(format!("Decryption failed: {}", e)))
}
