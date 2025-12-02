use crate::storage::Storage;
use aes_gcm::{
    aead::{Aead, KeyInit, Payload},
    Aes256Gcm, Key, Nonce,
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
        let key = self.get_or_create_key_with_txn(&mut txn, stream_id)?;
        txn.commit()?;
        Ok(key)
    }

    pub fn get_or_create_key_with_txn(
        &self,
        txn: &mut heed::RwTxn,
        stream_id: u128,
    ) -> crate::error::Result<[u8; 32]> {
        match self.storage.keystore.get(txn, &stream_id)? {
            Some(key_bytes) => {
                let mut key = [0u8; 32];
                key.copy_from_slice(key_bytes);
                Ok(key)
            }
            None => {
                let mut key = [0u8; 32];
                rand::thread_rng().fill_bytes(&mut key);
                self.storage.keystore.put(txn, &stream_id, &key)?;
                Ok(key)
            }
        }
    }

    pub fn get_key(&self, stream_id: u128) -> crate::error::Result<Option<[u8; 32]>> {
        let txn = self.storage.env.read_txn()?;
        self.get_key_with_txn(&txn, stream_id)
    }

    pub fn get_key_with_txn(
        &self,
        txn: &heed::RoTxn,
        stream_id: u128,
    ) -> crate::error::Result<Option<[u8; 32]>> {
        match self.storage.keystore.get(txn, &stream_id)? {
            Some(key_bytes) => {
                let mut key = [0u8; 32];
                key.copy_from_slice(key_bytes);
                Ok(Some(key))
            }
            None => Ok(None),
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
/// # Arguments
/// * `key` - The 32-byte encryption key.
/// * `plaintext` - The data to encrypt.
/// * `aad` - **Additional Authenticated Data**. This data is not encrypted but is authenticated.
///   It ensures that the ciphertext is "bound" to this specific context.
///   If `aad` is modified, decryption will fail.
///
/// # Returns
/// Returns a `Vec<u8>` containing:
/// 1. **Nonce (12 bytes)**: A random value unique to this encryption. Required for security.
/// 2. **Ciphertext**: The encrypted data.
///
/// We prepend the nonce to the ciphertext so it can be retrieved for decryption.
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
/// # Arguments
/// * `key` - The 32-byte encryption key.
/// * `ciphertext_with_nonce` - The byte slice containing the 12-byte Nonce followed by the Ciphertext.
/// * `aad` - The **Additional Authenticated Data** used during encryption.
///   Must match exactly what was passed to `encrypt`, otherwise decryption fails.
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
