// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

use crate::storage::Storage;
use aes_gcm::{
    aead::{Aead, KeyInit, Payload},
    Aes256Gcm, Key, Nonce,
};
use rand::rngs::OsRng;
use rand::RngCore;
use zeroize::Zeroizing;

/// Manages the lifecycle of encryption keys.
///
/// The `KeyManager` is responsible for generating, retrieving, and securely storing per-stream encryption keys.
/// It employs a key wrapping strategy where each stream's key is encrypted using the global `master_key`
/// before being persisted in the `keystore` bucket.
///
/// # Key Hierarchy
///
/// 1.  **Master Key**: Provided in `StorageConfig`. Used to encrypt Stream Keys.
/// 2.  **Stream Key**: Generated randomly (32 bytes) for each stream. Used to encrypt Event Data.
///
/// # Examples
///
/// ```rust
/// use varvedb::storage::{Storage, StorageConfig};
/// use varvedb::crypto::KeyManager;
/// use tempfile::tempdir;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let dir = tempdir()?;
/// let config = StorageConfig {
///     path: dir.path().to_path_buf(),
///     encryption_enabled: true,
///     master_key: Some(zeroize::Zeroizing::new([0u8; varvedb::constants::KEY_SIZE])),
///     ..Default::default()
/// };
/// let storage = Storage::open(config)?;
/// let key_manager = KeyManager::new(storage);
///
/// // Retrieve or create a key for stream 123
/// let key = key_manager.get_or_create_key(123)?;
/// println!("Key: {:?}", key);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct KeyManager {
    storage: Storage,
}

impl KeyManager {
    /// Creates a new `KeyManager` instance.
    pub fn new(storage: Storage) -> Self {
        Self { storage }
    }

    fn get_master_key(&self) -> crate::error::Result<&[u8; crate::constants::KEY_SIZE]> {
        self.storage
            .config
            .master_key
            .as_deref()
            .ok_or_else(|| crate::error::Error::KeyNotFound(0)) // 0 for master key
    }

    pub fn get_or_create_key(
        &self,
        stream_id: u128,
    ) -> crate::error::Result<Zeroizing<[u8; crate::constants::KEY_SIZE]>> {
        let mut txn = self.storage.env.write_txn()?;
        let key = self.get_or_create_key_with_txn(&mut txn, stream_id)?;
        txn.commit()?;
        Ok(key)
    }

    pub fn get_or_create_key_with_txn(
        &self,
        txn: &mut heed::RwTxn,
        stream_id: u128,
    ) -> crate::error::Result<Zeroizing<[u8; crate::constants::KEY_SIZE]>> {
        match self.storage.keystore.get(txn, &stream_id)? {
            Some(encrypted_key_bytes) => {
                // Decrypt existing key
                let master_key = self.get_master_key()?;
                let aad = stream_id.to_be_bytes(); // Bind key to StreamID
                let plaintext_key_vec = decrypt(master_key, encrypted_key_bytes, &aad)?;

                let mut key = Zeroizing::new([0u8; crate::constants::KEY_SIZE]);
                if plaintext_key_vec.len() != crate::constants::KEY_SIZE {
                    return Err(crate::error::Error::InvalidKeyLength {
                        actual: plaintext_key_vec.len(),
                        expected: crate::constants::KEY_SIZE,
                    });
                }
                key.copy_from_slice(&plaintext_key_vec);
                Ok(key)
            }
            None => {
                // Generate new key
                let mut key = Zeroizing::new([0u8; crate::constants::KEY_SIZE]);
                OsRng.fill_bytes(&mut *key);

                // Encrypt with Master Key
                let master_key = self.get_master_key()?;
                let aad = stream_id.to_be_bytes();
                let encrypted_key = encrypt(master_key, &*key, &aad)?;

                self.storage.keystore.put(txn, &stream_id, &encrypted_key)?;
                Ok(key)
            }
        }
    }

    pub fn get_key(
        &self,
        stream_id: u128,
    ) -> crate::error::Result<Option<Zeroizing<[u8; crate::constants::KEY_SIZE]>>> {
        let txn = self.storage.env.read_txn()?;
        self.get_key_with_txn(&txn, stream_id)
    }

    pub fn get_key_with_txn(
        &self,
        txn: &heed::RoTxn,
        stream_id: u128,
    ) -> crate::error::Result<Option<Zeroizing<[u8; crate::constants::KEY_SIZE]>>> {
        match self.storage.keystore.get(txn, &stream_id)? {
            Some(encrypted_key_bytes) => {
                let master_key = self.get_master_key()?;
                let aad = stream_id.to_be_bytes();
                let plaintext_key_vec = decrypt(master_key, encrypted_key_bytes, &aad)?;

                let mut key = Zeroizing::new([0u8; crate::constants::KEY_SIZE]);
                if plaintext_key_vec.len() != crate::constants::KEY_SIZE {
                    return Err(crate::error::Error::InvalidKeyLength {
                        actual: plaintext_key_vec.len(),
                        expected: crate::constants::KEY_SIZE,
                    });
                }
                key.copy_from_slice(&plaintext_key_vec);
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

/// Encrypts data using AES-256-GCM.
///
/// This function performs authenticated encryption with associated data (AEAD).
/// It generates a random 12-byte nonce for each encryption operation and prepends it
/// to the resulting ciphertext.
///
/// # Arguments
///
/// *   `key`: The 32-byte (256-bit) encryption key.
/// *   `plaintext`: The data to be encrypted.
/// *   `aad`: Additional Authenticated Data. This data is not encrypted but is integrity-protected.
///     It is used to bind the ciphertext to a specific context (e.g., Stream ID + Sequence Number),
///     preventing replay or relocation attacks.
///
/// # Returns
///
/// A vector containing `[Nonce (12 bytes) | Ciphertext | Auth Tag (16 bytes)]`.
///
/// # Errors
///
/// Returns an error if encryption fails (e.g., internal crypto error).
pub fn encrypt(
    key: &[u8; crate::constants::KEY_SIZE],
    plaintext: &[u8],
    aad: &[u8],
) -> crate::error::Result<Vec<u8>> {
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    let mut nonce_bytes = [0u8; crate::constants::NONCE_SIZE];
    OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);

    let payload = Payload {
        msg: plaintext,
        aad,
    };

    let mut ciphertext = cipher
        .encrypt(nonce, payload)
        .map_err(|e| crate::error::Error::EncryptionError(format!("Encryption failed: {}", e)))?;

    // Prepend Nonce to ciphertext
    let mut result = nonce_bytes.to_vec();
    result.append(&mut ciphertext);
    Ok(result)
}

/// Decrypts data using AES-256-GCM.
///
/// Expects the input to contain the 12-byte nonce prepended to the ciphertext.
///
/// # Arguments
///
/// *   `key`: The 32-byte (256-bit) decryption key.
/// *   `ciphertext_with_nonce`: The byte slice containing `[Nonce (12 bytes) | Ciphertext]`.
/// *   `aad`: The Additional Authenticated Data used during encryption. Must match exactly.
///
/// # Errors
///
/// Returns an error if:
/// *   The input is too short (less than 12 bytes).
/// *   Decryption fails (e.g., invalid key, tampered ciphertext, or AAD mismatch).
pub fn decrypt(
    key: &[u8; crate::constants::KEY_SIZE],
    ciphertext_with_nonce: &[u8],
    aad: &[u8],
) -> crate::error::Result<Vec<u8>> {
    if ciphertext_with_nonce.len() < crate::constants::NONCE_SIZE {
        return Err(crate::error::Error::InvalidCiphertextLength {
            actual: ciphertext_with_nonce.len(),
            minimum: crate::constants::NONCE_SIZE,
        });
    }

    let (nonce_bytes, ciphertext) = ciphertext_with_nonce.split_at(crate::constants::NONCE_SIZE);
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    let nonce = Nonce::from_slice(nonce_bytes);

    let payload = Payload {
        msg: ciphertext,
        aad,
    };

    cipher
        .decrypt(nonce, payload)
        .map_err(|e| crate::error::Error::DecryptionError(format!("Decryption failed: {}", e)))
}
