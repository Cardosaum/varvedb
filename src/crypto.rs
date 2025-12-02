use crate::storage::Storage;
use aes_gcm::{
    Aes256Gcm, Key, Nonce,
    aead::{Aead, KeyInit},
};
use rand::RngCore;

pub struct KeyManager {
    storage: Storage,
}

impl KeyManager {
    pub fn new(storage: Storage) -> Self {
        Self { storage }
    }

    pub fn get_or_create_key(&self, stream_id: u128) -> crate::error::Result<[u8; 32]> {
        let mut txn = self.storage.env.write_txn()?;

        if let Some(key_bytes) = self.storage.keystore.get(&txn, &stream_id)? {
            let mut key = [0u8; 32];
            key.copy_from_slice(key_bytes);
            txn.commit()?;
            Ok(key)
        } else {
            let mut key = [0u8; 32];
            rand::thread_rng().fill_bytes(&mut key);

            self.storage.keystore.put(&mut txn, &stream_id, &key)?;
            txn.commit()?;
            Ok(key)
        }
    }

    pub fn delete_key(&self, stream_id: u128) -> crate::error::Result<()> {
        let mut txn = self.storage.env.write_txn()?;
        self.storage.keystore.delete(&mut txn, &stream_id)?;
        txn.commit()?;
        Ok(())
    }
}

pub fn encrypt(key: &[u8; 32], plaintext: &[u8]) -> crate::error::Result<Vec<u8>> {
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    let mut nonce_bytes = [0u8; 12];
    rand::thread_rng().fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);

    let mut ciphertext = cipher
        .encrypt(nonce, plaintext)
        .map_err(|e| crate::error::Error::Validation(format!("Encryption failed: {}", e)))?;

    // Prepend Nonce to ciphertext
    let mut result = nonce_bytes.to_vec();
    result.append(&mut ciphertext);
    Ok(result)
}

pub fn decrypt(key: &[u8; 32], ciphertext_with_nonce: &[u8]) -> crate::error::Result<Vec<u8>> {
    if ciphertext_with_nonce.len() < 12 {
        return Err(crate::error::Error::Validation(
            "Invalid ciphertext length".to_string(),
        ));
    }

    let (nonce_bytes, ciphertext) = ciphertext_with_nonce.split_at(12);
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    let nonce = Nonce::from_slice(nonce_bytes);

    cipher
        .decrypt(nonce, ciphertext)
        .map_err(|e| crate::error::Error::Validation(format!("Decryption failed: {}", e)))
}
