use log::{error, warn};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, ErrorKind};
use std::path::PathBuf;

use super::errors::Error;

pub const KEYSTORE_NAME: &str = "keystore.json";

#[derive(Clone, PartialEq, Debug, Eq, Serialize, Deserialize, Copy)]
pub enum KeyType {
    Secp256k1 = 1,
    Ed25519 = 2,
}

#[derive(Clone, PartialEq, Debug, Eq, Serialize, Deserialize)]
pub struct KeyInfo {
    pub key_type: KeyType,
    private_key: Vec<u8>,
}
/// KeyInfo struct, this contains the type of key (stored as a string) and the private key.
#[derive(Clone, PartialEq, Debug, Eq, Serialize, Deserialize)]
pub struct PersistentKeyInfo {
    pub key_type: KeyType,
    private_key: String,
}

impl KeyInfo {
    /// Return a new KeyInfo given the key_type and private_key
    pub fn new(key_type: KeyType, private_key: Vec<u8>) -> Self {
        KeyInfo {
            key_type,
            private_key,
        }
    }

    /// Return a reference to the key_type
    pub fn key_type(&self) -> &KeyType {
        &self.key_type
    }

    /// Return a reference to the private_key
    pub fn private_key(&self) -> &Vec<u8> {
        &self.private_key
    }
}

/// KeyStore struct, this contains a HashMap that is a set of KeyInfos resolved by their Address
pub trait Store {
    /// Return all of the keys that are stored in the KeyStore
    fn list(&self) -> Vec<String>;
    /// Return Keyinfo that corresponds to a given key
    fn get(&self, k: &str) -> Result<KeyInfo, Error>;
    /// Save a key key_info pair to the KeyStore
    fn put(&mut self, key: String, key_info: KeyInfo) -> Result<(), Error>;
    /// Remove the Key and corresponding key_info from the KeyStore
    fn remove(&mut self, key: String) -> Result<KeyInfo, Error>;
}

/// KeyStore struct, this contains a HashMap that is a set of KeyInfos resolved by their Address
#[derive(Clone, PartialEq, Debug, Eq)]
pub struct KeyStore {
    key_info: HashMap<String, KeyInfo>,
    persistence: Option<PersistentKeyStore>,
}

pub enum KeyStoreConfig {
    Memory,
    Persistent(PathBuf),
}

/// Persistent KeyStore in JSON cleartext in KEYSTORE_LOCATION
#[derive(Clone, PartialEq, Debug, Eq)]
struct PersistentKeyStore {
    file_path: PathBuf,
}

impl KeyStore {
    pub fn new(config: KeyStoreConfig) -> Result<Self, Error> {
        match config {
            KeyStoreConfig::Memory => Ok(Self {
                key_info: HashMap::new(),
                persistence: None,
            }),
            KeyStoreConfig::Persistent(location) => {
                let file_path = location.join(KEYSTORE_NAME);

                match File::open(&file_path) {
                    Ok(file) => {
                        let reader = BufReader::new(file);

                        // Existing cleartext JSON keystore
                        let persisted_key_info: HashMap<String, PersistentKeyInfo> =
                            serde_json::from_reader(reader)
                                .map_err(|e| {
                                    error!(
                                "failed to deserialize keyfile, initializing new keystore at: {:?}",
                                file_path
                            );
                                    e
                                })
                                .unwrap_or_default();

                        let mut key_info = HashMap::new();
                        for (key, value) in persisted_key_info.iter() {
                            key_info.insert(
                                key.to_string(),
                                KeyInfo {
                                    private_key: base64::decode(value.private_key.clone())
                                        .map_err(|error| Error::Other(error.to_string()))?,
                                    key_type: value.key_type,
                                },
                            );
                        }

                        Ok(Self {
                            key_info,
                            persistence: Some(PersistentKeyStore { file_path }),
                        })
                    }
                    Err(e) => {
                        if e.kind() == ErrorKind::NotFound {
                            warn!(
                                "Keystore does not exist, initializing new keystore at: {:?}",
                                file_path
                            );
                            Ok(Self {
                                key_info: HashMap::new(),
                                persistence: Some(PersistentKeyStore { file_path }),
                            })
                        } else {
                            Err(Error::Other(e.to_string()))
                        }
                    }
                }
            }
        }
    }

    pub fn flush(&self) -> Result<(), Error> {
        match &self.persistence {
            Some(persistent_keystore) => {
                let dir = persistent_keystore
                    .file_path
                    .parent()
                    .ok_or_else(|| Error::Other("Invalid Path".to_string()))?;
                fs::create_dir_all(dir)?;
                let file = File::create(&persistent_keystore.file_path)?;

                let writer = BufWriter::new(file);

                let mut key_info: HashMap<String, PersistentKeyInfo> = HashMap::new();
                for (key, value) in self.key_info.iter() {
                    key_info.insert(
                        key.to_string(),
                        PersistentKeyInfo {
                            private_key: base64::encode(value.private_key.clone()),
                            key_type: value.key_type,
                        },
                    );
                }

                // Flush for PersistentKeyStore
                serde_json::to_writer_pretty(writer, &key_info).map_err(|e| {
                    Error::Other(format!("failed to serialize and write key info: {}", e))
                })?;

                Ok(())
            }
            None => {
                // NoOp for MemKeyStore
                Ok(())
            }
        }
    }

    /// Return all of the keys that are stored in the KeyStore
    pub fn list(&self) -> Vec<String> {
        self.key_info.iter().map(|(key, _)| key.clone()).collect()
    }

    /// Return all of the info about keys that are stored in the KeyStore
    pub fn list_info(&self) -> Vec<KeyInfo> {
        self.key_info.iter().map(|(_, info)| info.clone()).collect()
    }

    /// Return Keyinfo that corresponds to a given key
    pub fn get(&self, k: &str) -> Result<KeyInfo, Error> {
        self.key_info.get(k).cloned().ok_or(Error::KeyInfo)
    }

    /// Save a key key_info pair to the KeyStore
    pub fn put(&mut self, key: String, key_info: KeyInfo) -> Result<(), Error> {
        if self.key_info.contains_key(&key) {
            return Err(Error::KeyExists);
        }
        self.key_info.insert(key, key_info);

        if self.persistence.is_some() {
            self.flush()?;
        }

        Ok(())
    }

    /// Remove the Key and corresponding key_info from the KeyStore
    pub fn remove(&mut self, key: String) -> Result<KeyInfo, Error> {
        let key_out = self.key_info.remove(&key).ok_or(Error::KeyInfo)?;

        if self.persistence.is_some() {
            self.flush()?;
        }

        Ok(key_out)
    }
}
