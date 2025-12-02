use tempfile::tempdir;
use varvedb::storage::{Storage, StorageConfig};

#[test]
fn test_storage_validation() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;

    // Test 1: map_size = 0
    let config = StorageConfig {
        path: dir.path().join("test_zero_map.mdb"),
        map_size: 0,
        ..Default::default()
    };
    match Storage::open(config) {
        Err(varvedb::error::Error::Validation(msg)) => {
            assert!(msg.contains("map_size"));
        }
        Ok(_) => panic!("Expected validation error for map_size=0"),
        Err(e) => panic!("Expected Validation error, got {:?}", e),
    }

    // Test 2: max_dbs too small
    let config = StorageConfig {
        path: dir.path().join("test_small_dbs.mdb"),
        max_dbs: 3,
        ..Default::default()
    };
    match Storage::open(config) {
        Err(varvedb::error::Error::Validation(msg)) => {
            assert!(msg.contains("max_dbs"));
        }
        Ok(_) => panic!("Expected validation error for max_dbs=3"),
        Err(e) => panic!("Expected Validation error, got {:?}", e),
    }

    Ok(())
}
