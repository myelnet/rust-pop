use crate::types::DBStore;

pub fn test_write<DB>(db: &DB)
where
    DB: DBStore,
{
    let key = [1];
    let value = [1];
    db.write(key, value).unwrap();
}

pub fn test_read<DB>(db: &DB)
where
    DB: DBStore,
{
    let key = [0];
    let value = [1];
    db.write(key, value).unwrap();
    let res = db.read(key).unwrap().unwrap();
    assert_eq!(value.as_ref(), res.as_slice());
}

pub fn test_exists<DB>(db: &DB)
where
    DB: DBStore,
{
    let key = [0];
    let value = [1];
    db.write(key, value).unwrap();
    let res = db.exists(key).unwrap();
    assert_eq!(res, true);
}

pub fn test_does_not_exist<DB>(db: &DB)
where
    DB: DBStore,
{
    let key = [0];
    let res = db.exists(key).unwrap();
    assert_eq!(res, false);
}

pub fn test_delete<DB>(db: &DB)
where
    DB: DBStore,
{
    let key = [0];
    let value = [1];
    db.write(key, value).unwrap();
    let res = db.exists(key).unwrap();
    assert_eq!(res, true);
    db.delete(key).unwrap();
    let res = db.exists(key).unwrap();
    assert_eq!(res, false);
}

pub fn test_bulk_write<DB>(db: &DB)
where
    DB: DBStore,
{
    let values = [([0], [0]), ([1], [1]), ([2], [2])];
    db.bulk_write(&values).unwrap();
    for (k, _) in values.iter() {
        let res = db.exists(*k).unwrap();
        assert_eq!(res, true);
    }
}

pub fn test_bulk_read<DB>(db: &DB)
where
    DB: DBStore,
{
    let keys = [[0], [1], [2]];
    let values = [[0], [1], [2]];
    let kvs: Vec<_> = keys.iter().zip(values.iter()).collect();
    db.bulk_write(&kvs).unwrap();
    let results = db.bulk_read(&keys).unwrap();
    for (result, value) in results.iter().zip(values.iter()) {
        match result {
            Some(v) => assert_eq!(v, value),
            None => panic!("No values found!"),
        }
    }
}

pub fn test_bulk_delete<DB>(db: &DB)
where
    DB: DBStore,
{
    let keys = [[0], [1], [2]];
    let values = [[0], [1], [2]];
    let kvs: Vec<_> = keys.iter().zip(values.iter()).collect();
    db.bulk_write(&kvs).unwrap();
    db.bulk_delete(&keys).unwrap();
    for k in keys.iter() {
        let res = db.exists(*k).unwrap();
        assert_eq!(res, false);
    }
}
