use blockstore::types::BlockStore;
use libipld::{Block, Cid};
use std::io::{BufRead, BufReader, Read, Write};
use std::sync::Arc;
use unixfs_v1::file::{adder::FileAdder, visit::IdleFileVisit};

pub fn add<S: BlockStore>(store: Arc<S>, data: &[u8]) -> Result<Option<Cid>, String> {
    let mut adder = FileAdder::default();

    let mut total = 0;
    while total < data.len() {
        let (blocks, consumed) = adder.push(&data[total..]);
        total += consumed;

        for (cid, bytes) in blocks {
            let block = Block::<S::Params>::new_unchecked(cid, bytes);
            store.insert(&block).map_err(|e| e.to_string())?;
        }
    }

    let blocks = adder.finish();

    let mut root: Option<Cid> = None;
    for (cid, bytes) in blocks {
        root = Some(cid.clone());
        let block = Block::<S::Params>::new_unchecked(cid, bytes);
        store.insert(&block).map_err(|e| e.to_string())?;
    }
    Ok(root)
}

pub fn add_from_read<S: BlockStore, F: Read>(
    store: Arc<S>,
    data: &mut F,
) -> Result<Option<Cid>, String> {
    let mut adder = FileAdder::default();
    // use BufReader for speed / efficiency and 100KiB buffer
    let mut buf = BufReader::with_capacity(100000, data);

    loop {
        match buf.fill_buf().unwrap() {
            chunk if chunk.is_empty() => {
                break;
            }
            chunk => {
                let mut total = 0;
                while total < chunk.len() {
                    let (blocks, consumed) = adder.push(&chunk[total..]);
                    total += consumed;
                    for (cid, bytes) in blocks {
                        let block = Block::<S::Params>::new_unchecked(cid, bytes);
                        store.insert(&block).map_err(|e| e.to_string())?;
                    }
                }
                buf.consume(total);
            }
        }
    }
    let blocks = adder.finish();
    let mut root: Option<Cid> = None;
    for (cid, bytes) in blocks {
        root = Some(cid.clone());
        let block = Block::<S::Params>::new_unchecked(cid, bytes);
        store.insert(&block).map_err(|e| e.to_string())?;
    }
    return Ok(root);
}

pub fn cat<S: BlockStore>(store: Arc<S>, root: Cid) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();

    let first = store.get(&root).map_err(|e| e.to_string())?;

    let (content, _, _metadata, mut step) = IdleFileVisit::default()
        .start(first.data())
        .map_err(|e| e.to_string())?;
    Write::write_all(&mut buf, content).map_err(|e| e.to_string())?;

    while let Some(visit) = step {
        let (first, _) = visit.pending_links();
        let block = store.get(&first).map_err(|e| e.to_string())?;

        let (content, next_step) = visit
            .continue_walk(block.data(), &mut None)
            .map_err(|e| e.to_string())?;
        Write::write_all(&mut buf, content).map_err(|e| e.to_string())?;
        step = next_step;
    }

    Ok(buf)
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_add_cat() {
        use super::*;
        use blockstore::memory::MemoryDB;
        use rand::prelude::*;

        // generate some random bytes
        const FILE_SIZE: usize = 500 * 1024;
        let mut data = vec![0u8; FILE_SIZE];
        rand::thread_rng().fill_bytes(&mut data);

        let store = Arc::new(MemoryDB::default());

        let root = add(store.clone(), &data).unwrap();
        let buf = cat(store, root.unwrap()).unwrap();

        assert_eq!(&buf, &data);
    }
}
