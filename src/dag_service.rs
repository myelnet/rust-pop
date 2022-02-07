use libipld::store::Store;
use libipld::{Block, Cid};
use std::io::Write;
use std::sync::Arc;
use unixfs_v1::file::{adder::FileAdder, visit::IdleFileVisit};

pub fn add<S: Store>(store: Arc<S>, data: &[u8]) -> Result<Option<Cid>, String> {
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

pub fn cat<S: Store>(store: Arc<S>, root: Cid) -> Result<Vec<u8>, String> {
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
        use libipld::mem::MemStore;
        use libipld::DefaultParams;
        use rand::prelude::*;

        // generate some random bytes
        const file_size: usize = 500 * 1024;
        let mut data = vec![0u8; file_size];
        rand::thread_rng().fill_bytes(&mut data);

        let store = Arc::new(MemStore::<DefaultParams>::default());

        let root = add(store.clone(), &data).unwrap();
        let buf = cat(store, root.unwrap()).unwrap();

        assert_eq!(&buf, &data);
    }
}