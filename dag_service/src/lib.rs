pub mod car;
mod error;

use blockstore::types::BlockStore;
use futures::prelude::*;
use libipld::{Block, Cid};
use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::marker::Unpin;
use std::sync::Arc;
use unixfs_v1::dir::builder::{BufferingTreeBuilder, TreeNode, TreeOptions};
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
        root = Some(cid);
        let block = Block::<S::Params>::new_unchecked(cid, bytes);
        store.insert(&block).map_err(|e| e.to_string())?;
    }
    Ok(root)
}

pub fn add_from_read<S: BlockStore, F: Read>(
    store: Arc<S>,
    data: &mut F,
) -> Result<Option<(Cid, u64)>, String> {
    let mut adder = FileAdder::default();
    // use BufReader for speed / efficiency
    let mut buf = BufReader::with_capacity(adder.size_hint(), data);

    let mut size = 0;
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
                size += total;
                buf.consume(total);
            }
        }
    }
    let blocks = adder.finish();
    let mut root: Option<(Cid, u64)> = None;
    for (cid, bytes) in blocks {
        root = Some((cid, size as u64));
        let block = Block::<S::Params>::new_unchecked(cid, bytes);
        store.insert(&block).map_err(|e| e.to_string())?;
    }
    Ok(root)
}

pub async fn add_from_stream<S: BlockStore>(
    store: Arc<S>,
    stream: &mut (impl AsyncRead + Unpin),
) -> Result<Cid, String> {
    let mut adder = FileAdder::default();

    loop {
        let mut buf = Vec::new();
        match stream.read(&mut buf).await.map_err(|e| e.to_string())? {
            0 => {
                break;
            }
            n => {
                let mut total = 0;
                while total < n {
                    let (blocks, consumed) = adder.push(&buf[total..]);
                    total += consumed;
                    for (cid, bytes) in blocks {
                        let block = Block::<S::Params>::new_unchecked(cid, bytes);
                        store.insert(&block).map_err(|e| e.to_string())?;
                    }
                }
            }
        }
    }
    let blocks = adder.finish();
    let mut root: Option<Cid> = None;
    for (cid, bytes) in blocks {
        root = Some(cid);
        let block = Block::<S::Params>::new_unchecked(cid, bytes);
        store.insert(&block).map_err(|e| e.to_string())?;
    }

    match root {
        Some(root) => Ok(root),
        None => Err("Failed to dagify stream".to_string()),
    }
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
        let block = store.get(first).map_err(|e| e.to_string())?;

        let (content, next_step) = visit
            .continue_walk(block.data(), &mut None)
            .map_err(|e| e.to_string())?;
        Write::write_all(&mut buf, content).map_err(|e| e.to_string())?;
        step = next_step;
    }

    Ok(buf)
}

pub fn cat_to_write<S: BlockStore, F: Write>(
    store: Arc<S>,
    root: Cid,
    out: F,
) -> Result<(), String> {
    let mut buf = BufWriter::with_capacity(100000, out);

    let first = store.get(&root).map_err(|e| e.to_string())?;

    let (content, _, _metadata, mut step) = IdleFileVisit::default()
        .start(first.data())
        .map_err(|e| e.to_string())?;
    Write::write_all(&mut buf, content).map_err(|e| e.to_string())?;

    while let Some(visit) = step {
        let (first, _) = visit.pending_links();
        let block = store.get(first).map_err(|e| e.to_string())?;

        let (content, next_step) = visit
            .continue_walk(block.data(), &mut None)
            .map_err(|e| e.to_string())?;
        Write::write_all(&mut buf, content).map_err(|e| e.to_string())?;
        step = next_step;
    }

    buf.flush().map_err(|e| e.to_string())?;

    Ok(())
}

pub struct Entry<R> {
    pub name: String,
    pub reader: R,
}

impl<R: Read> Read for Entry<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.reader.read(buf)
    }
}

pub fn add_entries<S: BlockStore, R: Read>(
    store: Arc<S>,
    mut entries: Vec<Entry<R>>,
) -> Result<(Cid, u64), String> {
    let mut options = TreeOptions::default();
    options.wrap_with_directory();
    let mut builder = BufferingTreeBuilder::new(options);
    for entry in entries.iter_mut() {
        if let Some((cid, size)) = add_from_read(store.clone(), entry)? {
            builder
                .put_link(&entry.name, cid, size)
                .map_err(|e| e.to_string())?;
        }
    }
    let mut iter = builder.build();
    // should be creating a single directory so no need to iterate
    if let Some(res) = iter.next_borrowed() {
        let TreeNode {
            cid,
            block,
            total_size,
            ..
        } = res.map_err(|e| e.to_string())?;
        let block = Block::<S::Params>::new_unchecked(*cid, block.to_vec());
        store.insert(&block).map_err(|e| e.to_string())?;
        return Ok((*cid, total_size));
    }
    Err("Failed to create directory".to_string())
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
    #[test]
    fn test_add_entries() {
        use super::*;
        use blockstore::memory::MemoryDB;
        use rand::prelude::*;

        let mut files = Vec::new();
        let mut entries = Vec::new();
        for i in 0..4 {
            let mut data = vec![0u8; i * 1024];
            rand::thread_rng().fill_bytes(&mut data);
            files.push(data);
        }
        for (i, data) in files.iter().enumerate() {
            entries.push(Entry {
                name: format!("file {}", i),
                reader: &data[..],
            })
        }
        let store = Arc::new(MemoryDB::default());
        let (_root, _size) = add_entries(store, entries).unwrap();
    }
}
