use super::error::Error;
use blockstore::types::BlockStore;
use filecoin::{cid_helpers::CidCbor, types::Cbor};
use integer_encoding::VarIntReader;
use libipld::codec::Decode;
use libipld::store::StoreParams;
use libipld::{Block, Cid, Ipld};
use serde::{Deserialize, Serialize};
use std::io::{BufReader, Read};
use std::marker::PhantomData;
use std::sync::Arc;

#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct CarHeader {
    pub roots: Vec<CidCbor>,
    pub version: u64,
}

impl Cbor for CarHeader {}

pub fn read_block<R: Read>(buf_reader: &mut R) -> Result<Option<(Cid, Vec<u8>)>, Error> {
    let buf = match ld_read(buf_reader) {
        Ok(buf) => buf,
        Err(e) => match e {
            Error::Eof => return Ok(None),
            _ => return Err(e),
        },
    };
    if buf.is_empty() {
        return Ok(None);
    }
    let mut cursor = std::io::Cursor::new(&buf);
    let cid = Cid::read_bytes(&mut cursor).map_err(|e| Error::Parsing(e.to_string()))?;
    Ok(Some((cid, buf[cursor.position() as usize..].to_vec())))
}

pub struct CarReader<R, P> {
    pub reader: R,
    pub header: CarHeader,
    _marker: PhantomData<P>,
}

impl<R, P> CarReader<R, P>
where
    R: Read,
    P: StoreParams,
    Ipld: Decode<<P>::Codecs>,
{
    /// Creates a new CarReader and parses the CarHeader
    pub fn new(mut reader: R) -> Result<Self, Error> {
        let buf = ld_read(&mut reader)?;
        let header = CarHeader::unmarshal_cbor(&buf).map_err(|e| Error::Parsing(e.to_string()))?;
        if header.roots.is_empty() {
            return Err(Error::Parsing("empty CAR file".to_owned()));
        }
        if header.version != 1 {
            return Err(Error::InvalidFile("CAR file version must be 1".to_owned()));
        }
        Ok(CarReader {
            reader,
            header,
            _marker: PhantomData,
        })
    }

    /// Verifies and returns the next IPLD Block in the buffer
    pub fn next_block(&mut self) -> Result<Option<Block<P>>, Error> {
        match read_block(&mut self.reader)?.map(|(cid, data)| Block::new(cid, data)) {
            Some(Ok(blk)) => Ok(Some(blk)),
            Some(Err(e)) => Err(e.into()),
            None => Ok(None),
        }
    }
}

pub fn import_car_file<S: BlockStore, F: Read>(store: Arc<S>, file: &mut F) -> Result<Cid, Error>
where
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    let buf_reader = BufReader::new(file);

    let mut car_reader = CarReader::new(buf_reader)?;
    while let Some(block) = car_reader.next_block()? {
        store
            .insert(&block)
            .map_err(|e| Error::Store(e.to_string()))?;
    }
    // cars usually have a single root
    let root = car_reader.header.roots[0]
        .to_cid()
        .ok_or(Error::Other("failed to parse CAR root"))?;
    Ok(root)
}

pub fn ld_read<R: Read>(reader: &mut R) -> Result<Vec<u8>, Error> {
    let l: usize = reader.read_varint()?;
    let mut buf = vec![0u8; l];
    reader.read_exact(&mut buf)?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockstore::memory::MemoryDB;
    use hex;

    #[test]
    fn read_car() {
        let car_data = hex::decode("3aa265726f6f747381d82a58250001711220151fe9e73c6267a7060c6f6c4cca943c236f4b196723489608edb42a8b8fa80b6776657273696f6e012c01711220151fe9e73c6267a7060c6f6c4cca943c236f4b196723489608edb42a8b8fa80ba165646f646779f5").unwrap();
        let store = Arc::new(MemoryDB::default());
        let root = import_car_file(store, &mut &car_data[..]).unwrap();
        assert_eq!(
            root.to_string(),
            "bafyreiavd7u6opdcm6tqmddpnrgmvfb4enxuwglhenejmchnwqvixd5ibm".to_string()
        );
    }
}
