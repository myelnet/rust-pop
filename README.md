# pop

A Rust implementation of Myel points of presence for wasm and all platforms.


### WASM

```sh
# install rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# install wasm-pack
curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh

# install wasm target
rustup target add wasm32-unknown-unknown
rustup target add wasm32-unknown-unknown --toolchain nightly

# Build the browser pkg (linked in root/package.json)
wasm-pack build --target web --out-dir web/src/wasm -- --features browser --no-default-features

# Move the wasm file to the dev server public directory
mv src/wasm/pop_bg.wasm public/pop_bg.wasm

# Start the dev server (after running npm install)
cd web && npm run start

# Build the native service
cargo build --bin bootnode

# Run the local service, take note of the multiaddr
target/debug/bootnode

```
Open http://localhost:8000 in a new incognito window.

#### M1 Mac

Additional steps you may need to run

```sh
brew install binaryen
cargo install wasm-pack --git https://github.com/rustwasm/wasm-pack --rev c9ea9aebbccf5029846a24a6a823b18bb41736c7
```
