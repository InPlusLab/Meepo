echo "if error, running: rustup override set 1.51"
sleep 3
cd ../
cargo build --release
rm build/openethereum
mv target/release/openethereum build/
echo "build release finish"
