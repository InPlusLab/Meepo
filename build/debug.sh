echo "if error, running: rustup override set 1.51"
sleep 3
cd ../
cargo build
rm build/openethereum
mv target/debug/openethereum build/
echo "build debug finish"
