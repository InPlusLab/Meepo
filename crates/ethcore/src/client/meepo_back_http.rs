//#![cfg_attr(debug_assertions, allow(dead_code,  unused_imports, unused_variables, unused_mut))]
#![allow(warnings)]

// client use
use std::{
    cmp,
    collections::{BTreeMap, HashSet, VecDeque},
    convert::TryFrom,
    io::{BufRead, BufReader},
    str::{from_utf8, FromStr},
    sync::{
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering as AtomicOrdering},
        Arc, Weak,
    },
    time::{Duration, Instant},
};

use blockchain::{
    BlockChain, BlockChainDB, BlockNumberKey, BlockProvider, BlockReceipts, ExtrasInsert,
    ImportRoute, TransactionAddress, TreeRoute,
};
use bytes::{Bytes, ToPretty};
use call_contract::CallContract;
use db::{DBTransaction, DBValue, KeyValueDB};
use ethcore_miner::pool::VerifiedTransaction;
use ethereum_types::{Address, H256, H264, U256};
use hash::keccak;
use itertools::Itertools;
use parking_lot::{Mutex, RwLock};
use rand::rngs::OsRng;
use rlp::{PayloadInfo, Rlp};
use rustc_hex::FromHex;
use trie::{Trie, TrieFactory, TrieSpec};
use types::{
    ancestry_action::AncestryAction,
    data_format::DataFormat,
    encoded,
    filter::Filter,
    header::{ExtendedHeader, Header},
    log_entry::LocalizedLogEntry,
    receipt::{LocalizedReceipt, TypedReceipt},
    transaction::{
        self, Action, LocalizedTransaction, SignedTransaction, TypedTransaction,
        UnverifiedTransaction,
    },
    BlockNumber,
};
use vm::{EnvInfo, LastHashes};

use ansi_term::Colour;
use block::{enact_verified, ClosedBlock, Drain, LockedBlock, OpenBlock, SealedBlock};
use call_contract::RegistryInfo;
use client::{
    ancient_import::AncientVerifier,
    bad_blocks,
    traits::{ForceUpdateSealing, TransactionRequest},
    AccountData, BadBlocks, Balance, BlockChain as BlockChainTrait, BlockChainClient,
    BlockChainReset, BlockId, BlockInfo, BlockProducer, BroadcastProposalBlock, Call,
    CallAnalytics, ChainInfo, ChainMessageType, ChainNotify, ChainRoute, ClientConfig,
    ClientIoMessage, EngineInfo, ImportBlock, ImportExportBlocks, ImportSealedBlock, IoClient,
    Mode, NewBlocks, Nonce, PrepareOpenBlock, ProvingBlockChainClient, PruningInfo, ReopenBlock,
    ScheduleInfo, SealedBlockImporter, StateClient, StateInfo, StateOrBlock, TraceFilter, TraceId,
    TransactionId, TransactionInfo, UncleId,
};
use engines::{
    epoch::PendingTransition, EngineError, EpochTransition, EthEngine, ForkChoice, SealingState,
    MAX_UNCLE_AGE,
};
use error::{
    BlockError, CallError, Error, Error as EthcoreError, ErrorKind as EthcoreErrorKind,
    EthcoreResult, ExecutionError, ImportErrorKind, QueueErrorKind,
};
use executive::{contract_address, Executed, Executive, TransactOptions};
use factory::{Factories, VmFactory};
use io::IoChannel;
use miner::{Miner, MinerService};
use snapshot::{self, io as snapshot_io, SnapshotClient};
use spec::Spec;
use state::{self, State};
use state_db::StateDB;
use stats::{PrometheusMetrics, PrometheusRegistry};
use trace::{
    self, Database as TraceDatabase, ImportRequest as TraceImportRequest, LocalizedTrace, TraceDB,
};
use transaction_ext::Transaction;
use verification::{
    self,
    queue::kind::{blocks::Unverified, BlockLike},
    BlockQueue, PreverifiedBlock, Verifier,
};
use vm::Schedule;
// re-export
pub use blockchain::CacheSize as BlockChainCacheSize;
use db::{keys::BlockDetails, Readable, Writable};
pub use reth_util::queue::ExecutionQueue;
pub use types::{block_status::BlockStatus, blockchain_info::BlockChainInfo};
pub use verification::QueueInfo as BlockQueueInfo;

// meepo use
use std::io::prelude::*;
use flate2::Compression;
use flate2::write::{ZlibEncoder, ZlibDecoder};
use rlp::{Decodable, DecoderError, Encodable, RlpStream};
use std::thread;
use block::{ExecutedBlock};
use state::{CleanupMode};
use trace::{Tracer, VMTracer};
use rustc_hex::{ToHex};
use std::net::{TcpListener, TcpStream};
//use std::sync::mpsc;
// server 
use client::futures::{
    future,
    future::{Loop},
    sync::{mpsc, oneshot},
    Async, Future, Sink, Stream,
};
use client::hyper::{
    header::{HeaderMap, HeaderValue, IntoHeaderName},
    StatusCode,
};
use std::{
    cmp::min,
    fmt, io,
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc::RecvTimeoutError,
    },
};
use client::tokio::{util::FutureExt, runtime::current_thread::Runtime, timer::Delay};
use client::hyper::{service::Service};
use std::{io::Read, net::SocketAddr};



pub struct Meepo {
    send_event: H256,
    pub state_root: Arc<std::sync::Mutex<H256>>,
    pub parent_state_root: Arc<std::sync::Mutex<H256>>,
    engine: Arc<dyn EthEngine>,
    factories: Factories,

    shard_id: u64,
    shard_port: u64,
    shards: Vec<String>,
    //tcp_tx: Arc<std::sync::Mutex<mpsc::Sender<Bytes>>>,
    //tcp_rx: Arc<std::sync::Mutex<mpsc::Receiver<Bytes>>>,

    // debug
    new_time: Instant,

    server: Handle,
}

pub struct EpochPacket{
    cross_calls: Vec<CrossCall>,
    finished: bool,
    error_tx_hashes: Vec<H256>,
    shard_id: u64,
    block_number: u64,
    epoch_number: u64,
}
impl Encodable for EpochPacket {
    fn rlp_append(&self, s: &mut RlpStream) {
        s.begin_list(6);
        s.append_list(&self.cross_calls);
        s.append(&self.finished);
        s.append_list(&self.error_tx_hashes);
        s.append(&self.shard_id);
        s.append(&self.block_number);
        s.append(&self.epoch_number);
    }
}

impl Decodable for EpochPacket {
    fn decode(rlp: &Rlp) -> Result<Self, DecoderError> {
        let res = EpochPacket {
            cross_calls: rlp.list_at(0)?,
            finished: rlp.val_at(1)?,
            error_tx_hashes: rlp.list_at(2)?,
            shard_id: rlp.val_at(3)?,
            block_number: rlp.val_at(4)?,
            epoch_number: rlp.val_at(5)?,
        };
        Ok(res)
    }
}


impl EpochPacket {
    fn to_zip(&self) -> Bytes {
        let mut rlp = RlpStream::new();
        rlp.append(self);
        let rlp_bytes = rlp.out();
        let mut e = ZlibEncoder::new(Vec::new(), Compression::default());
        e.write_all(&rlp_bytes).unwrap();
        e.finish().unwrap()
    }

    
    fn from_zip(zip_bytes: &Bytes) -> Self {
        let mut writer = Vec::new();
        let mut z = ZlibDecoder::new(writer);
        z.write_all(&zip_bytes[..]).unwrap();
        let writer = z.finish().unwrap();
        let rlp = Rlp::new(&writer);
        EpochPacket::decode(&rlp).unwrap()
    }
    
    
}


#[derive(Debug)]
pub struct CrossCall {
    tx_hash: H256,
    from: Address,
    to: Address,
    data: Bytes,
    is_partial: bool,
}

impl Encodable for CrossCall {
    fn rlp_append(&self, s: &mut RlpStream) {
        s.begin_list(5);
        s.append(&self.tx_hash);
        s.append(&self.from);
        s.append(&self.to);
        s.append(&self.data);
        s.append(&self.is_partial);
    }
}

impl Decodable for CrossCall {
    fn decode(rlp: &Rlp) -> Result<Self, DecoderError> {
        let res = CrossCall {
            tx_hash: rlp.val_at(0)?,
            from: rlp.val_at(1)?,
            to: rlp.val_at(2)?,
            data: rlp.val_at(3)?,
            is_partial: rlp.val_at(4)?,
        };

        Ok(res)
    }
}



impl Meepo {
    pub fn new(state_root_: H256, engine: Arc<dyn EthEngine>, factories: Factories) -> Meepo {
        //let (tcp_tx,tcp_rx) = mpsc::channel();
        let server = TestServer::run();

        let meepo = Meepo {
            send_event: H256::from_str("2b13ad5439aaffad595fea940bc5e5911043ebcdfb992ab22179a5de3c6f23e9").unwrap(),
            state_root: Arc::new(std::sync::Mutex::new(state_root_)),
            parent_state_root: Arc::new(std::sync::Mutex::new(state_root_)),
            engine: engine.clone(),
            factories: factories.clone(),

            shard_id: 0,
            shard_port: 10086,
            shards: Vec::<String>::new(),

            new_time: Instant::now(),

            //tcp_tx: Arc::new(std::sync::Mutex::new(tcp_tx)),
            //tcp_rx: Arc::new(std::sync::Mutex::new(tcp_rx)),
            server: server,
        };
        //meepo.start_tcp_server().unwrap();
        info!("newed!");
        //info!("server {}", server.addr());
        meepo
    }

    fn handle_client(stream: TcpStream) {
        info!("handle_client");
        // ...
    }
    /*
    fn start_tcp_server(&self) -> std::io::Result<()>{
        let listener = TcpListener::bind("0.0.0.0:9000")?;
        info!("listening");
        let tx = self.tcp_tx.clone().lock().unwrap();
        thread::spawn(move || {
            //let mut thread_vec: Vec<thread::JoinHandle<()>> = Vec::new();
            for stream in listener.incoming() {
                let stream = stream.unwrap();
                //let handle = thread::spawn(|| {
                    tx.send("aaa".as_bytes().to_vec()).unwrap();
                //});
                //thread_vec.push(handle);
            }
        });
        Ok(())
    }
    */

    fn make_tx(from: Address, to:Address, data: Bytes) -> SignedTransaction {
        TypedTransaction::Legacy(transaction::Transaction {
            nonce: U256::from(0),
            action: Action::Call(to),
            gas: U256::from(50_000_000),
            gas_price: U256::default(),
            value: U256::from(0),
            data: data,
        })
        .fake_sign(from)
    }

    fn exec_on_state(&self, state: &mut State<StateDB>, env_info: &EnvInfo, from: Address, to:Address, data: Bytes) -> Result<Executed, ExecutionError> {
        let machine = self.engine.machine();
        let schedule = machine.schedule(env_info.number);
        let mut evm = Executive::new(state, env_info, machine, &schedule);
        let t = Meepo::make_tx(from, to, data);
        let opts = TransactOptions::with_no_tracing().dont_check_nonce();
        evm.transact(&t, opts)
    }

    pub fn start(&self, receipts: &Vec<TypedReceipt> , state_db: &mut StateDB, header: &Header, transactions: &Vec<SignedTransaction>,  env_info: &EnvInfo, batch: &mut DBTransaction) -> Result<(), ::error::Error> {
        std::thread::sleep(Duration::from_millis(500));
        self.start_new(receipts, state_db, header, transactions, env_info, batch)
    }


    pub fn start_new(&self, receipts: &Vec<TypedReceipt> , state_db: &mut StateDB, header: &Header, transactions: &Vec<SignedTransaction>,  env_info: &EnvInfo, batch: &mut DBTransaction) -> Result<(), ::error::Error> {
        let start_time = Instant::now();
        let length = receipts.len();
        warn!("Meepo begin {} time={:?} ms", length, self.new_time.elapsed().as_millis());

        let mut epoch_packet = EpochPacket {
            cross_calls: Vec::<CrossCall>::new(),
            finished: false,
            error_tx_hashes: Vec::<H256>::new(),
            shard_id: 0,
            block_number: 0,
            epoch_number: 0,
        };

        let tx_len = transactions.len();    
        for tx_index in 0..tx_len {
            let receipt = &receipts[tx_index];
            let all_logs = &receipt.logs;
            for one_log in all_logs.into_iter() {
                if one_log.topics[0] == self.send_event  {
                    let cross_call =  CrossCall {
                        tx_hash: transactions[tx_index].hash(),
                        from: one_log.address,
                        to: Address::from_slice(&one_log.data[44..64]),
                        data: one_log.data[64..].to_vec(),
                        is_partial: false,
                    };
                    epoch_packet.cross_calls.push(cross_call);
                }
            }
        }

        let epoch_packet_bytes = epoch_packet.to_zip();
        let time1 = start_time.elapsed();

        warn!("zip={}, time1={:?}", epoch_packet_bytes.len(), time1);

        let start_time = Instant::now();
        let decoded_epoch_packet = EpochPacket::from_zip(&epoch_packet_bytes);
        let time2 = start_time.elapsed();
        warn!("crosscall.len={}, time2={:?}",decoded_epoch_packet.cross_calls.len(), time2);

        let start_time = Instant::now();
        let mut state = State::from_existing(
            state_db.boxed_clone_canon(&header.hash()),
            header.state_root.clone(),
            self.engine.account_start_nonce(header.number()),
            self.factories.clone(),
        ).unwrap();
        for cross_call in decoded_epoch_packet.cross_calls.into_iter() {
            let res1 = self.exec_on_state(&mut state, env_info, cross_call.from, cross_call.to, cross_call.data);
        }
        state.commit()?;
        //info!("state root 3 now {:} balance {}", state.root(), state.balance(header.author()).unwrap());
        state.db.journal_under(batch, header.number(), &header.hash()).expect("DB commit failed");
        let time3 = start_time.elapsed();
        warn!("block={}, stat_root={}, time3={:?}", header.state_root(), state.root(), time3);
        

        let temp_state_root = self.state_root.clone();
        let mut meepo_state_root = temp_state_root.lock().unwrap();
        *meepo_state_root = state.root().clone();


        Ok(())
    }

    pub fn start_old(&self, receipts: &Vec<TypedReceipt> , state_db: &mut StateDB, header: &Header, transactions: &Vec<SignedTransaction>,  env_info: &EnvInfo, batch: &mut DBTransaction) -> Result<(), ::error::Error> {
        let start_time = Instant::now();
        let length = receipts.len();
        warn!("Meepo begin {}", length);
        let mut cross_call_rlp = RlpStream::new_list(length);
        let tx_len = transactions.len();
    
        for tx_index in 0..tx_len {
            let receipt = &receipts[tx_index];
            let all_logs = &receipt.logs;
            for one_log in all_logs.into_iter() {
                if one_log.topics[0] == self.send_event  {
                    //info!("receipt {:?}", receipt);
                    let cross_call =  CrossCall {
                        tx_hash: transactions[tx_index].hash(),
                        from: one_log.address,
                        to: Address::from_slice(&one_log.data[44..64]),
                        data: one_log.data[64..].to_vec(),
                        is_partial: false,
                    };
                    cross_call_rlp.append(&cross_call);
                }
            }
            cross_call_rlp.append(&self.send_event);
        }
        let cross_call_bytes = cross_call_rlp.out();
        let time1 = start_time.elapsed();
        let mut e = ZlibEncoder::new(Vec::new(), Compression::default());
        //e.write(cross_call_bytes)
        e.write_all(&cross_call_bytes).ok();
        let compressed_bytes = e.finish()?;
        let time2 = start_time.elapsed();
        warn!("pre={}, zip={}, time1={:?}, time2={:?}",cross_call_bytes.len(), compressed_bytes.len(), time1, time2);

        let mut state = State::from_existing(
            state_db.boxed_clone_canon(&header.hash()),
            header.state_root.clone(),
            self.engine.account_start_nonce(header.number()),
            self.factories.clone(),
        ).unwrap();
        info!("state root 1 now {:} balance {}", state.root(), state.balance(header.author()).unwrap());
        state.add_balance(header.author() , &U256::from(10086), CleanupMode::NoEmpty).ok();
        state.commit()?;
        info!("state root 2 now {:} balance {}", state.root(), state.balance(header.author()).unwrap());

        let from=Address::default();
        let to =Address::from_str("c0AAe1EdD7A76C8cf99E5bA3cA69599eD29540ea").unwrap();
        let data1 = FromHex::from_hex("771602f700000000000000000000000000000000000000000000000000000000000100860000000000000000000000000000000000000000000000000000000000000001").unwrap();
        let res1 = self.exec_on_state(&mut state, env_info, from, to, data1);

        let data2 = FromHex::from_hex("9cc7f7080000000000000000000000000000000000000000000000000000000000010086").unwrap();
        let res2 = self.exec_on_state(&mut state, env_info, from, to, data2);

        info!("res1 {:?}, res2 {:?}", res1, res2);


        state.commit()?;
        info!("state root 3 now {:} balance {}", state.root(), state.balance(header.author()).unwrap());
        state.db.journal_under(batch, header.number(), &header.hash()).expect("DB commit failed");

        let temp_state_root = self.state_root.clone();
        let mut meepo_state_root = temp_state_root.lock().unwrap();
        *meepo_state_root = state.root().clone();

        Ok(())
        
    }
}





struct TestServer;

impl Service for TestServer {
    type ReqBody = hyper::Body;
    type ResBody = hyper::Body;
    type Error = hyper::Error;
    type Future = Box<
        dyn Future<Item = hyper::Response<Self::ResBody>, Error = Self::Error> + Send + 'static,
    >;

    fn call(&mut self, req: hyper::Request<hyper::Body>) -> Self::Future {
        info!("call ???");
        match req.uri().path() {
            "/" => {
                let body = req.uri().query().unwrap_or("").to_string();
                let res = hyper::Response::new(body.into());
                println!("require! {}", req.uri());
                std::thread::sleep(Duration::from_millis(10000));
                println!("response! {}", req.uri());
                Box::new(future::ok(res))
            }
        }
    }
}

impl TestServer {
    fn run() -> Handle {
        let (tx_start, rx_start) = std::sync::mpsc::sync_channel(1);
        let (tx_end, rx_end) = oneshot::channel();
        let rx_end_fut = rx_end.map(|_| ()).map_err(|_| ());
        thread::spawn(move || {
            const ADDRESS: &str = "0.0.0.0:9999";
            let addr = ADDRESS.parse().unwrap();

            let server = hyper::server::Server::bind(&addr)
                .serve(|| future::ok::<_, hyper::Error>(TestServer));

            tx_start.send(server.local_addr()).unwrap_or(());

            tokio::run(
                server
                    .with_graceful_shutdown(rx_end_fut)
                    .map_err(|e| panic!("server error: {}", e)),
            );
        });

        Handle(rx_start.recv().unwrap(), Some(tx_end))
    }
}

struct Handle(SocketAddr, Option<oneshot::Sender<()>>);

impl Handle {
    fn addr(&self) -> SocketAddr {
        self.0
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.1.take().unwrap().send(()).unwrap();
    }
}

