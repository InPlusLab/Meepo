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
    log_entry::{LocalizedLogEntry, LogEntry},
    receipt::{LocalizedReceipt, TypedReceipt, TransactionOutcome},
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
use std::sync::mpsc;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub struct MeepoConfiguration {
    pub shard_id: u64,
    pub shard_port: u64,
    pub shards: Vec<String>,
}

pub struct Meepo {
    cross_call_topic: H256,
    partial_cross_call_topic: H256,

    pub state_root: Arc<std::sync::Mutex<H256>>,
    engine: Arc<dyn EthEngine>,
    factories: Factories,

    shard_id: u64,
    shard_port: u64,
    shards: Vec<String>,
    epoch_packet_channel_sender: Arc<std::sync::Mutex<mpsc::Sender<EpochPacket>>>,
    epoch_packet_channel_receiver: Arc<std::sync::Mutex<mpsc::Receiver<EpochPacket>>>,

    // debug
    new_time: Instant,
}

#[derive(Debug)]
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


#[derive(Debug)]
pub struct CrossReturn {
    this_error_tx: Vec<H256>,
    epoch_number: u64,
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
    pub fn new(state_root_: H256, engine: Arc<dyn EthEngine>, factories: Factories, meepo_conf: MeepoConfiguration) -> Meepo {
        let (tx,rx) = mpsc::channel();
        let mut meepo = Meepo {
            cross_call_topic: H256::from_str("af9b76a75fc9f742ddc3eae6f36524f8446af407457f59c8d4163ecdad14cabd").unwrap(),
            partial_cross_call_topic: H256::from_str("077de832cc321c677c221ab39a84a5fde08f7b2d07c09a152fcb5e2087c53c37").unwrap(),

            state_root: Arc::new(std::sync::Mutex::new(state_root_)),
            engine: engine.clone(),
            factories: factories.clone(),

            shard_id: meepo_conf.shard_id,
            shard_port: meepo_conf.shard_port,
            shards: meepo_conf.shards,

            new_time: Instant::now(),

            epoch_packet_channel_sender: Arc::new(std::sync::Mutex::new(tx)),
            epoch_packet_channel_receiver: Arc::new(std::sync::Mutex::new(rx)),
        };
        meepo.start_tcp_server().unwrap();
        meepo
    }

    fn handle_client(mut stream: TcpStream, sender: Arc<std::sync::Mutex<mpsc::Sender<EpochPacket>>>) {
        //info!("handle_client");
        let mut buffer = Vec::new();
        stream.read_to_end(&mut buffer).unwrap();
        //info!("buffer {}", buffer.len());
        let mut receive_bytes = Bytes::new();
        receive_bytes.extend_from_slice(&buffer);
        //info!("receive_bytes {}", receive_bytes.len());

        let epoch_packet = EpochPacket::from_zip(&receive_bytes);

        sender.lock().unwrap().send(epoch_packet);
        stream.write(b"ok").unwrap();
    }
    
    fn start_tcp_server(&self) -> std::io::Result<()>{
        let to_listen = format!("0.0.0.0:{}", self.shard_port);
        let listener = TcpListener::bind(to_listen)?;
        info!("listening");
        let sender = self.epoch_packet_channel_sender.clone();
        thread::spawn(move || {
            for stream in listener.incoming() {
                match stream {
                    Ok(stream) => {
                        let sender = sender.clone();
                        thread::spawn(move|| {
                            Meepo::handle_client(stream, sender);
                        });
                    }
                    Err(e) => { 
                        println!("error! {:?}", e);
                    }
                }
            }        
        });
        Ok(())
    }

    

    fn make_tx(from: Address, to:Address, data: Bytes) -> SignedTransaction {
        TypedTransaction::Legacy(transaction::Transaction {
            nonce: U256::from(0),
            action: Action::Call(to),
            gas: U256::from(100_000),
            gas_price: U256::default(),
            value: U256::from(0),
            data: data,
        })
        .fake_sign(from)
    }

    fn exec_on_state(&self, state: &mut State<StateDB>, env_info: &EnvInfo, from: Address, to:Address, data: Bytes) -> Result<Executed, ExecutionError> {
        //info!("exec_on_state from={} to={} data={}", &from, &to, &data.to_hex());
        let machine = self.engine.machine();
        let schedule = machine.schedule(env_info.number);
        let mut evm = Executive::new(state, env_info, machine, &schedule);
        let t = Meepo::make_tx(from, to, data);
        let opts = TransactOptions::with_no_tracing().dont_check_nonce();
        evm.transact(&t, opts)
    }


    fn exec_tx_on_state(&self, state: &mut State<StateDB>, env_info: &EnvInfo, transaction: &SignedTransaction) -> Result<Executed, ExecutionError> {
        let machine = self.engine.machine();
        let schedule = machine.schedule(env_info.number);
        let mut evm = Executive::new(state, env_info, machine, &schedule);
        let opts = TransactOptions::with_no_tracing().dont_check_nonce();
        evm.transact(transaction, opts)
    }

    pub fn start(&self, receipts: &Vec<TypedReceipt> , state_db: &mut StateDB, header: &Header, transactions: &Vec<SignedTransaction>,  env_info: &EnvInfo, batch: &mut DBTransaction) -> Result<(), ::error::Error> {
        //std::thread::sleep(Duration::from_millis(500));
        //self.start_old(receipts, state_db, header, transactions, env_info, batch)
        self.execute_cross(receipts, state_db, header, transactions, env_info, batch)
    }

    pub fn execute_cross(&self, receipts: &Vec<TypedReceipt> , state_db: &mut StateDB, header: &Header, transactions: &Vec<SignedTransaction>,  env_info: &EnvInfo, batch: &mut DBTransaction) -> Result<(), ::error::Error> {
        let start_time = Instant::now();

        let mut total_error_txs = HashSet::<H256>::new();

        let error_tx_hashes = self.translate_and_send(receipts, header, transactions, 0).unwrap();
        for error_tx_hash in error_tx_hashes.into_iter() {
            total_error_txs.insert(error_tx_hash);
        }

        let mut is_replay = false;
        let mut epoch_number = 0;

        loop {
            let mut begin_state_root = header.state_root.clone();
            if is_replay {
                let temp_state_root = self.state_root.clone();
                let mut meepo_state_root = temp_state_root.lock().unwrap();
                begin_state_root = meepo_state_root.clone();
            }

            let mut state = State::from_existing(
                state_db.boxed_clone_canon(&header.hash()),
                begin_state_root.clone(),
                self.engine.account_start_nonce(header.number()),
                self.factories.clone(),
            ).unwrap();

            if is_replay {
                self.replay_and_send(&mut state, header, transactions, env_info, epoch_number, &total_error_txs);
                // replay epoch
            }
    
            let ret = self.withdraw_and_execute(&mut state, header, env_info, epoch_number)?;
            if ret.this_error_tx.len() > 0 {
                // replay epoch
                info!("replay epoch!");
                is_replay = true;
                for error_tx_hash in ret.this_error_tx.into_iter() {
                    //info!("insert {}", &error_tx_hash);
                    total_error_txs.insert(error_tx_hash);
                }
                epoch_number = ret.epoch_number;
                continue
            }

            state.commit()?;
    
            self.state_query(&mut state, env_info);
            
            state.db.journal_under(batch, header.number(), &header.hash()).expect("DB commit failed");
            let cross_time = start_time.elapsed();
            warn!("block={}, stat_root={}, cross_time={:?}", header.state_root(), state.root(), cross_time);
            
            let temp_state_root = self.state_root.clone();
            let mut meepo_state_root = temp_state_root.lock().unwrap();
            *meepo_state_root = state.root().clone();
            break;
        }
        Ok(())
    }


    pub fn state_query(&self, state: &mut State<StateDB>, env_info: &EnvInfo) {
        let from=Address::default();
        let to =Address::from_str("c0AAe1EdD7A76C8cf99E5bA3cA69599eD29540ea").unwrap();
        let data2 = FromHex::from_hex("9cc7f7080000000000000000000000000000000000000000000000000000000000000000").unwrap();
        let res2 = self.exec_on_state(state, env_info, from, to, data2);
        info!("balanceOf[0]  {:?}", res2.unwrap().output.to_hex());
        let data2 = FromHex::from_hex("9cc7f7080000000000000000000000000000000000000000000000000000000000000001").unwrap();
        let res2 = self.exec_on_state(state, env_info, from, to, data2);
        info!("balanceOf[1]  {:?}", res2.unwrap().output.to_hex());
        let data2 = FromHex::from_hex("9cc7f7080000000000000000000000000000000000000000000000000000000000000002").unwrap();
        let res2 = self.exec_on_state(state, env_info, from, to, data2);
        info!("balanceOf[2]  {:?}", res2.unwrap().output.to_hex());
        let data2 = FromHex::from_hex("bd94dbae0000000000000000000000000000000000000000000000000000000000000001").unwrap();
        let res2 = self.exec_on_state(state, env_info, from, to, data2);
        info!("billOf[1]  {:?}", res2.unwrap().output.to_hex());
    }



    pub fn withdraw_and_execute(&self, state: &mut State<StateDB>, header: &Header, env_info: &EnvInfo, epoch_number: u64) -> Result<CrossReturn, ::error::Error> {
        let mut all_finished = true;
        let start_time = Instant::now();

        info!("withdraw_and_execute");


        let mut receive_epoch_packet_vec = Vec::<EpochPacket>::with_capacity(self.shards.len());
        let mut send_epoch_packet_vec = Vec::<EpochPacket>::with_capacity(self.shards.len());
        for i in 0..self.shards.len() {
            receive_epoch_packet_vec.push(EpochPacket{
                cross_calls: Vec::<CrossCall>::new(),
                finished: false,
                error_tx_hashes: Vec::<H256>::new(),
                shard_id: 0,
                block_number: 0,
                epoch_number: 0,
            });
            send_epoch_packet_vec.push(EpochPacket {
                cross_calls: Vec::<CrossCall>::new(),
                finished: false,
                error_tx_hashes: Vec::<H256>::new(),
                shard_id: self.shard_id,
                block_number: header.number().clone(),
                epoch_number: epoch_number+1,
            });
        }
        
        let mut need_count = self.shards.len();
        let mut next_epoch_packet_vec = Vec::<EpochPacket>::new();
        loop {
            let decoded_epoch_packet = self.epoch_packet_channel_receiver.clone().lock().unwrap().recv().unwrap();
            let packet_epoch_number = decoded_epoch_packet.epoch_number.clone();
            let packet_block_number = decoded_epoch_packet.block_number.clone();
            if packet_epoch_number == epoch_number && packet_block_number == header.number() {
                let shard_id = decoded_epoch_packet.shard_id.clone() as usize;
                need_count -= 1;
                info!("receive {} need {}/{}", &shard_id, &need_count, receive_epoch_packet_vec.len());
                receive_epoch_packet_vec[shard_id] = decoded_epoch_packet;
                if need_count == 0 {
                    break;
                }
            } else {
                next_epoch_packet_vec.push(decoded_epoch_packet);
            }
        }
        while !next_epoch_packet_vec.is_empty() {
            let epoch_packet = next_epoch_packet_vec.pop().unwrap();
            let sender = self.epoch_packet_channel_sender.clone();
            sender.lock().unwrap().send(epoch_packet);
        }

        let withdraw_time = start_time.elapsed();
        let start_time = Instant::now();

        // check replay epoch
        let mut this_error_tx = Vec::<H256>::new();
        for epoch_packet in receive_epoch_packet_vec.iter() {
            for error_tx_hash in epoch_packet.error_tx_hashes.iter() {
                this_error_tx.push(error_tx_hash.clone());
            }
        }
        if this_error_tx.len() > 0 {
            Ok(
                CrossReturn {
                    this_error_tx: this_error_tx,
                    epoch_number: epoch_number,
                }
            )
        }
        else {
            // execute
            let mut local_finished = true;
            let mut partial_vec = Vec::new();
            let mut partial_stack = HashMap::<H256, Vec<CrossCall> >::new();
            let mut error_tx_hashes = Vec::<H256>::new();

            for epoch_packet in receive_epoch_packet_vec.into_iter() {
                if !epoch_packet.finished {
                    all_finished = false;
                }
                for cross_call in epoch_packet.cross_calls.into_iter() {
                    //info!("cross_call {} {} {:?}", &cross_call.from, &cross_call.to, &cross_call.data.to_hex());
                    if cross_call.is_partial {
                        let partials = partial_stack.get_mut(&cross_call.tx_hash);
                        match partials {
                            Some(vector) => {
                                vector.push(cross_call)
                            },
                            None => {
                                partial_vec.push(cross_call.tx_hash.clone());
                                let mut vector = Vec::new();
                                let tx_hash = cross_call.tx_hash.clone();
                                vector.push(cross_call);
                                partial_stack.insert(tx_hash, vector);
                            }
                        }
                        continue
                    }
                    let res = self.exec_on_state(state, env_info, cross_call.from, cross_call.to, cross_call.data).unwrap();
                    self.parse_logs(&res.logs, &mut send_epoch_packet_vec, &mut local_finished, cross_call.tx_hash);
                    match res.exception {
                        Some(vmerr) => {
                            error_tx_hashes.push(cross_call.tx_hash);
                        }
                        None => {}
                    }
                    //info!("finish execute_cross res1 {:?}", res1);
                }
            }


            for tx_hash in partial_vec.iter() {
                let partials = partial_stack.get(tx_hash).unwrap();
                let mut msg0data = partials[0].data.clone();
                for i in 1..partials.len() {
                    for j in 0..partials[i].data.len() {
                        if partials[i].data[j] > 0 {
                            msg0data[j] = partials[i].data[j]
                        }
                    }
                }

                let res = self.exec_on_state(state, env_info, partials[0].from, partials[0].to, msg0data).unwrap();
                self.parse_logs(&res.logs, &mut send_epoch_packet_vec, &mut local_finished, partials[0].tx_hash);
                match res.exception {
                    Some(vmerr) => {
                        error_tx_hashes.push(partials[0].tx_hash);
                    }
                    None => {}
                }
            }

            let execute_time = start_time.elapsed();
            info!("withdraw_time={:?} execute_time={:?}", withdraw_time, execute_time);

            if !all_finished {
                info!("not all finished");
                self.send(&mut send_epoch_packet_vec, local_finished, error_tx_hashes);
                self.withdraw_and_execute(state, header, env_info, epoch_number+1)
            }
            else {
                Ok (
                    CrossReturn {
                        this_error_tx: Vec::<H256>::new(),
                        epoch_number: epoch_number,
                    }
                )
            }
        }


    }

    pub fn parse_logs(&self, all_logs: &Vec<LogEntry>, epoch_packet_vec: &mut Vec<EpochPacket> , local_finished: &mut bool, tx_hash: H256){
        //info!("parse logs len={}", all_logs.len());
        for one_log in all_logs.into_iter() {
            if one_log.topics[1] == self.cross_call_topic  {
                //info!("one_log {}", &one_log.data.to_hex());
                *local_finished = false;
                
                let data = [&one_log.data[64..68], &one_log.data[96..]].concat();
                //info!("data {}", &data.to_vec().to_hex());
                let cross_call =  CrossCall {
                    tx_hash: tx_hash,
                    from: one_log.address,
                    to: Address::from_slice(&one_log.data[44..64]),
                    data: data,
                    is_partial: false,
                };
                let shard_id = usize::from_str_radix(&one_log.data[0..32].to_hex(), 16).unwrap();
                //info!("shard_id {}", shard_id);
                epoch_packet_vec[shard_id].cross_calls.push(cross_call);
            }
            if one_log.topics[1] == self.partial_cross_call_topic  {
                //info!("one_log {}", &one_log.data.to_hex());
                *local_finished = false;
                
                let data = [&one_log.data[64..68], &one_log.data[96..]].concat();
                //info!("data {}", &data.to_vec().to_hex());
                let cross_call =  CrossCall {
                    tx_hash: tx_hash,
                    from: one_log.address,
                    to: Address::from_slice(&one_log.data[44..64]),
                    data: data,
                    is_partial: true,
                };
                let shard_id = usize::from_str_radix(&one_log.data[0..32].to_hex(), 16).unwrap();
                //info!("shard_id {}", shard_id);
                epoch_packet_vec[shard_id].cross_calls.push(cross_call);
            }
        }
    }


    pub fn replay_and_send(&self, state: &mut State<StateDB>, header: &Header, transactions: &Vec<SignedTransaction>, env_info: &EnvInfo, epoch_number: u64, total_error_txs: &HashSet::<H256> ) -> Result<(), ::error::Error> {
        let start_time = Instant::now();
        let mut local_finished = true;

        let mut send_epoch_packet_vec = Vec::<EpochPacket>::new();
        for i in 0..self.shards.len() {
            send_epoch_packet_vec.push(EpochPacket {
                cross_calls: Vec::<CrossCall>::new(),
                finished: false,
                error_tx_hashes: Vec::<H256>::new(),
                shard_id: self.shard_id,
                block_number: header.number().clone(),
                epoch_number: epoch_number,
            });
        }

        let mut error_tx_hashes = Vec::<H256>::new();
        for transaction in transactions.iter() {
            let tx_hash =  transaction.hash().clone();
            if total_error_txs.contains(&tx_hash) {
                //info!("skip {}", &tx_hash);
                state.inc_nonce(&transaction.sender()).unwrap();
                continue;
            }
            
            let res = self.exec_tx_on_state(state, env_info, transaction).unwrap();
            self.parse_logs(&res.logs, &mut send_epoch_packet_vec, &mut local_finished, tx_hash);
            match res.exception {
                Some(vmerr) => {
                    error_tx_hashes.push(tx_hash);
                }
                None => {}
            }
        }

        let replay_time = start_time.elapsed();
        info!("replay_time={:?}", replay_time);

        self.send(&mut send_epoch_packet_vec, local_finished, error_tx_hashes);
        Ok(())
    }


    pub fn translate_and_send(&self, receipts: &Vec<TypedReceipt> , header: &Header, transactions: &Vec<SignedTransaction>, epoch_number: u64) -> Result<Vec<H256>, ::error::Error> {
        let start_time = Instant::now();
        let length = receipts.len();
        warn!("Meepo begin {} time={:?} ms", length, self.new_time.elapsed().as_millis());

        let mut local_finished = true;

        let mut send_epoch_packet_vec = Vec::<EpochPacket>::new();
        for i in 0..self.shards.len() {
            send_epoch_packet_vec.push(EpochPacket {
                cross_calls: Vec::<CrossCall>::new(),
                finished: false,
                error_tx_hashes: Vec::<H256>::new(),
                shard_id: self.shard_id,
                block_number: header.number().clone(),
                epoch_number: epoch_number,
            });
        }

        let tx_len = transactions.len();  
        let mut error_tx_hashes = Vec::<H256>::new();
        for tx_index in 0..tx_len {
            let receipt = &receipts[tx_index];
            let tx_hash =  transactions[tx_index].hash();
            //info!("tx receipt {:?}", receipt);
            self.parse_logs(&receipt.logs, &mut send_epoch_packet_vec, &mut local_finished, tx_hash.clone());
            match receipt.outcome {
                TransactionOutcome::StatusCode(status) =>  {
                    //info!("status {}", status);
                    if status == 0 {
                        //info!("init err");
                        error_tx_hashes.push(tx_hash);
                    }
                }
                _ => {},
            }
        }

        self.send(&mut send_epoch_packet_vec, local_finished, Vec::<H256>::new());

        Ok(error_tx_hashes)
    }

    pub fn send(&self, send_epoch_packet_vec: &mut Vec<EpochPacket>, local_finished: bool, error_tx_hashes: Vec<H256> ) {
        for shard_id in 0..self.shards.len() {
            let shard_url = self.shards[shard_id].clone();
            send_epoch_packet_vec[shard_id].finished = local_finished;
            send_epoch_packet_vec[shard_id].error_tx_hashes = error_tx_hashes.to_vec();

            let epoch_packet_bytes = send_epoch_packet_vec[shard_id].to_zip();
            //info!("zipped {} {}", &shard_id, &epoch_packet_bytes.len());

            thread::spawn(move || {
                //info!("sending");
                let mut stream = TcpStream::connect(&shard_url).unwrap();
                stream.write(&epoch_packet_bytes).unwrap();
                //info!("send!!! {}", shard_url);
            });
        }
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
                if one_log.topics[1] == self.cross_call_topic  {
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



