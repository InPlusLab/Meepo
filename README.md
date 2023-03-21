# Meepo (Meepo-OpenEthereum)

Meepo implementation on [OpenEthereum](https://github.com/openethereum/openethereum).

**MEEPO** = **M**ultiple **E**xecution **E**nvironments **P**er **O**rganization. 
(It is also a legendary hero who can split himself, each one acting independently but relying on together, just like the shards.)

Since Meepo-Geth (Golang) is unable to be open-source because of the copyright, our community provides another implementation (Rust) based on the published paper.

## Build and Run (Linux)

1. Install Rust v1.51
```bash
$ curl https://sh.rustup.rs -sSf | sh
$ rustup override set 1.51
```


2. Build Meepo-OpenEthereum
```bash
$ git clone http://github.com/InPlusLab/Meepo/
$ cd Meepo/build/
$ ./debug.sh # or ./release.sh
```

3. Run Node0 (Shard0)
```bash
$ cd Meepo/build/
$ ./initrun0.sh
```

4. Run Node1 (Shard1)
```bash
$ cd Meepo/build/
$ ./initrun1.sh
```

5. Try anything you want via Web3.js or other Ethereum toolchain. As for benchmark, please see the following.

## Benchmark

#### More benchmark scripts, logs, and analytic results can be found in this repo: [MeepoBenchmark](https://github.com/tczpl/MeepoBenchmark/).

![image](https://github.com/tczpl/MeepoBenchmark/raw/main/png/rq1-1_tps.png)

(32x4=128 machines in maximum, each machine is equipped with 4 vCPU, 32 GiB memory, and 894 GiB NVME disk, 102400 accounts per shard, 100000000+ transactions to an ERC20-like contract.)
