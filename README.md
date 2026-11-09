# CP372 Assignment 2 - Reliable Data Transfer over UDP

Implementations of Stop-and-Wait and Go-Back-N (GBN) reliable data transfer protocols built on top of Python UDP sockets (`socket.SOCK_DGRAM`). The protocols handle packet loss, out-of-order delivery, timeouts, retransmissions, cumulative ACKs, and checksum integrity detection.

**Course:** CP372 – Computer Networks (Fall 2026)  
**Assignment:** Assignment 2 – Reliable Data Transfer over UDP  
**Group:** Group #6  
**Authors / Members:** Mike Bangar, Joshua Goudy, Anson Huang, Ali Lopez, Mahi Srivastav  

## Requirements

* Python 3.x (standard library only; no third-party networking dependencies)
* Standard modules used: `socket`, `struct`, `zlib`, `time`, `os`, `random`, `argparse`, `subprocess`

## File Structure

```
CP372_Assignment2/
├── sender_stopwait.py     # Stop-and-Wait UDP Sender
├── receiver_stopwait.py   # Stop-and-Wait UDP Receiver (with simulated packet loss)
├── sender_gbn.py          # Go-Back-N UDP Sender (sliding window & cumulative ACKs)
├── receiver_gbn.py        # Go-Back-N UDP Receiver (out-of-order discard & cumulative ACKs)
├── run_experiments.py     # Automated experiment test suite for Part D benchmarking
├── experimental_results.csv # Generated performance dataset across file sizes & loss rates
├── README.md              # Project documentation and execution instructions
└── report.pdf             # Formal assignment evaluation report and analysis
```

## Packet Format Header Design

All four programs share a unified 15-byte fixed header structure packed via `struct` (`!BIiHI`), followed by the payload:

| Field | Type | Size | Description |
| ----- | ----- | ----- | ----- |
| **Type** | 1 Byte (`B`) | 1B | Packet type: `1=UPLOAD`, `2=DATA`, `3=ACK`, `4=FIN` |
| **Sequence Number** | 4 Bytes Unsigned (`I`) | 4B | Packet identifier (`UPLOAD=0`, `DATA=1..N`, `FIN=N+1`) |
| **ACK Number** | 4 Bytes Signed (`i`) | 4B | Stop-and-Wait ACK seq or GBN cumulative ACK seq (`-1` if none) |
| **Length** | 2 Bytes Unsigned (`H`) | 2B | Length of payload bytes |
| **Checksum** | 4 Bytes Unsigned (`I`) | 4B | CRC32 calculated over header fields + payload |

## How to Run the Programs

### 1. Stop-and-Wait Protocol (Parts A & B)

Open two terminals in the repository directory.

**Start the Receiver:**

```bash
# Basic receiver listening on port 9000
python3 receiver_stopwait.py --port 9000 --outdir received

# Receiver with 10% simulated packet loss (Part B)
python3 receiver_stopwait.py --port 9000 --loss 0.1 --outdir received
```

**Run the Sender:**

```bash
# Send an existing file
python3 sender_stopwait.py --host 127.0.0.1 --port 9000 --file test.txt

# Send auto-generated dummy data (e.g., 100 KB)
python3 sender_stopwait.py --host 127.0.0.1 --port 9000 --size 100000 --timeout 0.05
```

### 2. Go-Back-N Protocol (Part C)

**Start the Receiver:**

```bash
# Basic GBN receiver listening on port 9001
python3 receiver_gbn.py --port 9001 --outdir received

# GBN receiver with 20% simulated packet loss
python3 receiver_gbn.py --port 9001 --loss 0.2 --outdir received
```

**Run the Sender:**

```bash
# Send file with sliding window size N=8
python3 sender_gbn.py --host 127.0.0.1 --port 9001 --file test.txt --window 8

# Send 1MB auto-generated file with window N=16
python3 sender_gbn.py --host 127.0.0.1 --port 9001 --size 1000000 --window 16 --timeout 0.05
```

### 3. Running Automated Experiments (Part D)

To execute the automated benchmark suite across all required file sizes (10KB to 100MB) and loss rates (0%, 10%, 20%, 30%), run:

```bash
python3 run_experiments.py
```

This script spawns receivers dynamically, executes 5 test iterations per configuration for both Stop-and-Wait and GBN, and exports all metrics directly into `experimental_results.csv`.

## Program Options & Command-Line Arguments

### Sender Options (`sender_stopwait.py` & `sender_gbn.py`)

* `--host`: Target receiver IP address (default: `127.0.0.1`).
* `--port`: Target receiver UDP port (default: `9000` for SW, `9001` for GBN).
* `--file`: Path to file for upload.
* `--size`: Number of bytes of synthetic data to generate (default: `10000`).
* `--payload`: Payload size per DATA packet in bytes (default: `1024`).
* `--timeout`: Retransmission timer timeout in seconds (default: `0.05`).
* `--window`: *(GBN only)* Sliding window size $N$ (default: `8`).
* `--csv`: File path to log result metrics as CSV rows.
* `--tag`: Custom metadata tag saved in the CSV log (e.g., loss rate label).

### Receiver Options (`receiver_stopwait.py` & `receiver_gbn.py`)

* `--host`: Bind IP address (default: `0.0.0.0`).
* `--port`: Bind UDP port (default: `9000` for SW, `9001` for GBN).
* `--loss`: Simulated random packet drop probability between `0.0` and `1.0` (e.g., `0.1` = 10%).
* `--corruption`: Simulated packet corruption rate (bit-flip error injection for CRC32 testing).
* `--outdir`: Directory where received files are saved (default: `.`).
* `--seed`: RNG seed for reproducible packet drop sequences.

## Assumptions & Implementation Notes

1. **Sequence Numbering:** Sequence numbers start at `0` (`UPLOAD`), increment by `1` for each `DATA` chunk (`1..N`), and end with `N+1` (`FIN`).
2. **Timer Management:**
   * Stop-and-Wait operates a per-packet timer restarted after every transmission.
   * Go-Back-N uses a single logical timer linked to the oldest unacknowledged packet (`base`).
3. **Cumulative ACKs:** GBN receivers issue cumulative ACKs acknowledging the highest in-order sequence number received (`expected - 1`). Out-of-order or duplicate packets trigger an immediate re-ACK of the last in-order packet.
4. **End-to-End File Integrity:** Every packet header carries a CRC32 checksum. `FIN` packets convey the total file byte count, which the receiver validates upon closing the received file.