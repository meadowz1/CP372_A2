#!/usr/bin/env python3
"""
sender_stopwait.py - Stop-and-Wait reliable file sender over UDP (CP372 A2, Part A).

Behaviour
  1. Reads a file (--file) or generates content (--size bytes).
  2. Sends an UPLOAD packet announcing the file.
  3. Splits the content into DATA packets and sends ONE packet at a time.
  4. After each transmission a timer starts; the sender waits for the matching ACK
     and retransmits the same packet when the timer expires.
  5. Sends a FIN packet when every DATA packet has been acknowledged.

Usage
  python3 sender_stopwait.py --host 127.0.0.1 --port 9000 --file test.txt
  python3 sender_stopwait.py --size 1000000 --timeout 0.05 --csv results.csv --tag loss10
"""
import argparse
import os
import socket
import struct
import sys
import time
import zlib
from collections import namedtuple

# ---------------------------------------------------------------------------
# Packet format (identical in all four programs)
#
#   | Type (1B) | Seq (4B) | Ack (4B, signed) | Length (2B) | Checksum (4B) | Payload |
#
#   Type     UPLOAD = announce a file (payload = file name, seq 0)
#            DATA   = a chunk of file content
#            ACK    = acknowledgment (Ack field carries the acknowledged seq)
#            FIN    = file transfer complete (payload = total file size, 8B)
#   Seq      packet number: UPLOAD=0, DATA=1..N, FIN=N+1
#   Ack      Stop-and-Wait: seq being acknowledged
#            Go-Back-N    : highest in-order seq received (cumulative), -1 = none
#   Length   number of payload bytes
#   Checksum CRC32 over the header fields (without checksum) plus the payload
# ---------------------------------------------------------------------------
TYPE_UPLOAD, TYPE_DATA, TYPE_ACK, TYPE_FIN = 1, 2, 3, 4
TYPE_NAMES = {1: "UPLOAD", 2: "DATA", 3: "ACK", 4: "FIN"}

HEADER_FMT = "!BIiHI"
HEADER_SIZE = struct.calcsize(HEADER_FMT)  # 15 bytes
Packet = namedtuple("Packet", "ptype seq ack payload")


def _checksum(ptype, seq, ack, payload):
    head = struct.pack("!BIiH", ptype, seq, ack, len(payload))
    return zlib.crc32(head + payload) & 0xFFFFFFFF


def build_packet(ptype, seq, ack=-1, payload=b""):
    """Serialize a packet (header + payload) into bytes."""
    csum = _checksum(ptype, seq, ack, payload)
    return struct.pack(HEADER_FMT, ptype, seq, ack, len(payload), csum) + payload


def parse_packet(raw):
    """Return a Packet, or None if the datagram is malformed or corrupted."""
    if len(raw) < HEADER_SIZE:
        return None
    ptype, seq, ack, length, csum = struct.unpack(HEADER_FMT, raw[:HEADER_SIZE])
    payload = raw[HEADER_SIZE:]
    if ptype not in TYPE_NAMES or len(payload) != length:
        return None
    if csum != _checksum(ptype, seq, ack, payload):
        return None
    return Packet(ptype, seq, ack, payload)

DEFAULT_PAYLOAD = 1024      # bytes of file data per DATA packet
DEFAULT_TIMEOUT = 0.05      # seconds
MAX_RETRIES = 500           # give up on a packet after this many retransmissions


class TransferError(Exception):
    pass


class Stats:
    def __init__(self):
        self.packets_sent = 0       # every transmission, including retransmissions
        self.retransmissions = 0
        self.timeouts = 0


def send_reliably(sock, addr, seq, packet, timeout, stats):
    """Send one packet and block until its ACK arrives (retransmit on timeout)."""
    attempts = 0
    while True:
        sock.sendto(packet, addr)
        stats.packets_sent += 1
        if attempts > 0:
            stats.retransmissions += 1
        attempts += 1

        deadline = time.monotonic() + timeout      # timer starts after transmission
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            sock.settimeout(remaining)
            try:
                raw, src = sock.recvfrom(2048)
            except socket.timeout:
                break
            pkt = parse_packet(raw)
            if pkt and src == addr and pkt.ptype == TYPE_ACK and pkt.ack == seq:
                return                              # correct ACK -> next packet
            # corrupted, stale or duplicate ACK: ignore and keep waiting

        stats.timeouts += 1
        if attempts > MAX_RETRIES:
            raise TransferError(f"no ACK for packet {seq} after {attempts} attempts")


def load_content(args):
    if args.file:
        with open(args.file, "rb") as f:
            return os.path.basename(args.file), f.read()
    line = b"CP372 reliable data transfer over UDP - constant test content.\n"
    data = (line * (args.size // len(line) + 1))[: args.size]
    return f"generated_{args.size}.txt", data


def main():
    ap = argparse.ArgumentParser(description="Stop-and-Wait UDP file sender")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9000)
    src = ap.add_mutually_exclusive_group()
    src.add_argument("--file", help="file to send")
    src.add_argument("--size", type=int, default=10_000,
                     help="generate this many bytes of constant content (default 10000)")
    ap.add_argument("--payload", type=int, default=DEFAULT_PAYLOAD, help="bytes per DATA packet")
    ap.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT, help="retransmission timeout (s)")
    ap.add_argument("--csv", help="append a result row to this CSV file")
    ap.add_argument("--tag", default="", help="label stored in the CSV row (e.g. loss rate)")
    args = ap.parse_args()

    name, data = load_content(args)
    addr = (args.host, args.port)
    n_data = (len(data) + args.payload - 1) // args.payload
    fin_seq = n_data + 1

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    stats = Stats()
    try:
        start = time.monotonic()

        # 1) announce the file
        send_reliably(sock, addr, 0, build_packet(TYPE_UPLOAD, 0, payload=name.encode()),
                      args.timeout, stats)
        # 2) one DATA packet at a time
        for i in range(n_data):
            seq = i + 1
            chunk = data[i * args.payload:(i + 1) * args.payload]
            send_reliably(sock, addr, seq, build_packet(TYPE_DATA, seq, payload=chunk),
                          args.timeout, stats)
        # 3) announce completion (payload = total size so the receiver can verify)
        send_reliably(sock, addr, fin_seq,
                      build_packet(TYPE_FIN, fin_seq, payload=struct.pack("!Q", len(data))),
                      args.timeout, stats)

        elapsed = time.monotonic() - start
    except TransferError as e:
        print(f"[sender] transfer failed: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        sock.close()

    throughput = len(data) / elapsed if elapsed > 0 else float("inf")
    print(f"[sender] Stop-and-Wait: sent '{name}' ({len(data)} bytes, {n_data} DATA packets)")
    print(f"[sender] time={elapsed:.4f}s  throughput={throughput:.0f} B/s  "
          f"retransmissions={stats.retransmissions}  total_packets_sent={stats.packets_sent}")
    print(f"RESULT protocol=stopwait bytes={len(data)} time={elapsed:.6f} "
          f"throughput={throughput:.2f} retransmissions={stats.retransmissions}")

    if args.csv:
        new = not os.path.exists(args.csv)
        with open(args.csv, "a") as f:
            if new:
                f.write("tag,protocol,bytes,time_s,throughput_Bps,retransmissions\n")
            f.write(f"{args.tag},stopwait,{len(data)},{elapsed:.6f},{throughput:.2f},"
                    f"{stats.retransmissions}\n")


if __name__ == "__main__":
    main()
