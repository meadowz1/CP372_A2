#!/usr/bin/env python3
"""
sender_gbn.py - Go-Back-N reliable file sender over UDP (CP372 A2, Part C).

Sender state
  base         sequence number of the oldest unacknowledged packet
  nextseqnum   sequence number of the next packet to send
  window_size  max number of unacknowledged packets in flight

The whole transfer is one sequence of packets: UPLOAD (seq 0), DATA (1..N), FIN (N+1).
  * Packets are sent without waiting while nextseqnum < base + window_size.
  * ONE timer runs for the oldest unacknowledged packet (base).
  * A cumulative ACK n slides the window: base = n + 1 (timer restarts, or stops if
    nothing is outstanding).
  * On timeout the whole window [base, nextseqnum) is retransmitted.

Usage
  python3 sender_gbn.py --host 127.0.0.1 --port 9001 --file test.txt --window 8
  python3 sender_gbn.py --size 1000000 --window 16 --csv results.csv --tag loss10
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

DEFAULT_PAYLOAD = 1024
DEFAULT_TIMEOUT = 0.05
DEFAULT_WINDOW = 8
MAX_CONSECUTIVE_TIMEOUTS = 500   # give up if the window makes no progress this often


def load_content(args):
    if args.file:
        with open(args.file, "rb") as f:
            return os.path.basename(args.file), f.read()
    line = b"CP372 reliable data transfer over UDP - constant test content.\n"
    data = (line * (args.size // len(line) + 1))[: args.size]
    return f"generated_{args.size}.txt", data


def main():
    ap = argparse.ArgumentParser(description="Go-Back-N UDP file sender")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9001)
    src = ap.add_mutually_exclusive_group()
    src.add_argument("--file", help="file to send")
    src.add_argument("--size", type=int, default=10_000,
                     help="generate this many bytes of constant content (default 10000)")
    ap.add_argument("--payload", type=int, default=DEFAULT_PAYLOAD, help="bytes per DATA packet")
    ap.add_argument("--window", type=int, default=DEFAULT_WINDOW, help="window size N")
    ap.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT, help="timeout (s)")
    ap.add_argument("--csv", help="append a result row to this CSV file")
    ap.add_argument("--tag", default="", help="label stored in the CSV row (e.g. loss rate)")
    args = ap.parse_args()
    if args.window < 1:
        ap.error("--window must be >= 1")

    name, data = load_content(args)
    addr = (args.host, args.port)
    n_data = (len(data) + args.payload - 1) // args.payload
    total = n_data + 2                     # UPLOAD + DATA packets + FIN
    fin_seq = total - 1
    upload_pkt = build_packet(TYPE_UPLOAD, 0, payload=name.encode())
    fin_pkt = build_packet(TYPE_FIN, fin_seq, payload=struct.pack("!Q", len(data)))

    def make_packet(seq):                  # built on demand to keep memory small
        if seq == 0:
            return upload_pkt
        if seq == fin_seq:
            return fin_pkt
        chunk = data[(seq - 1) * args.payload: seq * args.payload]
        return build_packet(TYPE_DATA, seq, payload=chunk)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
    except OSError:
        pass

    base, nextseqnum = 0, 0
    timer_start = None                     # single timer for the oldest unACKed packet
    packets_sent = retransmissions = timeouts = 0
    stalled = 0

    start = time.monotonic()
    try:
        while base < total:
            # ---- fill the window ---------------------------------------------
            while nextseqnum < base + args.window and nextseqnum < total:
                sock.sendto(make_packet(nextseqnum), addr)
                packets_sent += 1
                if base == nextseqnum:
                    timer_start = time.monotonic()
                nextseqnum += 1

            # ---- wait for an ACK or for the timer to expire -------------------
            remaining = timer_start + args.timeout - time.monotonic()
            if remaining <= 0:
                # timeout: go back and resend everything outstanding
                timeouts += 1
                stalled += 1
                if stalled > MAX_CONSECUTIVE_TIMEOUTS:
                    raise RuntimeError(f"no progress at packet {base} after {stalled} timeouts")
                for seq in range(base, nextseqnum):
                    sock.sendto(make_packet(seq), addr)
                    packets_sent += 1
                    retransmissions += 1
                timer_start = time.monotonic()
                continue

            sock.settimeout(remaining)
            try:
                raw, src_addr = sock.recvfrom(2048)
            except socket.timeout:
                continue                    # loop top handles the expired timer
            pkt = parse_packet(raw)
            if pkt is None or src_addr != addr or pkt.ptype != TYPE_ACK:
                continue
            # ---- cumulative ACK -------------------------------------------------
            if base <= pkt.ack < nextseqnum:
                base = pkt.ack + 1
                stalled = 0
                timer_start = time.monotonic() if base < nextseqnum else None
            # ACKs below base are duplicates (receiver saw a gap) - ignored
        elapsed = time.monotonic() - start
    except RuntimeError as e:
        print(f"[sender] transfer failed: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        sock.close()

    throughput = len(data) / elapsed if elapsed > 0 else float("inf")
    print(f"[sender] Go-Back-N (N={args.window}): sent '{name}' "
          f"({len(data)} bytes, {n_data} DATA packets)")
    print(f"[sender] time={elapsed:.4f}s  throughput={throughput:.0f} B/s  "
          f"retransmissions={retransmissions}  timeouts={timeouts}  "
          f"total_packets_sent={packets_sent}")
    print(f"RESULT protocol=gbn window={args.window} bytes={len(data)} time={elapsed:.6f} "
          f"throughput={throughput:.2f} retransmissions={retransmissions}")

    if args.csv:
        new = not os.path.exists(args.csv)
        with open(args.csv, "a") as f:
            if new:
                f.write("tag,protocol,bytes,time_s,throughput_Bps,retransmissions\n")
            f.write(f"{args.tag},gbn,{len(data)},{elapsed:.6f},{throughput:.2f},"
                    f"{retransmissions}\n")


if __name__ == "__main__":
    main()
