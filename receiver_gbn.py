#!/usr/bin/env python3
"""
receiver_gbn.py - Go-Back-N reliable file receiver over UDP (CP372 A2, Part C).

Behaviour
  * Accepts ONLY the expected packet (seq == expected), then expected += 1.
  * Every other packet is discarded (duplicates and out-of-order alike).
  * After every packet it replies with a CUMULATIVE ACK = expected - 1, the highest
    sequence number received in order.
      e.g. expected = 3, packet 4 arrives -> discard 4, send ACK 2.
  * UPLOAD -> create file, DATA -> write content, FIN -> close file.
  * --loss P randomly drops incoming packets with probability P (loss simulation).
  * --corruption P corrupts incoming packets; the checksum detects and discards them.

Usage
  python3 receiver_gbn.py --port 9001 --loss 0.1 --outdir received
"""
import argparse
import os
import random
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

IDLE, ACTIVE, DONE = "idle", "active", "done"
IDLE_TIMEOUT = 10.0


def corrupt(raw):
    """Flip one random bit of the datagram (simulated transmission error)."""
    b = bytearray(raw)
    i = random.randrange(len(b))
    b[i] ^= 1 << random.randrange(8)
    return bytes(b)


def main():
    ap = argparse.ArgumentParser(description="Go-Back-N UDP file receiver")
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=9001)
    ap.add_argument("--loss", type=float, default=0.0, help="packet loss rate, 0.0-1.0")
    ap.add_argument("--corruption", type=float, default=0.0, help="corruption rate, 0.0-1.0")
    ap.add_argument("--outdir", default=".", help="directory for received files")
    ap.add_argument("--seed", type=int, default=None, help="RNG seed (reproducible loss)")
    args = ap.parse_args()

    if args.seed is not None:
        random.seed(args.seed)
    os.makedirs(args.outdir, exist_ok=True)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
    except OSError:
        pass
    sock.bind((args.host, args.port))
    sock.settimeout(IDLE_TIMEOUT)
    print(f"[receiver] Go-Back-N listening on {args.host}:{args.port} "
          f"(loss={args.loss:.0%}, corruption={args.corruption:.0%})")

    def reset_stats():
        return dict(received=0, dropped=0, corrupted=0, discarded=0)

    def send_ack(addr, last_in_order):
        sock.sendto(build_packet(TYPE_ACK, 0, ack=last_in_order), addr)

    state, expected, peer = IDLE, 0, None
    fobj, path, written, t_start = None, None, 0, 0.0
    st = reset_stats()
    try:
        while True:
            try:
                raw, addr = sock.recvfrom(65535)
            except socket.timeout:
                if state == ACTIVE:
                    print("[receiver] sender went silent - transfer abandoned")
                    fobj.close()
                    state = IDLE
                continue

            st["received"] += 1
            # ---- simulated packet loss ---------------------------------------
            if random.random() < args.loss:
                st["dropped"] += 1
                continue
            # ---- simulated corruption (bonus B) ------------------------------
            if args.corruption > 0 and random.random() < args.corruption:
                raw = corrupt(raw)
            pkt = parse_packet(raw)
            if pkt is None:
                st["corrupted"] += 1
                continue
            if state == ACTIVE and addr != peer:
                continue

            # ---- start of a new transfer --------------------------------------
            if pkt.ptype == TYPE_UPLOAD and pkt.seq == 0 and state != ACTIVE:
                name = os.path.basename(pkt.payload.decode(errors="replace")) or "file.bin"
                path = os.path.join(args.outdir, "received_" + name)
                fobj = open(path, "wb")
                state, expected, peer = ACTIVE, 1, addr
                written, t_start = 0, time.monotonic()
                st = reset_stats()
                st["received"] = 1
                print(f"[receiver] UPLOAD from {addr[0]}:{addr[1]} -> {path}")
                send_ack(addr, 0)
                continue

            if state == DONE:
                # late duplicates after completion: repeat the final cumulative ACK
                if pkt.seq < expected:
                    send_ack(addr, expected - 1)
                continue
            if state != ACTIVE:
                continue

            # ---- transfer in progress ----------------------------------------
            if pkt.seq == expected and pkt.ptype in (TYPE_DATA, TYPE_FIN):
                if pkt.ptype == TYPE_DATA:
                    fobj.write(pkt.payload)
                    written += len(pkt.payload)
                else:
                    fobj.close()
                    total = struct.unpack("!Q", pkt.payload)[0] if len(pkt.payload) == 8 else -1
                    ok = "OK" if total == written else f"SIZE MISMATCH (expected {total})"
                    print(f"[receiver] FIN: saved {written} bytes to {path} [{ok}] in "
                          f"{time.monotonic() - t_start:.3f}s  "
                          f"(received={st['received']} dropped={st['dropped']} "
                          f"corrupted={st['corrupted']} discarded={st['discarded']})")
                    state = DONE
                expected += 1
                send_ack(addr, expected - 1)
            else:
                st["discarded"] += 1               # duplicate or out of order
                send_ack(addr, expected - 1)       # cumulative ACK of last in-order pkt
    except KeyboardInterrupt:
        print("\n[receiver] stopped")
    finally:
        if fobj and not fobj.closed:
            fobj.close()
        sock.close()


if __name__ == "__main__":
    main()
