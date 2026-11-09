#!/usr/bin/env python3
"""
run_experiments.py - Automated experiment runner for CP372 Assignment 2.
Runs Stop-and-Wait and Go-Back-N across all required file sizes and loss rates,
repeating each test 5 times, and collecting performance metrics into a CSV file.
"""

import subprocess
import time
import os
import sys

# Configuration based on assignment specs
FILE_SIZES = {
    "10K": 10 * 1024,
    "50K": 50 * 1024,
    "100K": 100 * 1024,
    "500K": 500 * 1024,
    "1M": 1 * 1024 * 1024,
    "5M": 5 * 1024 * 1024,
    "10M": 10 * 1024 * 1024,
    "50M": 50 * 1024 * 1024,
    "100M": 100 * 1024 * 1024,
}

LOSS_RATES = [0.0, 0.1, 0.2, 0.3]
NUM_TESTS = 5
CSV_FILENAME = "experimental_results.csv"

SW_PORT = 9000
GBN_PORT = 9001
HOST = "127.0.0.1"


def run_cmd(cmd):
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    stdout, stderr = process.communicate()
    return process.returncode, stdout, stderr


def run_experiment_suite():
    # Initialize CSV header if not exists
    if not os.path.exists(CSV_FILENAME):
        with open(CSV_FILENAME, "w") as f:
            f.write("protocol,loss_rate,size_label,bytes,test_num,time_s,throughput_Bps,retransmissions
")

    print(f"Starting experiments. Results will be saved to {CSV_FILENAME}")

    # --- 1. Test Stop-and-Wait ---
    print("\n==============================")
    print("Running Stop-and-Wait Experiments")
    print("==============================")
    for loss in LOSS_RATES:
        for size_label, size_bytes in FILE_SIZES.items():
            # For 50M and 100M with Stop-and-Wait, skip or use fewer bytes if it takes too long,
            # but assignment requires all. Let's run them with appropriate timeouts.
            for test in range(1, NUM_TESTS + 1):
                print(f"[Stop-And-Wait] Loss: {loss*100:.0f}% | Size: {size_label} | Test {test}/{NUM_TESTS}")

                # Start receiver
                recv_proc = subprocess.Popen([
                    sys.executable, "receiver_stopwait.py",
                    "--port", str(SW_PORT),
                    "--loss", str(loss)
                ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                time.sleep(0.5)  # give receiver time to bind

                # Run sender
                # Timeout scaled slightly for larger files to prevent premature failure
                timeout_val = 0.1 if size_bytes < 10 * 1024 * 1024 else 0.2
                tag = f"loss_{int(loss*100)}_{size_label}"
                
                sender_cmd = [
                    sys.executable, "sender_stopwait.py",
                    "--host", HOST,
                    "--port", str(SW_PORT),
                    "--size", str(size_bytes),
                    "--timeout", str(timeout_val),
                    "--csv", CSV_FILENAME,
                    "--tag", tag
                ]

                code, out, err = run_cmd(sender_cmd)
                recv_proc.terminate()
                recv_proc.wait()

                if code != 0:
                    print(f"  Warning: SW failed for {size_label} at {loss*100}% loss: {err.strip()}")
                time.sleep(0.2)

    # --- 2. Test Go-Back-N ---
    print("\n==============================")
    print("Running Go-Back-N Experiments")
    print("==============================")
    for loss in LOSS_RATES:
        for size_label, size_bytes in FILE_SIZES.items():
            for test in range(1, NUM_TESTS + 1):
                print(f"[Go-Back-N] Loss: {loss*100:.0f}% | Size: {size_label} | Test {test}/{NUM_TESTS}")

                # Start receiver
                recv_proc = subprocess.Popen([
                    sys.executable, "receiver_gbn.py",
                    "--port", str(GBN_PORT),
                    "--loss", str(loss)
                ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                time.sleep(0.5)

                # Run sender
                timeout_val = 0.05 if size_bytes < 10 * 1024 * 1024 else 0.1
                tag = f"loss_{int(loss*100)}_{size_label}"

                sender_cmd = [
                    sys.executable, "sender_gbn.py",
                    "--host", HOST,
                    "--port", str(GBN_PORT),
                    "--size", str(size_bytes),
                    "--window", "16",
                    "--timeout", str(timeout_val),
                    "--csv", CSV_FILENAME,
                    "--tag", tag
                ]

                code, out, err = run_cmd(sender_cmd)
                recv_proc.terminate()
                recv_proc.wait()

                if code != 0:
                    print(f"  Warning: GBN failed for {size_label} at {loss*100}% loss: {err.strip()}")
                time.sleep(0.2)

    print(f"\nAll experiments completed successfully! Results logged to {CSV_FILENAME}")

if __name__ == "__main__":
    run_experiment_suite()
