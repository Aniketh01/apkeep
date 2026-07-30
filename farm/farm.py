#!/usr/bin/env python3
"""Phone-farm orchestrator for apkeep Google Play crawling.

Drives N apkeep subprocesses (one worker per Google account) over a crash-safe
SQLite work queue. Durable resume + automatic redistribution of a banned
account's apps. Stdlib only.

See README.md in this directory for setup and usage.
"""
import argparse
import csv
import os
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
import requests

# ---- pure queue logic (unit-tested in test_farm.py) -------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS apps (
    pkg TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'pending',  -- pending | claimed | done | failed | not_found
    attempts INTEGER NOT NULL DEFAULT 0,
    account TEXT,
    updated REAL NOT NULL DEFAULT 0,
    provider TEXT NOT NULL DEFAULT 'google_play'
);
CREATE TABLE IF NOT EXISTS accounts (
    email TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'active',   -- active | cooldown | disabled
    fails INTEGER NOT NULL DEFAULT 0
);
"""


def init_db(conn):
    conn.executescript(SCHEMA)
    conn.commit()


def seed_apps(conn, pkgs, now):
    conn.executemany(
        "INSERT OR IGNORE INTO apps(pkg, status, updated) VALUES (?, 'pending', ?)",
        [(p, now) for p in pkgs],
    )
    conn.commit()


def seed_accounts(conn, emails):
    conn.executemany(
        "INSERT OR IGNORE INTO accounts(email, status, fails) VALUES (?, 'active', 0)",
        [(e,) for e in emails],
    )
    conn.commit()


def recover(conn, now):
    """Revert rows left 'claimed' by a crashed run back to 'pending'. Returns count."""
    cur = conn.execute("SELECT COUNT(*) FROM apps WHERE status='claimed'")
    n = cur.fetchone()[0]
    conn.execute(
        "UPDATE apps SET status='pending', account=NULL, updated=? WHERE status='claimed'",
        (now,),
    )
    conn.commit()
    return n


def claim_batch(conn, account, n, now):
    """Atomically claim up to n pending pkgs for `account`. Returns list of pkgs."""
    rows = conn.execute(
        "SELECT pkg FROM apps WHERE status='pending' ORDER BY attempts, pkg LIMIT ?",
        (n,),
    ).fetchall()
    pkgs = [r[0] for r in rows]
    if pkgs:
        qs = ",".join("?" * len(pkgs))
        conn.execute(
            f"UPDATE apps SET status='claimed', account=?, updated=? WHERE pkg IN ({qs})",
            [account, now, *pkgs],
        )
        conn.commit()
    return pkgs


def record_results(conn, successes, failures, max_attempts, now, not_found=None):
    """Mark successes done; bump failures back to pending (or 'failed'/'not_found' at max_attempts)."""
    if not_found is None:
        not_found = []

    if successes:
        qs = ",".join("?" * len(successes))
        conn.execute(
            f"UPDATE apps SET status='done', updated=? WHERE pkg IN ({qs})",
            [now, *successes],
        )
    if not_found:
        qs = ",".join("?" * len(not_found))
        conn.execute(
            f"UPDATE apps SET attempts=attempts+1, status='not_found', updated=?, account=NULL WHERE pkg IN ({qs})",
            [now, *not_found],
        )
    for pkg in failures:
        conn.execute(
            """UPDATE apps SET attempts=attempts+1, account=NULL, updated=?,
               status=CASE WHEN attempts+1 >= ? THEN 'failed' ELSE 'pending' END
               WHERE pkg=?""",
            (now, max_attempts, pkg),
        )
    conn.commit()


def remaining(conn):
    """Count apps still workable (pending or claimed)."""
    return conn.execute(
        "SELECT COUNT(*) FROM apps WHERE status IN ('pending','claimed')"
    ).fetchone()[0]


def release_claimed(conn, account, now):
    """Return an account's in-flight claimed rows to the pool (crash/exit cleanup)."""
    conn.execute(
        "UPDATE apps SET status='pending', account=NULL, updated=? WHERE status='claimed' AND account=?",
        (now, account),
    )
    conn.commit()


def set_account(conn, email, status, fails):
    conn.execute(
        "UPDATE accounts SET status=?, fails=? WHERE email=?", (status, fails, email)
    )
    conn.commit()


def summary(conn):
    return dict(conn.execute("SELECT status, COUNT(*) FROM apps GROUP BY status").fetchall())


# ---- apkeep invocation ------------------------------------------------------


def build_cmd(apkeep, account, batch_csv, outdir, options, parallel, sleep, accept_tos):
    opts = list(options)
    if account["locale"]:
        opts.append(f"locale={account['locale']}")
    if account["device_properties_path"]:
        # a custom file must be paired with device=default (see USAGE-google-play.md)
        opts.append("device=default")
        opts.append(f"device_properties_file={account['device_properties_path']}")
    cmd = [
        apkeep, "-c", batch_csv, "-d", "google-play",
        "-e", account["email"],
        "-r", str(parallel), "-s", str(sleep),
    ]
    if account["token_type"] == "auth":
        cmd += ["--auth-token", account["token"]]
    else:
        cmd += ["-t", account["token"]]
    if accept_tos:
        cmd += ["--accept-tos"]
    cmd += ["-o", ",".join(opts), outdir]
    return cmd


def check_accounts(cfg, accounts):
    """Probe each account's login without downloading. Returns list of (email, ok, detail)."""
    import concurrent.futures

    bogus = "com.apkeep.farm.login.probe"  # nonexistent app: login runs, download is skipped
    tmp = tempfile.mkdtemp(prefix="apkeep-check-")

    def probe(a):
        cmd = [cfg.apkeep, "-d", "google-play", "-e", a["email"], "-r", "1", "-s", "0", "--accept-tos"]
        cmd += ["--auth-token", a["token"]] if a["token_type"] == "auth" else ["-t", a["token"]]
        opts = []
        if a["locale"]:
            opts.append(f"locale={a['locale']}")
        if a["device_properties_path"]:
            opts.append("device=default")
            opts.append(f"device_properties_file={a['device_properties_path']}")
        if opts:
            cmd += ["-o", ",".join(opts)]
        cmd += ["-a", bogus, tmp]
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            out = (r.stdout + r.stderr).lower()
        except subprocess.TimeoutExpired:
            return a["email"], False, "timed out"
        if "could not log in" in out or "could not accept" in out:
            return a["email"], False, "login rejected (bad/expired token)"
        # login succeeded; the probe app is (correctly) reported invalid/skipped
        return a["email"], True, "ok"

    with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(accounts))) as ex:
        return list(ex.map(probe, accounts))


def produced(outdir, pkg):
    """True if apkeep produced output for pkg (single apk or split dir)."""
    apk = Path(outdir) / f"{pkg}.apk"
    if apk.is_file() and apk.stat().st_size > 0:
        return True
    d = Path(outdir) / pkg
    if d.is_dir() and any(d.iterdir()):
        return True
    # gpapi may suffix versioned names; fall back to a prefix glob
    return any(Path(outdir).glob(f"{pkg}*"))


# ---- worker -----------------------------------------------------------------


def worker(name, account, conn, lock, cfg, stop):
    email = account["email"]
    cooldowns_used = 0
    try:
        _worker_loop(name, account, conn, lock, cfg, stop, cooldowns_used)
    except Exception as e:  # disk full, DB error, etc. -- don't strand this account's work
        print(f"[{name}] worker crashed: {e}; releasing its claimed apps")
    finally:
        try:
            with lock:
                release_claimed(conn, email, time.time())
        except Exception:
            pass  # next run's recover() will reclaim if this also fails


def _worker_loop(name, account, conn, lock, cfg, stop, cooldowns_used):
    email = account["email"]
    while not stop.is_set():
        with lock:
            batch = claim_batch(conn, email, cfg.batch_size, time.time())
        if not batch:
            with lock:
                left = remaining(conn)
            if left == 0:
                return  # queue drained
            time.sleep(5)  # others still hold claimed rows; wait for possible release
            continue

        with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False) as f:
            f.write("\n".join(batch) + "\n")
            batch_csv = f.name
        try:
            cmd = build_cmd(cfg.apkeep, account, batch_csv, cfg.outdir,
                            cfg.options, cfg.parallel, cfg.sleep, cfg.accept_tos)
            log_path = Path(cfg.logdir) / f"{email}.log"
            with open(log_path, "a") as lf:
                lf.write(f"\n=== batch of {len(batch)} @ {time.strftime('%F %T')} ===\n")
                rc = subprocess.run(cmd, stdout=lf, stderr=subprocess.STDOUT).returncode
        finally:
            os.unlink(batch_csv)

        successes = [p for p in batch if produced(cfg.outdir, p)]
        failures = [p for p in batch if p not in successes]

        not_found = []
        final_failures = []
        androzoo_successes = []
        if failures:
            with lock:
                qs = ",".join("?" * len(failures))
                attempts_map = dict(conn.execute(f"SELECT pkg, attempts FROM apps WHERE pkg IN ({qs})", failures).fetchall())
            
            for pkg in failures:
                if attempts_map.get(pkg, 0) + 1 >= cfg.max_attempts:
                    print(f"[{name}] {pkg} reached max_attempts, checking if it exists on Play Store...")
                    exists = check_package_exists(pkg)
                    
                    if exists is False:
                        print(f"[{name}] {pkg} not found on Play Store.")
                    elif exists is None:
                        print(f"[{name}] Network error checking {pkg}.")
                    else:
                        print(f"[{name}] {pkg} exists but failed to download.")
                        
                    sha256 = cfg.apk_info.get(pkg) if hasattr(cfg, "apk_info") else None
                    if cfg.androzoo_api_key and sha256:
                        print(f"[{name}] Attempting to download {pkg} from AndroZoo...")
                        if download_package_from_Androzoo(pkg, sha256, cfg.outdir, cfg.androzoo_api_key):
                            print(f"[{name}] Successfully downloaded {pkg} from AndroZoo.")
                            androzoo_successes.append(pkg)
                            successes.append(pkg)
                        else:
                            if exists is False:
                                not_found.append(pkg)
                            else:
                                final_failures.append(pkg)
                    else:
                        # Silently skip AndroZoo if no API key or no SHA256 mapping
                        if exists is False:
                            not_found.append(pkg)
                        else:
                            final_failures.append(pkg)
                else:
                    final_failures.append(pkg)

        with lock:
            if androzoo_successes:
                qs = ",".join("?" * len(androzoo_successes))
                conn.execute(f"UPDATE apps SET provider='androzoo' WHERE pkg IN ({qs})", androzoo_successes)
            record_results(conn, successes, final_failures, cfg.max_attempts, time.time(), not_found=not_found)

        # account health: rc!=0 (login death) or a batch that produced nothing = a strike
        if rc != 0 or (batch and not successes):
            account["fails"] += 1
            print(f"[{name}] strike {account['fails']}/{cfg.max_fails} "
                  f"(rc={rc}, {len(successes)}/{len(batch)} ok)")
        else:
            account["fails"] = 0

        if account["fails"] >= cfg.max_fails:
            if cooldowns_used < cfg.cooldowns:
                cooldowns_used += 1
                account["fails"] = 0
                with lock:
                    set_account(conn, email, "cooldown", 0)
                print(f"[{name}] cooldown {cooldowns_used}/{cfg.cooldowns} "
                      f"for {cfg.cooldown}s")
                if stop.wait(cfg.cooldown):
                    return
                with lock:
                    set_account(conn, email, "active", 0)
            else:
                with lock:
                    set_account(conn, email, "disabled", account["fails"])
                print(f"[{name}] DISABLED after repeated failures; "
                      f"its apps return to the queue for other accounts")
                return


# ---- setup ------------------------------------------------------------------


def read_accounts(path):
    base = Path(path).resolve().parent  # device paths are relative to accounts.csv
    accounts = []
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            if not row.get("email", "").strip() or row["email"].lstrip().startswith("#"):
                continue
            dpp = (row.get("device_properties_path") or "").strip()
            if dpp and not Path(dpp).is_absolute():
                dpp = str(base / dpp)
            if dpp and not Path(dpp).is_file():
                sys.exit(f"device_properties_path not found for {row['email']}: {dpp}")
            accounts.append({
                "email": row["email"].strip(),
                "token": row["token"].strip(),
                "token_type": (row.get("token_type") or "aas").strip().lower(),
                "device_properties_path": dpp,
                "locale": (row.get("locale") or "").strip(),
                "fails": 0,
            })
    return accounts


def read_pkgs(path, field):
    pkgs = []
    with open(path, newline="") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            cols = line.split(",")
            if field - 1 < len(cols):
                pkg = cols[field - 1].strip()
                if pkg:
                    pkgs.append(pkg)
    return pkgs

def check_package_exists(package_name: str):
    """
    Checks if a package exists on the Play Store.
    Returns True if it exists, False if it doesn't, None if there's a network error.
    """
    url = f"https://play.google.com/store/apps/details?id={package_name}"
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    try:
        response = requests.head(url, headers=headers, allow_redirects=True, timeout=10)
        return response.status_code != 404
    except requests.RequestException as e:
        print(f"Network error checking {package_name}: {e}")
        return None

def download_package_from_Androzoo(package_name, sha256, outdir, api_key):
    if not api_key:
        return False
    if not sha256:
        print(f"[{package_name}] No SHA256 available, cannot download from AndroZoo.")
        return False

    pkg_dir = Path(outdir) / package_name
    pkg_dir.mkdir(parents=True, exist_ok=True)
    final_apk_path = pkg_dir / f"{package_name}.apk"
    
    # Download directly to the final destination with "-o" instead of renaming later.
    # The "-f" flag ensures curl fails cleanly on HTTP errors (e.g. 404).
    cmd = [
        "curl", "-f", "-s", "-L", "-G",
        "-d", f"apikey={api_key}",
        "-d", f"sha256={sha256}",
        "-o", str(final_apk_path),
        "https://androzoo.uni.lu/api/download"
    ]
    try:
        res = subprocess.run(cmd, capture_output=True, timeout=600)
        if res.returncode == 0 and final_apk_path.exists() and final_apk_path.stat().st_size > 0:
            return True
        else:
            if final_apk_path.exists():
                final_apk_path.unlink()
            return False
    except subprocess.TimeoutExpired:
        if final_apk_path.exists():
            final_apk_path.unlink()
        return False

def load_apk_info(json_path):
    import json
    if not json_path or not Path(json_path).exists():
        return {}
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return {item["packageName"]: item.get("sha256") for item in data}


def main(argv=None):
    import sqlite3

    p = argparse.ArgumentParser(description="apkeep phone-farm orchestrator")
    p.add_argument("--apps", help="CSV/text of package names (not needed with --check)")
    p.add_argument("--accounts", required=True, help="accounts.csv (see .example)")
    p.add_argument("--outdir", help="download output directory (not needed with --check)")
    p.add_argument("--check", action="store_true",
                   help="probe every account's login and report valid/invalid, then exit")
    p.add_argument("--json_parser_path", help="path to apk info json file generated by metadata_paser tool")
    p.add_argument("--db", default="queue.db", help="SQLite queue file")
    p.add_argument("--logdir", default="logs", help="per-account apkeep logs")
    p.add_argument("--apkeep", default="apkeep", help="path to apkeep binary")
    p.add_argument("--field", type=int, default=1, help="1-based app-id column in --apps")
    p.add_argument("--batch-size", type=int, default=50)
    p.add_argument("--parallel", type=int, default=4, help="apkeep -r (in-flight per account)")
    p.add_argument("--device", help="built-in device profile name (e.g. px_9a); overrides default")
    p.add_argument("--sleep", type=int, default=1000, help="apkeep -s ms between apps")
    p.add_argument("--max-attempts", type=int, default=3, help="per-app tries before 'failed'")
    p.add_argument("--max-fails", type=int, default=3, help="consecutive bad batches before cooldown")
    p.add_argument("--cooldowns", type=int, default=1, help="cooldowns before an account is disabled")
    p.add_argument("--cooldown", type=int, default=600, help="cooldown seconds")
    p.add_argument("--accept-tos", action="store_true")
    p.add_argument("--split-apk", action="store_true", default=True)
    p.add_argument("--no-split-apk", dest="split_apk", action="store_false")
    p.add_argument("--additional-files", action="store_true", default=True,
                   help="include OBB expansion files")
    p.add_argument("--dex-metadata", action="store_true", help="include .dm cloud profiles")
    cfg = p.parse_args(argv)

    accounts = read_accounts(cfg.accounts)
    if not accounts:
        sys.exit("no accounts found")

    if cfg.check:
        print(f"Checking {len(accounts)} accounts...")
        results = check_accounts(cfg, accounts)
        bad = 0
        for email, ok, detail in results:
            print(f"  {'OK   ' if ok else 'BAD  '} {email}  ({detail})")
            bad += not ok
        print(f"\n{len(results) - bad}/{len(results)} valid.")
        sys.exit(1 if bad else 0)

    if not cfg.apps or not cfg.outdir:
        sys.exit("--apps and --outdir are required (unless using --check)")
    if not Path(cfg.outdir).is_dir():
        sys.exit(f"outdir is not a directory: {cfg.outdir}")
    Path(cfg.logdir).mkdir(parents=True, exist_ok=True)

    cfg.options = []
    if cfg.device:  # per-account device_properties_path (Option B) takes precedence in build_cmd
        cfg.options.append(f"device={cfg.device}")
    if cfg.split_apk:
        cfg.options.append("split_apk=1")
    if cfg.additional_files:
        cfg.options.append("include_additional_files=1")
    if cfg.dex_metadata:
        cfg.options.append("include_dex_metadata=1")

    # Load environment variables from .env if present
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass  # If python-dotenv is not installed, fallback to standard environment variables

    cfg.androzoo_api_key = os.environ.get("ANDROZOO_API_KEY")

    if cfg.json_parser_path:
        cfg.apk_info = load_apk_info(cfg.json_parser_path)
    else:
        cfg.apk_info = {}

    pkgs = read_pkgs(cfg.apps, cfg.field)
    if not pkgs:
        sys.exit("no packages found")

    conn = sqlite3.connect(cfg.db, check_same_thread=False)
    conn.execute("PRAGMA busy_timeout=5000")
    init_db(conn)
    now = time.time()
    seed_apps(conn, pkgs, now)
    seed_accounts(conn, [a["email"] for a in accounts])
    reverted = recover(conn, now)
    if reverted:
        print(f"Recovered {reverted} claimed apps from a previous run -> pending")
    # a fresh run re-activates accounts disabled last time (tokens may be refreshed)
    conn.execute("UPDATE accounts SET status='active', fails=0")
    conn.commit()

    print(f"{len(pkgs)} apps seeded ({remaining(conn)} remaining), "
          f"{len(accounts)} accounts. Starting workers...")

    lock = threading.Lock()  # ponytail: one global DB lock; batch claims make contention negligible
    stop = threading.Event()
    threads = []
    for i, acc in enumerate(accounts):
        t = threading.Thread(target=worker, args=(f"w{i}:{acc['email']}", acc, conn, lock, cfg, stop))
        t.start()
        threads.append(t)
    try:
        while any(t.is_alive() for t in threads):
            for t in threads:
                t.join(timeout=1)
    except KeyboardInterrupt:
        print("\nInterrupted; signalling workers to stop after current batch...")
        stop.set()
        for t in threads:
            t.join()

    s = summary(conn)
    print(f"\nDone. {s}")
    if s.get("failed"):
        print(f"{s['failed']} apps hit --max-attempts and are marked 'failed' "
              f"(query the DB: SELECT pkg FROM apps WHERE status='failed').")
    conn.close()


if __name__ == "__main__":
    main()
