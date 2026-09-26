"""Real, SHA-pinned 0.7.38–0.7.40 packages against current 0.7.41+ retention.

Called only by lifecycle-cross-version-proof.ts under a disposable CI HOME.
The released cli.mjs bytes come from the published tarballs; the package's
unchanged better-sqlite3@12.10.0 dependency comes from this checkout.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import time

REPO = Path(__file__).resolve().parents[2]
HEAD = REPO / "packages/collector-cli/dist/cli.mjs"
HOOK = REPO / "scripts/lib/lifecycle-cross-version-kill.cjs"
NODE = shutil.which("node")
RELEASES = {
    "0.7.38": ("33a2ac796af9bef9ecd2a4bda0de64c9d1689de511f06ba004dc202fc1ae4f7f", "09a16956ddcc7af7f7173f573689d539a5c3c792df35fbb8a81b72d8e593a242"),
    "0.7.39": ("4d6920097c0eab03fe3b63ac329761a1f3948e25cc558d27dcac17aa300299b9", "6829d9c0c6ea84471837e27ab083e8bd9c2d6911a31567e5b6b741e236cb1354"),
    "0.7.40": ("9d84b45d9884474eb9fe98cdd5910c0fc86ef826b071e7e42fcbb7d65d7a9c88", "f065623520e69d1adc2809e4a908e81468ff1fa857a985def2258483633218b2"),
}
SCENES = ("pending_restore", "both_in_trash", "interrupted_prune", "interrupted_update", "interrupted_prune_same_id")


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def files(directory):
    return sorted(directory.iterdir()) if directory.exists() else []


def read_json(path):
    return json.loads(path.read_text())


def released_clis(root):
    clis = {}
    dependency = (REPO / "node_modules/better-sqlite3").resolve()
    require(dependency.is_dir(), "better-sqlite3 dependency is absent")
    for version, (asset_hash, cli_hash) in RELEASES.items():
        asset = root / f"plimsoll-cli-{version}.tgz"
        url = f"https://github.com/CryptoJym/plimsoll/releases/download/v{version}/{asset.name}"
        subprocess.run(["curl", "--silent", "--show-error", "--fail", "--location", "--max-time", "120", "--retry", "2", "--output", str(asset), url], check=True, timeout=300)
        require(digest(asset.read_bytes()) == asset_hash, f"{version} release tarball SHA-256 changed")
        with tarfile.open(asset, "r:gz") as archive:
            package = json.load(archive.extractfile("package/package.json"))
            cli_bytes = archive.extractfile("package/dist/cli.mjs").read()
        require(package["name"] == "@plimsoll/cli" and package["version"] == version and
                package["dependencies"]["better-sqlite3"] == "12.10.0", f"{version} package identity changed")
        require(digest(cli_bytes) == cli_hash, f"{version} cli.mjs SHA-256 changed")
        installation = root / version / "node_modules"
        cli = installation / "@plimsoll/cli/dist/cli.mjs"
        cli.parent.mkdir(parents=True)
        cli.write_bytes(cli_bytes)
        (installation / "better-sqlite3").symlink_to(dependency, target_is_directory=True)
        clis[version] = cli
        print(f"PASS published package {version} tarball={asset_hash} cli={cli_hash}", flush=True)
    return clis


class Fixture:
    def __init__(self, root):
        self.home = root / "home"
        self.collector = self.home / ".plimsoll"
        self.life = self.collector / "lifecycle"
        for directory in (self.collector, self.home / ".codex", self.home / ".claude", self.home / "tmp"):
            directory.mkdir(parents=True, exist_ok=True)
        self.home.chmod(0o700)
        self.collector.chmod(0o700)
        db = sqlite3.connect(self.collector / "work-ledger.sqlite")
        db.execute("pragma journal_mode=WAL")
        db.execute("create table proof_rows (id integer primary key, label text)")
        db.execute("insert into proof_rows(label) values ('seed')")
        db.commit()
        db.close()
        (self.collector / "collector.config.json").write_text("{}\n")

    def call(self, cli, args, fault=None):
        env = dict(os.environ)
        env.pop("NODE_OPTIONS", None)
        env.update(HOME=str(self.home), USERPROFILE=str(self.home), PLIMSOLL_HOME=str(self.collector),
                   CODEX_HOME=str(self.home / ".codex"), CLAUDE_CONFIG_DIR=str(self.home / ".claude"),
                   XDG_CONFIG_HOME=str(self.home / ".config"), XDG_CACHE_HOME=str(self.home / ".cache"),
                   XDG_STATE_HOME=str(self.home / ".local/state"), TMPDIR=str(self.home / "tmp"))
        if fault:
            source, destination, mode = fault
            env.update(NODE_OPTIONS=f"--require={HOOK}", PLS_PROOF_RENAME_SOURCE=source,
                       PLS_PROOF_RENAME_DESTINATION=destination, PLS_PROOF_RENAME_MODE=mode)
        result = subprocess.run([NODE, str(cli), "lifecycle", *args], cwd=REPO, env=env,
                                capture_output=True, text=True, timeout=180)
        try:
            output = json.loads(result.stdout)
        except json.JSONDecodeError:
            output = {}
        return result, output

    def update(self, operation, version, fault=None):
        return self.call(HEAD, ["update", "--operation-id", operation, "--artifact", str(HEAD),
                                "--artifact-version", version], fault)

    def prune(self, operation, fault=None):
        return self.call(HEAD, ["snapshots", "prune", "--keep", "1", "--operation-id", operation, "--apply"], fault)

    def seed(self):
        for operation, version in (("a1", "1.0.1"), ("a2", "1.0.2"), ("a3", "1.0.3")):
            result, _ = self.update(operation, version)
            require(result.returncode == 0, f"head update {operation} failed: {result.stderr[-2500:]} stdout={result.stdout[-500:]}")

    def expire_lease(self):
        authority = self.collector / "lifecycle-authority"
        for path in authority.rglob("*") if authority.exists() else ():
            if path.is_file() and path.suffix == ".json":
                try:
                    value = read_json(path)
                    if value.get("state") == "held" and "expiresAtMs" in value:
                        value["expiresAtMs"] = int(time.time() * 1000) - 1000
                        path.write_text(json.dumps(value) + "\n")
                except (ValueError, TypeError):
                    pass

    def lose_newest_way_back(self):
        executable = Path(read_json(self.life / "snapshots/a3/snapshot.json")["currentExecutable"])
        shutil.rmtree(executable.parents[2])

    def census(self):
        observed = {}
        for name in ("snapshots", "versions", "trash", "removals"):
            base = self.life / name
            for entry in sorted(base.rglob("*")) if base.exists() else ():
                relative = f"{name}/{entry.relative_to(base)}"
                if entry.is_symlink():
                    observed[relative] = ["link", os.readlink(entry)]
                elif entry.is_file():
                    observed[relative] = ["file", digest(entry.read_bytes())]
                elif entry.is_dir():
                    observed[relative] = ["dir"]
        return observed

    def assert_marked_record(self):
        records = list((self.life / "removals").glob("*.json"))
        require(len(records) == 1, f"expected one pending removal, found {len(records)}")
        value = read_json(records[0])
        require(sorted(value) == ["items", "operationId", "requiresCliVersion", "schemaVersion"] and
                value["requiresCliVersion"] == "0.7.41", "head did not fence its removal record")


def scenario(fixture, name):
    fixture.seed()
    if name == "interrupted_update":
        fault = (r"/snapshots/a2$", r"/trash/snapshot\+a2\+[0-9a-f]+$", "kill-before")
        result, _ = fixture.update("a4", "1.0.4", fault)
        require(result.returncode == -9, f"update retention fault did not fire: {result.returncode} {result.stderr[-400:]}")
    else:
        if name in ("interrupted_prune", "interrupted_prune_same_id"):
            fault = (r"/snapshots/a2$", r"/trash/snapshot\+a2\+[0-9a-f]+$", "kill-before")
        else:
            fault = (r"/versions/1\.0\.1$", r"/trash/runtime_version\+1\.0\.1\+[0-9a-f]+$", "kill-after")
        result, _ = fixture.prune("head-prune", fault)
        require(result.returncode == -9, f"prune fault did not fire: {result.returncode} {result.stderr[-400:]}")
    fixture.expire_lease()
    if name in ("pending_restore", "both_in_trash"):
        fixture.lose_newest_way_back()
        if name == "pending_restore":
            fault = (r"/trash/runtime_version\+1\.0\.1\+[0-9a-f]+$", r"/versions/1\.0\.1$", "throw")
            retry, _ = fixture.prune("head-retry", fault)
            require(retry.returncode != 0 and "needed_restore_incomplete" in retry.stderr,
                    f"head restore interruption was not refused: {retry.returncode} {retry.stderr[-400:]}")
            require((fixture.life / "snapshots/a2").is_dir() and not (fixture.life / "versions/1.0.1").exists(),
                    "pending-restore state was not created")
        else:
            require(any(p.name.startswith("snapshot+a2+") for p in files(fixture.life / "trash")) and
                    any(p.name.startswith("runtime_version+1.0.1+") for p in files(fixture.life / "trash")),
                    "both-in-trash state was not created")
    fixture.assert_marked_record()


def main():
    require(NODE is not None and HEAD.is_file() and HOOK.is_file(), "Node, head build or fixture hook missing")
    require(read_json(REPO / "packages/collector-cli/package.json")["version"] == "0.7.43", "head package version changed")
    temp = Path(os.environ["TMPDIR"]).resolve()
    require(temp.is_dir(), "CI TMPDIR is missing")
    # Packaged lifecycle paths require the install tree to be a strict child
    # of the common ownership root. Keep fixture homes beside this checkout,
    # while the proof process still runs under CI's synthetic HOME and TMPDIR.
    fixture_parent = REPO.parent if temp == REPO or REPO in temp.parents else temp
    with tempfile.TemporaryDirectory(prefix="plimsoll-cross-version-", dir=fixture_parent) as directory:
        root = Path(directory)
        clis = released_clis(root)
        checks = []
        for name in SCENES:
            for version, cli in clis.items():
                with tempfile.TemporaryDirectory(prefix=f"{name}-{version}-", dir=root) as fixture_root:
                    fixture = Fixture(Path(fixture_root))
                    scenario(fixture, name)
                    before = fixture.census()
                    operation = (read_json(next((fixture.life / "removals").glob("*.json")))["operationId"]
                                 if name == "interrupted_prune_same_id" else f"older-{version.replace('.', '')}-{name}")
                    result, output = fixture.call(cli, ["snapshots", "prune", "--keep", "1", "--operation-id",
                                                        operation, "--apply"])
                    after = fixture.census()
                    retention = output.get("retention") or (output.get("receipt") or {}).get("retention") or {}
                    ok = (result.returncode == 0 and retention.get("status") == "skipped" and
                          retention.get("skippedReason") == "removal_record_unreadable" and
                          retention.get("removed") == [] and before == after)
                    checks.append({"scenario": name, "release": version, "passed": ok,
                                   "exit": result.returncode, "status": retention.get("status"),
                                   "reason": retention.get("skippedReason"), "protectedEntries": len(before),
                                   "byteCensusUnchanged": before == after})
                    print(f"{'PASS' if ok else 'FAIL'} {name} {version}: {checks[-1]}", flush=True)
        summary = {"proof": "lifecycle-cross-version", "checks": checks, "passed": sum(c["passed"] for c in checks),
                   "total": len(checks), "liveStateTouched": False}
        print(json.dumps(summary), flush=True)
        require(summary["passed"] == summary["total"] == 15, "released CLI matrix failed")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"FAIL lifecycle cross-version proof: {error}", file=sys.stderr, flush=True)
        sys.exit(1)
