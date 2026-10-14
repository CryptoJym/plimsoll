#!/usr/bin/env python3
"""Synthetic offline proof. Never load the real bank helper or run real psql."""

import contextlib
import copy
import datetime as dt
import fcntl
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
import uuid

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("subscription_bank_refresh_proof_target", HERE / "subscription-bank-source-refresh.py")
M = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = M
SPEC.loader.exec_module(M)
ENGINE = "--engine" in sys.argv[1:]
CANARY = "SYNTHETIC_DIAGNOSTIC_NEVER_PRINT"
READ = (dt.datetime.now(M.UTC) - dt.timedelta(seconds=10)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
PROCESS = M.now_utc()
TODAY = M.instant(READ).date()
START = (TODAY - dt.timedelta(days=45)).isoformat()
END = (TODAY + dt.timedelta(days=1)).isoformat()
CHARGE = (TODAY - dt.timedelta(days=5)).isoformat()
OLD = (M.instant(READ) - dt.timedelta(days=2)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
SYNC = (M.instant(READ) - dt.timedelta(hours=4)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
EVIDENCE = "sha256:" + "a" * 64
DEFAULT_PAYLOAD = object()


def native_id(n):
    return str(uuid.UUID(int=n))


def transaction(n=1, **patch):
    row = {"id": native_id(n), "financial_account_id": native_id(9000),
           "external_transaction_id": "synthetic-external-" + str(n), "amount": "23.69", "currency": "USD",
           "merchant_name": "OpenAI", "description": "synthetic charge",
           "transacted_at": CHARGE + " 23:30:00", "posted_at": CHARGE + " 23:59:59.123456",
           "pending": False, "provider_revision": 3, "provider_removed_at": None, "mask": "4242"}
    row.update(patch)
    return row


def export(rows=None, **patch):
    result = {"schema": "subscription-bank-db-export/v1", "readAt": READ,
              "periodStart": START, "periodEnd": END,
              "sync": {"item_count": 2, "healthy_count": 2, "synced_healthy_count": 2,
                       "oldest_successful_sync_at": SYNC},
              "transactions": [transaction()] if rows is None else rows}
    result.update(patch)
    return result


def document():
    invoice_source = {"sourceRef": "owner-invoices-20261014", "type": "INVOICES", "readAt": OLD,
                      "lastSyncedAt": None, "providerObservedAt": OLD,
                      "coverage": {"state": "PARTIAL", "periodStart": START, "periodEnd": END},
                      "evidenceRefs": [EVIDENCE]}
    bank_source = {**copy.deepcopy(invoice_source), "sourceRef": M.DEFAULT_SOURCE, "type": "BANK",
                   "providerObservedAt": None}
    other_source = {**copy.deepcopy(bank_source), "sourceRef": "other-bank"}
    bank = {"sourceRef": M.DEFAULT_SOURCE, "transactionRef": M.pseudonym("banktx:", native_id(1)),
            "revision": 1, "sourceObservedAt": OLD, "provider": "openai", "amountCents": 1,
            "currency": "USD", "state": "PENDING", "transactedAt": CHARGE, "postedAt": None,
            "evidenceRefs": [EVIDENCE]}
    return {"schema": "subscription-reconciliation/v1", "observedAt": OLD,
            "sources": [invoice_source, bank_source, other_source],
            "invoices": [{"sourceRef": invoice_source["sourceRef"], "invoiceRef": "synthetic-invoice",
                          "revision": 7, "sourceObservedAt": OLD, "provider": "openai", "amountCents": 2369,
                          "currency": "USD", "status": "PAID", "paidAt": CHARGE, "invoiceDate": CHARGE,
                          "kind": "MONTHLY_SUBSCRIPTION", "paymentLast4": "1111", "evidenceRefs": [EVIDENCE]}],
            "bankTransactions": [bank, {**copy.deepcopy(bank), "sourceRef": "other-bank"}],
            "cardAliases": [{"provider": "openai", "payerRef": M.pseudonym("bank:", native_id(9000)),
                             "invoiceLast4": "1111", "bankLast4": "4242", "validFrom": START + "T00:00:00Z",
                             "validThrough": END + "T00:00:00Z", "evidenceRefs": [EVIDENCE]}]}


class Proof(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="subscription-bank-offline-")
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name).resolve()
        self.root = self.home / "Library/Application Support/Plimsoll"
        self.root.mkdir(parents=True, mode=0o700)
        self.root.chmod(0o700)
        self.input = self.root / M.INPUT_NAME
        self.before = document()
        self.write_input(self.before)
        helper = self.home / "Library/Memory/ops/manager/bank_collect.py"
        helper.parent.mkdir(parents=True)
        helper.write_text(
            "import sys\nfrom pathlib import Path\n"
            "def pg_environment(runtime_env):\n"
            "    assert isinstance(runtime_env, Path)\n"
            "    assert not runtime_env.exists()\n"
            f"    print({CANARY!r})\n    print({CANARY!r}, file=sys.stderr)\n"
            f"    return {{'PGHOST':'127.0.0.1','PGDATABASE':'synthetic','PGPASSWORD':{CANARY!r}}}\n"
            "if __name__ == '__main__':\n    raise RuntimeError('collector main forbidden')\n",
            encoding="utf-8")
        self.real_run = subprocess.run

    def write_input(self, value):
        self.input.write_bytes(M.json_bytes(value))
        self.input.chmod(0o600)

    def invoke(self, payload=DEFAULT_PAYLOAD, error=None, mutate=None, args=None, return_code=0):
        self.calls = []
        payload = export() if payload is DEFAULT_PAYLOAD else payload

        def fake_psql(command, **kwargs):
            self.calls.append((command, kwargs))
            self.assertEqual(command[0], "/opt/homebrew/opt/postgresql@15/bin/psql")
            self.assertEqual(kwargs["timeout"], 20)
            self.assertEqual(kwargs["stderr"], subprocess.DEVNULL)
            self.assertIn("-X", command)
            self.assertIn("-w", command)
            self.assertIn("ON_ERROR_STOP=1", command)
            self.assertIn("default_transaction_read_only=on", kwargs["env"]["PGOPTIONS"])
            self.assertEqual(kwargs["env"]["PGCONNECT_TIMEOUT"], "5")
            self.assertNotIn(CANARY, str(command))
            sql = kwargs["input"].decode("ascii")
            self.assertIn("READ ONLY;", sql)
            self.assertIn("SET LOCAL TIME ZONE 'UTC';", sql)
            self.assertIn("statement_timeout = '10s'", sql)
            self.assertIn("LIMIT 5001", sql)
            self.assertIn("ROLLBACK;", sql)
            self.assertNotIn("balance", sql.lower())
            self.assertNotIn("metadata", sql.lower())
            self.assertNotIn("provider_account_id", sql.lower())
            self.assertNotIn("access_token", sql.lower())
            self.assertNotIn("INSERT", sql)
            self.assertNotIn("UPDATE", sql)
            self.assertIn("status = 'healthy'", sql)
            self.assertIn("min(last_successful_sync_at)", sql)
            if error:
                raise error
            if mutate:
                mutate()
            kwargs["stdout"].write(M.json_bytes(payload))
            kwargs["stdout"].flush()
            return subprocess.CompletedProcess(command, return_code)

        stdout, stderr = io.StringIO(), io.StringIO()
        with mock.patch.object(M.subprocess, "run", side_effect=fake_psql), contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            rc = M.main(["--home", str(self.home)] + (args or []))
        self.assertEqual(stderr.getvalue(), "")
        self.assertNotIn(CANARY, stdout.getvalue())
        summary = json.loads(stdout.getvalue())
        self.assertLessEqual(set(summary), {"status", "code", "sourceRef", "exportedAt", "providerRows", "includedRows", "heldRows", "excludedRows", "exclusionsState"})
        return rc, summary

    def raws(self):
        return list((self.root / "subscription-reconciliation-inputs/bank").glob("*.json"))

    def test_01_exact_signed_cents_without_context_rounding(self):
        for text, expected in [("23.69", 2369), ("0.01", 1), ("-0.01", -1), ("-12.34", -1234), ("0.00", 0),
                               ("1.2300", 123), ("90071992547409.91", M.MAX_SAFE_INTEGER), ("-90071992547409.91", -M.MAX_SAFE_INTEGER)]:
            self.assertEqual(M.cents(text), expected)
        for text in ["0.001", "-0.001", "NaN", "Infinity", "1e2", "1,00", " 1.00", "90071992547409.92", 23.69, None, True]:
            with self.assertRaises(M.RefreshError):
                M.cents(text)

    def test_02_closed_provider_classification_and_explicit_reset(self):
        for merchant, description, expected in [
            ("CHATGPT", "", ("openai", "UNKNOWN")), ("Claude", "", ("anthropic", "UNKNOWN")),
            ("Cursor", "", ("cursor", "UNKNOWN")), ("Grok", "", ("xai", "UNKNOWN")),
            ("XAI", "", ("xai", "UNKNOWN")), ("prexaipost", "", (None, "UNKNOWN")),
            ("_xai", "", (None, "UNKNOWN")), ("AI", "", (None, "UNKNOWN")),
            ("OpenAI", "RESET", ("openai", "USAGE_RESET")), ("OpenAI", "resetting", ("openai", "UNKNOWN")),
            ("OpenAI", "Claude RESET", (None, "UNKNOWN")), ("Claude", "RESET", ("anthropic", "UNKNOWN"))]:
            self.assertEqual(M.classify(merchant, description), expected)

    def test_03_actual_cli_flow_preserves_sources_clocks_and_aliases(self):
        rc, summary = self.invoke()
        self.assertEqual((rc, summary["status"], summary["includedRows"]), (0, "UPDATED", 1))
        after = json.loads(self.input.read_bytes())
        M.validate_document(after)
        for key in ("invoices", "cardAliases"):
            self.assertEqual(after[key], self.before[key])
        self.assertEqual([s for s in after["sources"] if s["type"] == "INVOICES"], [self.before["sources"][0]])
        self.assertEqual(after["sources"][2], self.before["sources"][2])
        self.assertEqual(after["bankTransactions"][0], self.before["bankTransactions"][1])
        source, row = after["sources"][1], after["bankTransactions"][1]
        self.assertEqual(source["readAt"], READ)
        self.assertEqual(source["lastSyncedAt"], SYNC)
        self.assertIsNone(source["providerObservedAt"])
        self.assertEqual(source["coverage"], {"state": "PARTIAL", "periodStart": START, "periodEnd": END})
        self.assertEqual(row["amountCents"], 2369)
        self.assertEqual(row["sourceObservedAt"], READ)
        self.assertEqual(row["transactedAt"], CHARGE)
        self.assertEqual(row["postedAt"], CHARGE)
        self.assertEqual(row["transactionRef"], "banktx:" + hashlib.sha256(native_id(1).encode()).hexdigest())
        self.assertEqual(row["payerRef"], "bank:" + hashlib.sha256(native_id(9000).encode()).hexdigest())
        self.assertEqual(row["accountIdentityState"], "UNKNOWN")
        self.assertIsNone(row["accountRef"])
        self.assertIsNone(row["cardLast4"])
        self.assertEqual(row["kind"], "UNKNOWN")
        raw_path = self.raws()[0]
        raw_bytes = raw_path.read_bytes()
        evidence = "sha256:" + hashlib.sha256(raw_bytes).hexdigest()
        self.assertEqual(raw_path.stem, evidence[7:])
        self.assertEqual(source["evidenceRefs"], [evidence])
        self.assertEqual(row["evidenceRefs"], [evidence])
        for path in [self.input, raw_path, self.root / M.LOCK_NAME]:
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
        for path in [self.root, raw_path.parent, raw_path.parent.parent]:
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o700)
        text = raw_bytes.decode()
        for forbidden in [native_id(1), native_id(9000), "synthetic-external-1", "synthetic charge", CANARY]:
            self.assertNotIn(forbidden, text)
        raw = json.loads(raw_bytes)
        self.assertEqual(raw["observations"][0]["externalIdDigest"], M.pseudonym("sha256:", "synthetic-external-1"))
        self.assertEqual(raw["observations"][0]["mask4"], "4242")

    def test_04_pending_removed_revisions_signed_refunds_and_no_inferred_links(self):
        rows = [transaction(1, pending=True, posted_at=None, provider_revision=4),
                transaction(2, pending=True, posted_at=None, provider_removed_at=CHARGE + " 19:00:01.123", provider_revision=9),
                transaction(3, amount="-23.69", currency="EUR", provider_revision=2)]
        self.assertEqual(self.invoke(export(rows))[0], 0)
        output = json.loads(self.input.read_bytes())["bankTransactions"][1:]
        self.assertEqual([r["state"] for r in output], ["PENDING", "REMOVED", "POSTED"])
        self.assertEqual([r["revision"] for r in output], [4, 9, 2])
        self.assertEqual(output[2]["amountCents"], -2369)
        self.assertEqual(output[2]["currency"], "EUR")
        self.assertEqual(len({r["transactionRef"] for r in output}), 3)
        self.assertEqual(json.loads(self.raws()[0].read_bytes())["observations"][1]["providerRemovedAtNaive"], CHARGE + " 19:00:01.123")

    def test_05_sync_freshness_is_not_laundered_by_read_or_processing(self):
        for sync in [
            {"item_count": 0, "healthy_count": 0, "synced_healthy_count": 0, "oldest_successful_sync_at": None},
            {"item_count": 2, "healthy_count": 1, "synced_healthy_count": 1, "oldest_successful_sync_at": SYNC},
            {"item_count": 2, "healthy_count": 2, "synced_healthy_count": 1, "oldest_successful_sync_at": SYNC}]:
            raw, rows, _ = M.prepare_export(export(sync=sync), M.DEFAULT_SOURCE, START, PROCESS)
            self.assertIsNone(raw["lastSyncedAt"])
            self.assertEqual(raw["readAt"], READ)
            self.assertEqual(rows[0]["sourceObservedAt"], READ)
        raw, _, _ = M.prepare_export(export(), M.DEFAULT_SOURCE, START, PROCESS)
        self.assertEqual(raw["lastSyncedAt"], SYNC)
        self.assertNotEqual(SYNC, READ)

    def test_06_unknown_provider_dates_and_payer_stay_unknown(self):
        rows = [transaction(1, merchant_name="OpenAI Claude", transacted_at=None, financial_account_id=None),
                transaction(2, pending=True, posted_at=None, mask="1234567890123456")]
        rc, summary = self.invoke(export(rows))
        self.assertEqual((rc, summary["heldRows"]), (0, 2))
        output = json.loads(self.input.read_bytes())["bankTransactions"][1:]
        self.assertIsNone(output[0]["provider"])
        self.assertIsNone(output[0]["transactedAt"])
        self.assertIsNone(output[0]["payerRef"])
        self.assertTrue(all(r["accountRef"] is None and r["accountIdentityState"] == "UNKNOWN" for r in output))
        self.assertNotIn("1234567890123456", self.raws()[0].read_text())

    def test_07_unsafe_rows_saved_as_sanitized_exclusions_without_input_change(self):
        original = self.input.read_bytes()
        bad_rows = [transaction(1, currency=None), transaction(2, currency="XXX"),
                    transaction(3, amount="0.001"), transaction(4, transacted_at="2026-02-30 00:00:00"),
                    transaction(5, provider_revision=0), transaction(6, pending=None),
                    transaction(7, id="private unsafe native id"), transaction(8, transacted_at=None, posted_at=None)]
        rc, summary = self.invoke(export(bad_rows))
        self.assertEqual((rc, summary["code"], summary["excludedRows"]), (1, "ROW_QUALITY", 8))
        self.assertEqual(self.input.read_bytes(), original)
        raw = json.loads(self.raws()[0].read_bytes())
        self.assertEqual(raw["exclusions"], {"count": 8, "state": "UNKNOWN"})
        self.assertTrue(all(o["issues"] for o in raw["observations"]))
        self.assertIsNone(raw["observations"][5]["state"])
        self.assertNotIn("private unsafe native id", self.raws()[0].read_text())

    def test_08_provider_bounds_5000_and_5001_and_engine_bound_2000(self):
        raw, rows, _ = M.prepare_export(export([transaction(n) for n in range(1, 5001)]), M.DEFAULT_SOURCE, START, PROCESS)
        self.assertEqual((len(raw["observations"]), len(rows)), (5000, 5000))
        original = self.input.read_bytes()
        rc, summary = self.invoke(export([transaction(n) for n in range(1, 5002)]))
        self.assertEqual((rc, summary["code"]), (1, "PROVIDER_ROW_LIMIT"))
        self.assertEqual(self.input.read_bytes(), original)
        # One preserved other-bank row leaves room for 1,999 refreshed rows.
        self.assertEqual(self.invoke(export([transaction(n) for n in range(1, 2000)]))[0], 0)
        before_limit = self.input.read_bytes()
        rc, summary = self.invoke(export([transaction(n) for n in range(1, 2001)]))
        self.assertEqual((rc, summary["code"]), (1, "CONTRACT_ROW_LIMIT"))
        self.assertEqual(self.input.read_bytes(), before_limit)

    def test_09_period_bounds_and_unsafe_query_inputs_fail_closed(self):
        sql = M.build_sql(START)
        for predicate in ["t.transacted_at >= b.start_day", "t.transacted_at < b.end_day", "t.posted_at >= b.start_day", "t.posted_at < b.end_day"]:
            self.assertIn(predicate, sql)
        for start in ["2026-02-30", "2026-01-01'; DELETE FROM transactions; --", None]:
            with self.assertRaises(M.RefreshError):
                M.build_sql(start)
        for patch in [{"periodEnd": "2099-01-01"}, {"periodStart": "2000-01-01"}, {"readAt": "2999-01-01T00:00:00Z"}]:
            with self.assertRaises(M.RefreshError):
                M.prepare_export(export(**patch), M.DEFAULT_SOURCE, START, PROCESS)
        original = self.input.read_bytes()
        self.assertEqual(self.invoke(export([transaction(transacted_at="2000-01-01", posted_at="2000-01-02")]))[1]["code"], "ROW_QUALITY")
        self.assertEqual(self.input.read_bytes(), original)

    def test_10_missing_duplicate_wrong_type_and_invalid_document_never_query(self):
        invalids = []
        value = document(); value["sources"].pop(1); invalids.append(value)
        value = document(); value["sources"].append(copy.deepcopy(value["sources"][1])); invalids.append(value)
        value = document(); value["sources"][1]["type"] = "INVOICES"; invalids.append(value)
        value = document(); value["sources"][1]["coverage"]["periodStart"] = None; invalids.append(value)
        value = document(); value["invoices"][0]["amountCents"] = 23.69; invalids.append(value)
        value = document(); value["invoices"][0]["hidden"] = CANARY; invalids.append(value)
        value = document(); value["sources"][0]["providerObservedAt"] = "2999-01-01T00:00:00Z"; invalids.append(value)
        for value in invalids:
            self.write_input(value)
            before = self.input.read_bytes()
            self.assertEqual(self.invoke()[0], 1)
            self.assertEqual(self.calls, [])
            self.assertEqual(self.input.read_bytes(), before)

    def test_11_private_modes_symlinks_hardlinks_and_owner(self):
        self.input.chmod(0o644)
        self.assertEqual(self.invoke()[0], 1)
        self.assertEqual(self.calls, [])
        self.input.chmod(0o600)
        saved = self.root / "saved.json"
        self.input.rename(saved)
        self.input.symlink_to(saved)
        self.assertEqual(self.invoke()[0], 1)
        self.assertEqual(self.calls, [])
        self.input.unlink()
        os.link(saved, self.input)
        self.assertEqual(self.invoke()[0], 1)
        self.assertEqual(self.calls, [])
        self.input.unlink(); saved.rename(self.input)
        self.root.chmod(0o755)
        self.assertEqual(self.invoke()[0], 1)
        self.root.chmod(0o700)
        uid = os.getuid()
        with mock.patch.object(M.os, "getuid", return_value=uid + 1):
            self.assertEqual(self.invoke()[0], 1)
        self.assertEqual(self.calls, [])

    def test_12_os_lock_and_noncooperating_input_change(self):
        lock_path = self.root / M.LOCK_NAME
        with lock_path.open("w") as lock:
            lock_path.chmod(0o600)
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.assertEqual(self.invoke()[1]["code"], "REFRESH_LOCKED")
            self.assertEqual(self.calls, [])
        changed = document(); changed["invoices"][0]["revision"] = 11
        rc, summary = self.invoke(mutate=lambda: self.write_input(changed))
        self.assertEqual((rc, summary["code"]), (1, "INPUT_CHANGED"))
        self.assertEqual(self.input.read_bytes(), M.json_bytes(changed))
        self.assertFalse(list(self.root.glob("*.tmp")))

    def test_13_atomic_precommit_failure_preserves_input_and_raw_is_immutable(self):
        original = self.input.read_bytes()
        with mock.patch.object(M.os, "replace", side_effect=OSError(CANARY)):
            self.assertEqual(self.invoke()[1]["code"], "PRIVATE_IO")
        self.assertEqual(self.input.read_bytes(), original)
        self.assertEqual(len(self.raws()), 1)
        self.assertEqual(self.invoke()[0], 0)  # Identical raw bytes can be reused.
        raw = self.raws()[0]; raw.write_text('{"tampered":true}')
        before = self.input.read_bytes()
        self.assertEqual(self.invoke()[1]["code"], "EVIDENCE_CONFLICT")
        self.assertEqual(self.input.read_bytes(), before)

    def test_14_read_errors_and_timeout_never_echo_diagnostics(self):
        original = self.input.read_bytes()
        for error, code in [(OSError(CANARY), "DB_READ_FAILED"),
                            (subprocess.TimeoutExpired("psql", 20, stderr=CANARY), "DB_READ_TIMEOUT")]:
            self.assertEqual(self.invoke(error=error)[1]["code"], code)
            self.assertEqual(self.input.read_bytes(), original)
        with mock.patch.object(M, "approved_environment", side_effect=RuntimeError(CANARY)):
            self.assertEqual(self.invoke()[1]["code"], "REFRESH_FAILED")
        self.assertEqual(self.input.read_bytes(), original)
        self.assertEqual(self.invoke(return_code=1)[1]["code"], "DB_READ_FAILED")
        self.assertEqual(self.input.read_bytes(), original)

    def test_15_duplicate_json_nonfinite_invalid_export_and_byte_bound(self):
        for data in [b'{"a":1,"a":2}', b'{"a":NaN}', b'{', b'\xff', b' ' * (M.MAX_BYTES + 1)]:
            with self.assertRaises(M.RefreshError):
                M.parse_json(data, "INPUT_INVALID")
        for value in [None, {**export(), "unexpected": CANARY}, export([{"invalid": CANARY}]),
                      export(sync={"item_count": 1, "healthy_count": 1, "synced_healthy_count": 1, "oldest_successful_sync_at": None}),
                      export([transaction(), transaction()])]:
            before = self.input.read_bytes()
            self.assertEqual(self.invoke(value)[0], 1)
            self.assertEqual(self.input.read_bytes(), before)
        self.input.write_bytes(b" " * (M.MAX_BYTES + 1))
        self.assertEqual(self.invoke()[1]["code"], "INPUT_TOO_LARGE")
        self.assertEqual(self.calls, [])

    def test_16_local_environment_guard_and_only_approved_function(self):
        # Fixture helper is the only import target, and runtime.env does not exist.
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            env = M.approved_environment(self.home)
        self.assertEqual(env["PGHOST"], "127.0.0.1")
        self.assertEqual(env["PGPASSWORD"], CANARY)
        self.assertFalse(list(self.home.rglob("__pycache__")))
        helper = self.home / "Library/Memory/ops/manager/bank_collect.py"
        for override in [{"PGHOST": "example.invalid"}, {"PGHOST": "/tmp,example.invalid"},
                         {"PGHOSTADDR": "192.0.2.1"}, {"PGDATABASE": "host=example.invalid"}, {"PGSERVICE": "remote"}]:
            bad_env = {"PGHOST": "127.0.0.1", "PGDATABASE": "synthetic", **override}
            helper.write_text("def pg_environment(path):\n    return " + repr(bad_env) + "\n")
            self.assertEqual(self.invoke()[0], 1)
            self.assertEqual(self.calls, [])

    @unittest.skipUnless(ENGINE, "actual TS parser requested with --engine")
    def test_17_actual_typescript_contract_parser_offline(self):
        self.assertEqual(self.invoke()[0], 0)
        node = shutil.which("node")
        self.assertIsNotNone(node, "--engine requires existing Node with type stripping")
        engine_url = (HERE.parent / "packages/collector-cli/src/subscription-reconciliation.ts").as_uri()
        javascript = "import {parseSubscriptionReconciliationInput as parse,reconcileSubscriptionCharges as reconcile} from " + json.dumps(engine_url) + ";" + (
            "let data='';for await(const chunk of process.stdin)data+=chunk;"
            "const input=JSON.parse(data);const p=parse(input);const r=reconcile(input);"
            "if(p.bankTransactions.length!==2||r.certifiedSettledTotals.length!==0)process.exit(2);"
            "if(r.bankTransactions[1].datePrecision.postedAt!=='DAY')process.exit(3);"
            "process.stdout.write('PASS');")
        result = self.real_run([node, "--disable-warning=ExperimentalWarning", "--input-type=module", "-e", javascript],
                               input=self.input.read_bytes(), stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                               timeout=20, env={"PATH": str(Path(node).parent)})
        self.assertEqual((result.returncode, result.stdout), (0, b"PASS"))

    def test_18_explicit_source_selection_and_invalid_cli(self):
        self.assertEqual(self.invoke(args=["--source-ref", "other-bank"])[0], 0)
        updated = json.loads(self.input.read_bytes())
        self.assertEqual(updated["sources"][1], self.before["sources"][1])
        self.assertEqual(updated["bankTransactions"][0], self.before["bankTransactions"][0])
        self.assertEqual(updated["bankTransactions"][1]["sourceRef"], "other-bank")
        for args in [["--source-ref", "unsafe/value"], ["--source-ref", "missing"], ["--unknown", CANARY]]:
            before = self.input.read_bytes()
            self.assertEqual(self.invoke(args=args)[0], 1)
            self.assertEqual(self.calls, [])
            self.assertEqual(self.input.read_bytes(), before)

    def test_19_empty_snapshot_remains_partial_and_preserves_other_bank(self):
        rc, summary = self.invoke(export([]))
        self.assertEqual((rc, summary["providerRows"], summary["includedRows"]), (0, 0, 0))
        output = json.loads(self.input.read_bytes())
        self.assertEqual(output["bankTransactions"], [self.before["bankTransactions"][1]])
        self.assertEqual(output["sources"][1]["coverage"]["state"], "PARTIAL")
        raw = json.loads(self.raws()[0].read_bytes())
        self.assertEqual(raw["accountCompletenessState"], "UNKNOWN")
        self.assertEqual(raw["bankCompletenessState"], "UNKNOWN")
        self.assertEqual(raw["fleetCompletenessState"], "UNKNOWN")


    def test_20_postgresql_fractional_timestamps_on_python39(self):
        for fraction in ["1", "02", "123", "1234", "12345", "123456"]:
            value = CHARGE + " 09:15:17." + fraction
            self.assertEqual(M.calendar_day(value), CHARGE)
            raw, _, summary = M.prepare_export(export([transaction(1, provider_removed_at=value)]), M.DEFAULT_SOURCE, START, PROCESS)
            self.assertEqual(summary["excludedRows"], 0)
            self.assertEqual(raw["observations"][0]["providerRemovedAtNaive"], value)
        with self.assertRaises(M.RefreshError):
            M.calendar_day("2026-02-30 09:15:17.02")


if __name__ == "__main__":
    stream = io.StringIO()
    result = unittest.TextTestRunner(stream=stream, verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Proof))
    print(json.dumps({"status": "PASS" if result.wasSuccessful() else "FAIL", "tests": result.testsRun,
                      "failures": len(result.failures), "errors": len(result.errors), "skipped": len(result.skipped),
                      "database": "NOT_CHECKED", "credentials": "NOT_READ", "engine": ENGINE}, sort_keys=True))
    if not result.wasSuccessful():
        sys.stderr.write(stream.getvalue())
    raise SystemExit(0 if result.wasSuccessful() else 1)
