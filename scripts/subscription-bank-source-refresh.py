#!/usr/bin/env python3
"""Read the approved local tracker only; replace one private BANK snapshot.

No collector main, provider client, balance read, DB write, or scheduler. See
docs/subscription-bank-source-refresh.md for the trust and provenance boundaries.
"""

import argparse
import contextlib
import copy
import datetime as dt
from decimal import Decimal, InvalidOperation
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import secrets
import stat
import subprocess
import sys
import tempfile


DEFAULT_SOURCE = "company-tracker-chase-20261014"
PSQL = "/opt/homebrew/opt/postgresql@15/bin/psql"
INPUT_NAME = "subscription-reconciliation-input.json"
LOCK_NAME = ".subscription-bank-source-refresh.lock"
MAX_PROVIDER_ROWS = 5000
MAX_BANK_ROWS = 2000  # The existing reconciliation contract is stricter.
MAX_BYTES = 16 * 1024 * 1024
MAX_TEXT = 1024
MAX_SAFE_INTEGER = 9007199254740991
UTC = dt.timezone.utc
REF = r"[A-Za-z0-9][A-Za-z0-9._:-]{0,191}"
NATIVE = r"native:(?:[a-f0-9]{32}|[a-f0-9]{64})"
PAYER = r"(?:native|payer|bank):(?:[a-f0-9]{32}|[a-f0-9]{64})"
EVIDENCE = r"sha256:[a-f0-9]{64}"
UUID = r"[a-fA-F0-9]{8}(?:-[a-fA-F0-9]{4}){3}-[a-fA-F0-9]{12}"
PROVIDER_PATTERNS = {
    "openai": r"openai|chatgpt",
    "anthropic": r"anthropic|claude",
    "cursor": r"cursor",
    "xai": r"grok|(^|[^a-z0-9_])xai([^a-z0-9_]|$)",
}
PROVIDER_FILTER = "(" + "|".join(PROVIDER_PATTERNS.values()) + ")"
KINDS = {"MONTHLY_SUBSCRIPTION", "UPGRADE_PRORATION", "USAGE_RESET", "API_CONSOLE", "REFUND", "CREDIT", "UNKNOWN"}
# Explicit supported denominations, never a fallback. Other codes are held.
SUPPORTED_CURRENCIES = set("USD EUR GBP CAD AUD NZD CHF JPY CNY HKD SGD INR BRL MXN SEK NOK DKK PLN CZK HUF RON ZAR KRW TWD ILS AED SAR TRY THB MYR IDR PHP VND".split())


class RefreshError(Exception):
    """Only fixed, developer-authored codes may reach the CLI."""

    def __init__(self, code, summary=None):
        super().__init__(code)
        self.code = code
        self.summary = summary


def require(condition, code="INPUT_INVALID"):
    if not condition:
        raise RefreshError(code)


def matches(value, pattern):
    return isinstance(value, str) and re.fullmatch(pattern, value, flags=re.ASCII) is not None


def integer(value, minimum=-MAX_SAFE_INTEGER):
    require(type(value) is int and minimum <= value <= MAX_SAFE_INTEGER)
    return value


def day(value):
    require(matches(value, r"\d{4}-\d{2}-\d{2}"))
    try:
        require(dt.date.fromisoformat(value).isoformat() == value)
    except ValueError:
        raise RefreshError("INPUT_INVALID") from None
    return value


def instant(value):
    require(matches(value, r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?Z"))
    try:
        return dt.datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        raise RefreshError("INPUT_INVALID") from None


def now_utc():
    return dt.datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def digest(value):
    return hashlib.sha256(value).hexdigest()


def pseudonym(prefix, value):
    return prefix + digest(str(value).encode("utf-8"))


def json_bytes(value):
    return (json.dumps(value, sort_keys=True, ensure_ascii=True, allow_nan=False,
                       separators=(",", ":")) + "\n").encode("utf-8")


def parse_json(data, code):
    # Duplicate keys and NaN/Infinity must not be silently normalized.
    def pairs(entries):
        result = {}
        for key, value in entries:
            require(key not in result, code)
            result[key] = value
        return result

    def constant(_):
        raise RefreshError(code)

    try:
        require(len(data) <= MAX_BYTES, code)
        return json.loads(data, object_pairs_hook=pairs, parse_constant=constant)
    except (ValueError, UnicodeError, RecursionError):
        raise RefreshError(code) from None


def validate_document(value):
    """Validate the existing v1 wire contract without normalizing preserved rows.

    This is structural validation, not reconciliation or a new matching engine.
    The offline proof also feeds successful outputs to the actual TS parser.
    """
    def obj(v, required, optional=""):
        require(type(v) is dict)
        require(set(required.split()) <= v.keys() <= set((required + " " + optional).split()))

    def array(v, maximum):
        require(type(v) is list and len(v) <= maximum)

    def text(v, pattern=REF):
        require(matches(v, pattern))

    def nullable(v, check):
        if v is not None:
            check(v)

    def evidence(v, empty=False):
        array(v, 64)
        require(empty or len(v) > 0)
        for item in v:
            text(item, EVIDENCE)

    def provider(v):
        text(v, r"[a-z][a-z0-9_-]{0,63}")

    def charge_date(v):
        (day if isinstance(v, str) and len(v) == 10 else instant)(v)

    obj(value, "schema observedAt sources invoices bankTransactions", "cardAliases")
    require(value["schema"] == "subscription-reconciliation/v1")
    observed = instant(value["observedAt"])
    array(value["sources"], 256)
    sources = {}
    for source in value["sources"]:
        obj(source, "sourceRef type readAt lastSyncedAt providerObservedAt coverage evidenceRefs", "provider accountRef")
        text(source["sourceRef"])
        require(source["sourceRef"] not in sources)
        require(source["type"] in ("BANK", "INVOICES"))
        nullable(source.get("provider"), provider)
        nullable(source.get("accountRef"), lambda v: text(v, NATIVE))
        read = instant(source["readAt"])
        require(read <= observed)
        for key in ("lastSyncedAt", "providerObservedAt"):
            if source[key] is not None:
                require(instant(source[key]) <= read)
        evidence(source["evidenceRefs"])
        coverage = source["coverage"]
        obj(coverage, "state periodStart periodEnd")
        require(coverage["state"] in ("COMPLETE", "PARTIAL", "UNKNOWN"))
        start, end = coverage["periodStart"], coverage["periodEnd"]
        nullable(start, day)
        nullable(end, day)
        require(coverage["state"] != "COMPLETE" or bool(start and end))
        require(not (start and end) or start < end)
        sources[source["sourceRef"]] = source
    common = "sourceRef revision sourceObservedAt provider amountCents currency evidenceRefs"
    optional = "accountRef accountIdentityState accountEvidenceRefs payerRef paymentReference"
    for name, source_type in (("invoices", "INVOICES"), ("bankTransactions", "BANK")):
        array(value[name], 2000)
        for row in value[name]:
            is_bank = source_type == "BANK"
            obj(row, common + (" transactionRef state transactedAt postedAt" if is_bank else " invoiceRef status paidAt invoiceDate kind"),
                optional + (" kind cardLast4" if is_bank else " paymentLast4"))
            text(row["sourceRef"])
            text(row["transactionRef" if is_bank else "invoiceRef"])
            integer(row["revision"], 1)
            integer(row["amountCents"])
            text(row["currency"], r"[A-Z]{3}")
            (lambda v: nullable(v, provider) if is_bank else provider(v))(row["provider"])
            nullable(row.get("payerRef"), lambda v: text(v, PAYER))
            nullable(row.get("paymentReference"), text)
            nullable(row.get("accountRef"), lambda v: text(v, NATIVE))
            require(row.get("accountIdentityState", "UNKNOWN") in ("UNKNOWN", "VERIFIED"))
            evidence(row.get("accountEvidenceRefs", []), True)
            require(row.get("accountIdentityState") != "VERIFIED" or bool(row.get("accountRef") and row.get("accountEvidenceRefs")))
            evidence(row["evidenceRefs"])
            kind = row.get("kind")
            require(kind in KINDS or is_bank and kind is None)
            nullable(row.get("cardLast4" if is_bank else "paymentLast4"), lambda v: text(v, r"[0-9]{4}"))
            if is_bank:
                require(row["state"] in ("POSTED", "PENDING", "REMOVED"))
                nullable(row["transactedAt"], charge_date)
                nullable(row["postedAt"], charge_date)
            else:
                require(row["status"] in ("PAID", "OPEN", "VOID", "REFUNDED", "CREDIT", "UNKNOWN"))
                nullable(row["paidAt"], charge_date)
                nullable(row["invoiceDate"], day)
            source = sources.get(row["sourceRef"])
            require(source is not None and source["type"] == source_type)
            require(instant(row["sourceObservedAt"]) <= instant(source["readAt"]))
            for key in ("provider", "accountRef"):
                require(not (source.get(key) and row.get(key)) or source[key] == row[key])
    aliases = value.get("cardAliases", [])
    array(aliases, 256)
    for alias in aliases:
        obj(alias, "provider payerRef invoiceLast4 bankLast4 validFrom validThrough evidenceRefs")
        provider(alias["provider"])
        text(alias["payerRef"], PAYER)
        for key in ("invoiceLast4", "bankLast4"):
            text(alias[key], r"[0-9]{4}")
        require(alias["invoiceLast4"] != alias["bankLast4"])
        require(instant(alias["validFrom"]) < instant(alias["validThrough"]))
        evidence(alias["evidenceRefs"])


def build_sql(period_start):
    # The only interpolated value is a strictly validated ISO day. No SQL from
    # source refs, environment, merchant text, IDs, or command-line text.
    day(period_start)
    return f"""BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL TIME ZONE 'UTC';
SET LOCAL statement_timeout = '10s';
SET LOCAL DateStyle = 'ISO, YMD';
WITH bounds AS (
  SELECT DATE '{period_start}' AS start_day,
         ((CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date + 1) AS end_day
), selected AS (
  SELECT t.id::text AS id, fa.id::text AS financial_account_id,
         left(t.external_transaction_id, 1025) AS external_transaction_id,
         left(t.amount::text, 129) AS amount, left(t.currency, 4) AS currency,
         left(t.merchant_name, 1025) AS merchant_name,
         left(t.description, 1025) AS description,
         t.transacted_at::text AS transacted_at, t.posted_at::text AS posted_at,
         t.pending, t.provider_revision, t.provider_removed_at::text AS provider_removed_at,
         left(fa.mask, 5) AS mask
  FROM transactions t
  LEFT JOIN financial_accounts fa ON fa.id = t.financial_account_id
  CROSS JOIN bounds b
  WHERE (coalesce(t.merchant_name, '') || ' ' || coalesce(t.description, '')) ~* '{PROVIDER_FILTER}'
    AND ((t.transacted_at >= b.start_day AND t.transacted_at < b.end_day)
      OR (t.posted_at >= b.start_day AND t.posted_at < b.end_day))
  ORDER BY t.id
  LIMIT 5001
), sync AS (
  SELECT count(*) AS item_count,
         count(*) FILTER (WHERE status = 'healthy') AS healthy_count,
         count(*) FILTER (WHERE status = 'healthy' AND last_successful_sync_at IS NOT NULL) AS synced_healthy_count,
         to_char(min(last_successful_sync_at) FILTER (WHERE status = 'healthy'),
                 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS oldest_successful_sync_at
  FROM plaid_items
)
SELECT json_build_object(
  'schema', 'subscription-bank-db-export/v1',
  'readAt', to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
  'periodStart', b.start_day::text, 'periodEnd', b.end_day::text,
  'sync', (SELECT row_to_json(sync) FROM sync),
  'transactions', coalesce((SELECT json_agg(row_to_json(selected) ORDER BY id) FROM selected), '[]'::json)
) FROM bounds b;
ROLLBACK;
"""


def approved_environment(home):
    """Only the approved function handles credentials; never read runtime.env."""
    helper = home / "Library/Memory/ops/manager/bank_collect.py"
    runtime_env = home / "Library/Memory/ops/finance-daily-sync/runtime/company-expense-tracker/.local-runtime/runtime.env"
    module_name = "_subscription_bank_approved_environment"
    old_bytecode = sys.dont_write_bytecode
    try:
        sys.dont_write_bytecode = True
        spec = importlib.util.spec_from_file_location(module_name, helper)
        require(spec is not None and spec.loader is not None, "DB_ENVIRONMENT")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)  # Its __main__ is never executed.
        environment = module.pg_environment(Path(runtime_env))
        require(type(environment) is dict and all(isinstance(k, str) and isinstance(v, str) for k, v in environment.items()), "DB_ENVIRONMENT")
        environment = dict(environment)
        host = environment.get("PGHOST", "")
        require((host in ("", "localhost", "127.0.0.1", "::1") or host.startswith("/"))
                and not any(c in host for c in (",", "\n", "\r")), "DB_NOT_LOCAL")
        require(environment.get("PGHOSTADDR", "") in ("", "127.0.0.1", "::1"), "DB_NOT_LOCAL")
        require(not environment.get("PGSERVICE") and not environment.get("PGSERVICEFILE"), "DB_ENVIRONMENT")
        # libpq accepts connection strings in PGDATABASE; refuse such overrides.
        require("=" not in environment.get("PGDATABASE", "") and "://" not in environment.get("PGDATABASE", ""), "DB_ENVIRONMENT")
        environment["PGOPTIONS"] = "-c default_transaction_read_only=on -c timezone=UTC -c statement_timeout=10000"
        environment["PGCONNECT_TIMEOUT"] = "5"
        environment["LC_ALL"] = "C"
        environment["TZ"] = "UTC"
        return environment
    except RefreshError:
        raise
    except Exception:
        raise RefreshError("DB_ENVIRONMENT") from None
    finally:
        sys.dont_write_bytecode = old_bytecode
        sys.modules.pop(module_name, None)


def read_database(home, period_start):
    environment = approved_environment(home)
    try:
        # stdout stays in an unnamed temporary file, never a credential log.
        # SQL caps rows and text; the independent byte cap also fails closed.
        with tempfile.TemporaryFile(mode="w+b") as output:
            result = subprocess.run(
                [PSQL, "-X", "-w", "-q", "-A", "-t", "-P", "pager=off", "-v", "ON_ERROR_STOP=1"],
                input=build_sql(period_start).encode("ascii"), env=environment,
                stdout=output, stderr=subprocess.DEVNULL, timeout=20, check=False,
            )
            require(result.returncode == 0, "DB_READ_FAILED")
            require(output.tell() <= MAX_BYTES, "EXPORT_TOO_LARGE")
            output.seek(0)
            data = output.read(MAX_BYTES + 1)
        return parse_json(data, "DB_EXPORT_INVALID")
    except subprocess.TimeoutExpired:
        raise RefreshError("DB_READ_TIMEOUT") from None
    except OSError:
        raise RefreshError("DB_READ_FAILED") from None


def cents(value):
    require(matches(value, r"-?\d+(?:\.\d+)?") and len(value) <= 128, "ROW_AMOUNT")
    try:
        # Decimal.as_integer_ratio avoids the ambient decimal context and floats.
        numerator, denominator = Decimal(value).as_integer_ratio()
        scaled, remainder = divmod(numerator * 100, denominator)
        require(remainder == 0 and abs(scaled) <= MAX_SAFE_INTEGER, "ROW_AMOUNT")
        return scaled
    except (InvalidOperation, ValueError, OverflowError):
        raise RefreshError("ROW_AMOUNT") from None


def calendar_day(value):
    if value is None:
        return None
    require(matches(value, r"\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?)?"), "ROW_DATE")
    try:
        # PostgreSQL omits trailing fractional zeros. Python 3.9 accepts only
        # three/six fractional digits, so pad the validation copy only. The
        # source spelling is retained in removal evidence; charge dates stay days.
        validation = re.sub(r"\.(\d{1,6})$", lambda m: "." + m.group(1).ljust(6, "0"), value)
        dt.datetime.fromisoformat(validation)
        return day(value[:10])
    except (ValueError, RefreshError):
        raise RefreshError("ROW_DATE") from None


def classify(merchant, description):
    text = (merchant or "") + " " + (description or "")
    providers = [name for name, pattern in PROVIDER_PATTERNS.items()
                 if re.search(pattern, text, flags=re.IGNORECASE | re.ASCII)]
    provider = providers[0] if len(providers) == 1 else None
    reset = re.search(r"(?<![a-z0-9_])reset(?![a-z0-9_])", text, flags=re.IGNORECASE | re.ASCII) is not None
    return provider, "USAGE_RESET" if provider == "openai" and reset else "UNKNOWN"


def prepare_export(export, source_ref, period_start, processing_time):
    """Return sanitized evidence and candidate rows; never retain free text/IDs."""
    require(type(export) is dict and set(export) == {"schema", "readAt", "periodStart", "periodEnd", "sync", "transactions"}, "DB_EXPORT_INVALID")
    require(export["schema"] == "subscription-bank-db-export/v1", "DB_EXPORT_INVALID")
    read_at = export["readAt"]
    read_time = instant(read_at)
    require(read_time <= instant(processing_time), "CLOCK_ORDER")
    end = day(export["periodEnd"])
    require(export["periodStart"] == period_start and period_start < end, "PERIOD_INVALID")
    require(end == (read_time.date() + dt.timedelta(days=1)).isoformat(), "PERIOD_INVALID")
    sync = export["sync"]
    require(type(sync) is dict and set(sync) == {"item_count", "healthy_count", "synced_healthy_count", "oldest_successful_sync_at"}, "DB_EXPORT_INVALID")
    total, healthy, synced = [integer(sync[key], 0) for key in ("item_count", "healthy_count", "synced_healthy_count")]
    require(synced <= healthy <= total, "DB_EXPORT_INVALID")
    oldest = sync["oldest_successful_sync_at"]
    require((oldest is not None) == (synced > 0), "DB_EXPORT_INVALID")
    if oldest is not None:
        require(instant(oldest) <= read_time, "CLOCK_ORDER")
    last_sync = oldest if total > 0 and total == healthy == synced else None
    transactions = export["transactions"]
    require(type(transactions) is list, "DB_EXPORT_INVALID")
    require(len(transactions) <= MAX_PROVIDER_ROWS, "PROVIDER_ROW_LIMIT")
    fields = set("id financial_account_id external_transaction_id amount currency merchant_name description transacted_at posted_at pending provider_revision provider_removed_at mask".split())
    observations, rows, seen = [], [], set()
    held = 0
    for item in transactions:
        require(type(item) is dict and set(item) == fields, "DB_EXPORT_INVALID")
        observation, issues = {}, []

        def checked(key, convert):
            try:
                return convert(item[key])
            except RefreshError as error:
                issues.append(error.code if error.code.startswith("ROW_") else "ROW_INVALID")
                return None

        def native(value, prefix, nullable=False):
            if nullable and value is None:
                return None
            require(matches(value, UUID), "ROW_ID")
            return pseudonym(prefix, value)

        transaction_ref = checked("id", lambda v: native(v, "banktx:"))
        payer_ref = checked("financial_account_id", lambda v: native(v, "bank:", True))
        if transaction_ref in seen:
            issues.append("ROW_DUPLICATE_ID")
        seen.add(transaction_ref)
        observation["transactionRef"] = transaction_ref
        observation["payerRef"] = payer_ref

        def external(v):
            require(v is None or isinstance(v, str) and len(v) <= MAX_TEXT, "ROW_TEXT")
            return None if v is None else pseudonym("sha256:", v)

        observation["externalIdDigest"] = checked("external_transaction_id", external)
        amount_cents = checked("amount", cents)
        observation["amount"] = item["amount"] if amount_cents is not None else None
        observation["amountCents"] = amount_cents
        def denomination(v):
            require(matches(v, r"[A-Z]{3}") and v in SUPPORTED_CURRENCIES, "ROW_CURRENCY")
            return v
        currency = checked("currency", denomination)
        observation["currency"] = currency
        transacted = checked("transacted_at", calendar_day)
        posted = checked("posted_at", calendar_day)
        observation["transactedAt"], observation["postedAt"] = transacted, posted
        if not any(v is not None and period_start <= v < end for v in (transacted, posted)):
            issues.append("ROW_PERIOD_UNKNOWN" if transacted is None and posted is None else "ROW_OUTSIDE_PERIOD")
        removed = checked("provider_removed_at", lambda v: None if v is None else (calendar_day(v), v)[1])
        observation["providerRemovedAtNaive"] = removed
        pending = checked("pending", lambda v: v if type(v) is bool else (_ for _ in ()).throw(RefreshError("ROW_STATE")))
        revision = checked("provider_revision", lambda v: integer(v, 1))
        state = "REMOVED" if item["provider_removed_at"] is not None else ("PENDING" if pending is True else "POSTED" if pending is False else None)
        observation.update(pending=pending, revision=revision, state=state)
        names = []
        for key in ("merchant_name", "description"):
            def provider_text(v):
                require(v is None or isinstance(v, str) and len(v) <= MAX_TEXT, "ROW_TEXT")
                return v
            names.append(checked(key, provider_text))
            observation["merchantDigest" if key == "merchant_name" else "descriptionDigest"] = (
                pseudonym("sha256:", item[key]) if isinstance(item[key], str) and len(item[key]) <= MAX_TEXT else None)
        provider, kind = classify(*names)
        observation["provider"], observation["kind"] = provider, kind
        # An account mask does not prove the card number used for a payment.
        observation["mask4"] = item["mask"] if matches(item["mask"], r"[0-9]{4}") else None
        observation["accountIdentityState"] = "UNKNOWN"
        observation["issues"] = sorted(set(issues))
        observations.append(observation)
        if issues:
            continue
        if provider is None or transacted is None or posted is None or payer_ref is None:
            held += 1
        rows.append({"sourceRef": source_ref, "transactionRef": transaction_ref,
                     "revision": revision, "sourceObservedAt": read_at, "provider": provider,
                     "amountCents": amount_cents, "currency": currency, "state": state,
                     "transactedAt": transacted, "postedAt": posted, "payerRef": payer_ref,
                     "cardLast4": None, "kind": kind, "accountRef": None,
                     "accountIdentityState": "UNKNOWN", "accountEvidenceRefs": []})
    raw = {"schema": "subscription-bank-source-export/v1", "sourceRef": source_ref,
           "readAt": read_at, "lastSyncedAt": last_sync, "providerObservedAt": None,
           "coverage": {"state": "PARTIAL", "periodStart": period_start, "periodEnd": end},
           "sync": copy.deepcopy(sync), "observations": observations,
           "exclusions": {"count": len(transactions) - len(rows), "state": "UNKNOWN"},
           "accountIdentityState": "UNKNOWN", "accountCompletenessState": "UNKNOWN", "bankCompletenessState": "UNKNOWN",
           "fleetCompletenessState": "UNKNOWN"}
    summary = {"status": "UPDATED", "sourceRef": source_ref, "exportedAt": read_at,
               "providerRows": len(transactions), "includedRows": len(rows), "heldRows": held,
               "excludedRows": len(transactions) - len(rows), "exclusionsState": "UNKNOWN"}
    return raw, rows, summary


def open_directory(path, private=False):
    """Walk with O_NOFOLLOW, anchoring all later file operations to a dirfd."""
    require(path.is_absolute() and ".." not in path.parts, "PRIVATE_PATH")
    descriptor = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    try:
        for component in path.parts[1:]:
            child = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        info = os.fstat(descriptor)
        require(info.st_uid == os.getuid(), "PRIVATE_OWNER")
        require(not private or stat.S_IMODE(info.st_mode) == 0o700, "PRIVATE_MODE")
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def private_file(info):
    require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1, "PRIVATE_FILE")
    require(info.st_uid == os.getuid(), "PRIVATE_OWNER")
    require(stat.S_IMODE(info.st_mode) == 0o600, "PRIVATE_MODE")


def read_private(directory, name):
    descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
    try:
        info = os.fstat(descriptor)
        private_file(info)
        require(info.st_size <= MAX_BYTES, "INPUT_TOO_LARGE")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            data = stream.read(MAX_BYTES + 1)
        require(len(data) <= MAX_BYTES, "INPUT_TOO_LARGE")
        after = os.fstat(descriptor)
        signature = (info.st_dev, info.st_ino, info.st_mtime_ns, info.st_ctime_ns, info.st_size)
        require(signature == (after.st_dev, after.st_ino, after.st_mtime_ns, after.st_ctime_ns, after.st_size), "INPUT_CHANGED")
        return data, (signature, digest(data))
    finally:
        os.close(descriptor)


def child_directory(parent, name):
    try:
        os.mkdir(name, 0o700, dir_fd=parent)
    except FileExistsError:
        pass
    descriptor = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
    try:
        info = os.fstat(descriptor)
        require(info.st_uid == os.getuid() and stat.S_IMODE(info.st_mode) == 0o700, "PRIVATE_DIRECTORY")
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def staged_file(directory, data):
    require(len(data) <= MAX_BYTES, "INPUT_TOO_LARGE")
    name = ".subscription-bank-" + secrets.token_hex(16) + ".tmp"
    descriptor = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=directory)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        return name
    except BaseException:
        os.unlink(name, dir_fd=directory)
        raise


def save_evidence(directory, raw):
    data = json_bytes(raw)
    hexdigest = digest(data)
    base = child_directory(directory, "subscription-reconciliation-inputs")
    try:
        bank = child_directory(base, "bank")
        try:
            name = hexdigest + ".json"
            temporary = staged_file(bank, data)
            try:
                try:
                    os.link(temporary, name, src_dir_fd=bank, dst_dir_fd=bank, follow_symlinks=False)
                except FileExistsError:
                    require(read_private(bank, name)[0] == data, "EVIDENCE_CONFLICT")
            finally:
                os.unlink(temporary, dir_fd=bank)
            os.fsync(bank)
        finally:
            os.close(bank)
        os.fsync(base)
    finally:
        os.close(base)
    os.fsync(directory)
    return "sha256:" + hexdigest


def refresh(home, source_ref):
    require(matches(source_ref, REF), "SOURCE_REF_INVALID")
    root_path = home / "Library/Application Support/Plimsoll"
    root = open_directory(root_path, private=True)
    lock = None
    try:
        lock = os.open(LOCK_NAME, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600, dir_fd=root)
        private_file(os.fstat(lock))
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RefreshError("REFRESH_LOCKED") from None
        original, signature = read_private(root, INPUT_NAME)
        document = parse_json(original, "INPUT_INVALID")
        validate_document(document)
        selected = [s for s in document["sources"] if s["sourceRef"] == source_ref and s["type"] == "BANK"]
        require(len(selected) == 1, "BANK_SOURCE_NOT_UNIQUE")
        source = selected[0]
        period_start = day(source["coverage"]["periodStart"])
        require(period_start <= dt.datetime.now(UTC).date().isoformat(), "PERIOD_INVALID")
        export = read_database(home, period_start)
        processing_time = now_utc()
        raw, rows, summary = prepare_export(export, source_ref, period_start, processing_time)
        require(instant(raw["readAt"]) >= instant(source["readAt"]), "CLOCK_ORDER")
        require(instant(document["observedAt"]) <= instant(processing_time), "CLOCK_ORDER")
        evidence_ref = save_evidence(root, raw)
        if summary["excludedRows"]:
            summary.update(status="UNKNOWN", code="ROW_QUALITY")
            raise RefreshError("ROW_QUALITY", summary)
        for row in rows:
            row["evidenceRefs"] = [evidence_ref]
        updated = copy.deepcopy(document)
        updated["observedAt"] = processing_time
        new_source = {"sourceRef": source_ref, "type": "BANK", "provider": None, "accountRef": None,
                      "readAt": raw["readAt"], "lastSyncedAt": raw["lastSyncedAt"],
                      "providerObservedAt": None, "coverage": raw["coverage"], "evidenceRefs": [evidence_ref]}
        updated["sources"] = [new_source if s["sourceRef"] == source_ref else s for s in updated["sources"]]
        updated["bankTransactions"] = [r for r in updated["bankTransactions"] if r["sourceRef"] != source_ref] + rows
        require(len(updated["bankTransactions"]) <= MAX_BANK_ROWS, "CONTRACT_ROW_LIMIT")
        validate_document(updated)
        data = json_bytes(updated)
        temporary = staged_file(root, data)
        committed = False
        try:
            # Detect changes by other writers during the DB read, including a
            # replaced lock/path. Never unlink the persistent lock inode.
            check_root = open_directory(root_path, private=True)
            try:
                require((os.fstat(root).st_dev, os.fstat(root).st_ino) ==
                        (os.fstat(check_root).st_dev, os.fstat(check_root).st_ino), "INPUT_CHANGED")
            finally:
                os.close(check_root)
            current_lock = os.stat(LOCK_NAME, dir_fd=root, follow_symlinks=False)
            require((current_lock.st_dev, current_lock.st_ino) == (os.fstat(lock).st_dev, os.fstat(lock).st_ino), "INPUT_CHANGED")
            require(read_private(root, INPUT_NAME)[1] == signature, "INPUT_CHANGED")
            os.replace(temporary, INPUT_NAME, src_dir_fd=root, dst_dir_fd=root)
            committed = True
            os.fsync(root)
            require(read_private(root, INPUT_NAME)[0] == data, "COMMIT_UNCERTAIN")
        except OSError:
            raise RefreshError("COMMIT_UNCERTAIN" if committed else "PRIVATE_IO") from None
        finally:
            if not committed:
                os.unlink(temporary, dir_fd=root)
        return summary
    finally:
        if lock is not None:
            os.close(lock)
        os.close(root)


class QuietParser(argparse.ArgumentParser):
    def error(self, message):
        raise RefreshError("ARGUMENTS")


def main(argv=None):
    result, exit_code = None, 0
    # Suppress helper/import/runtime diagnostics as well as psql stderr. Only
    # the deliberately constructed summary is written to the original stdout.
    with open(os.devnull, "w") as sink, contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
        try:
            parser = QuietParser(add_help=False, allow_abbrev=False)
            parser.add_argument("--source-ref", default=DEFAULT_SOURCE)
            parser.add_argument("--home", type=Path, default=Path.home())
            args = parser.parse_args(argv)
            result = refresh(args.home, args.source_ref)
        except RefreshError as error:
            result = error.summary or {"status": "ERROR", "code": error.code}
            exit_code = 1
        except (OSError, ValueError, TypeError, KeyError, RecursionError):
            result, exit_code = {"status": "ERROR", "code": "PRIVATE_OR_INVALID_INPUT"}, 1
        except Exception:
            result, exit_code = {"status": "ERROR", "code": "REFRESH_FAILED"}, 1
        except (KeyboardInterrupt, SystemExit):
            result, exit_code = {"status": "ERROR", "code": "REFRESH_INTERRUPTED"}, 1
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
