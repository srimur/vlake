# -*- coding: utf-8 -*-
"""
Column-level PHI encryption for V-Lake.

Hybrid scheme per-column:
  * det   - AES-SIV deterministic encryption. Same plaintext -> same ciphertext.
            Used for high-cardinality identifiers (MRN, SSN, email, name).
            Enables equality filters and joins. Minimal frequency leak because
            values are near-unique.
  * rand  - Fernet randomized encryption. Same plaintext -> different ciphertext.
            Used for free-text and low-cardinality values where frequency
            leakage would be re-identifying (diagnosis, notes).
            No SQL filtering possible on the stored column.
  * bidx  - HMAC-based blind index, stored in a SIDE column `_bidx_<col>`.
            Computed over a COARSENED value (e.g. year-of-DOB, ZIP3, ICD-3).
            Enables equality filters at a documented, bounded frequency leak.
            The primary column is still stored under its `rand` encryption.

Key hierarchy:
  master KEK (Fernet)
    -> per-dataset DEK (already exists in app.py _dek_store)
       -> derived column keys (this module):
            - siv_key    (64 bytes, AES-SIV)
            - fernet_key (32 url-safe base64 bytes, Fernet)
            - bidx_key   (32 bytes, HMAC-SHA256)
"""
import base64, hmac, hashlib, re
from typing import Optional
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives.ciphers.aead import AESSIV


# ---------------------------------------------------------------
# Key derivation from a per-dataset DEK
# ---------------------------------------------------------------

def _hkdf_like(dek_bytes: bytes, label: bytes, length: int) -> bytes:
    """Simple HMAC-based KDF. Sufficient because the input DEK already
    carries full entropy; HKDF-expand is overkill for our needs."""
    out = b""
    counter = 1
    while len(out) < length:
        out += hmac.new(dek_bytes, label + bytes([counter]), hashlib.sha256).digest()
        counter += 1
    return out[:length]


def derive_column_keys(dek_bytes: bytes) -> dict:
    """Derive the three subkeys from the raw DEK bytes."""
    return {
        "siv":    _hkdf_like(dek_bytes, b"vlake.colcrypt.siv.v1",    64),
        "fernet": base64.urlsafe_b64encode(_hkdf_like(dek_bytes, b"vlake.colcrypt.fernet.v1", 32)),
        "bidx":   _hkdf_like(dek_bytes, b"vlake.colcrypt.bidx.v1",   32),
    }


# ---------------------------------------------------------------
# Ciphertext token format
# ---------------------------------------------------------------
# All ciphertexts are stored as short ASCII strings with a mode tag so
# that callers can identify an encrypted value at a glance and so that
# mixed-mode tables are unambiguous.
#
#   det: <urlsafe-b64 AES-SIV ciphertext>
#   rnd: <Fernet token>
#
# Blind index values are stored in a SEPARATE column and are bare hex.

_DET_PREFIX = "det:"
_RND_PREFIX = "rnd:"


def encrypt_det(siv_key: bytes, value) -> str:
    if value is None:
        return None
    siv = AESSIV(siv_key)
    ct = siv.encrypt(str(value).encode("utf-8"), None)
    return _DET_PREFIX + base64.urlsafe_b64encode(ct).decode("ascii")


def decrypt_det(siv_key: bytes, token):
    if token is None or not isinstance(token, str) or not token.startswith(_DET_PREFIX):
        return token
    siv = AESSIV(siv_key)
    pt = siv.decrypt(base64.urlsafe_b64decode(token[len(_DET_PREFIX):]), None)
    return pt.decode("utf-8")


def encrypt_rand(fernet_key: bytes, value) -> str:
    if value is None:
        return None
    return _RND_PREFIX + Fernet(fernet_key).encrypt(str(value).encode("utf-8")).decode("ascii")


def decrypt_rand(fernet_key: bytes, token):
    if token is None or not isinstance(token, str) or not token.startswith(_RND_PREFIX):
        return token
    return Fernet(fernet_key).decrypt(token[len(_RND_PREFIX):].encode("ascii")).decode("utf-8")


# ---------------------------------------------------------------
# Coarsening transformers (for blind indexes)
# ---------------------------------------------------------------
# Each transformer takes a raw value and returns the coarsened form
# that will be HMAC'd. The coarser the form, the smaller the frequency
# leak -- at the cost of filter precision.

def _xform_year(v) -> str:
    m = re.search(r"(\d{4})", str(v))
    return m.group(1) if m else str(v).strip()

def _xform_zip3(v) -> str:
    s = str(v).strip().replace(" ", "")
    return s[:3]

def _xform_icd3(v) -> str:
    # ICD-10 codes are letter + 2 digits at 3-char level (e.g. "E11" = T2DM family).
    s = str(v).strip().upper()
    return s[:3]

def _xform_lower(v) -> str:
    return str(v).strip().lower()

def _xform_identity(v) -> str:
    return str(v).strip()

TRANSFORMERS = {
    "year":     _xform_year,
    "zip3":     _xform_zip3,
    "icd3":     _xform_icd3,
    "lower":    _xform_lower,
    "identity": _xform_identity,
}


def blind_index(bidx_key: bytes, value, xform: str = "identity") -> str:
    if value is None:
        return None
    fn = TRANSFORMERS.get(xform, _xform_identity)
    coarsened = fn(value)
    if not coarsened:
        return None
    return hmac.new(bidx_key, coarsened.encode("utf-8"), hashlib.sha256).hexdigest()[:24]


# ---------------------------------------------------------------
# PHI column registry
# ---------------------------------------------------------------
# Maps lowercase column name -> (mode, bidx_xform or None)
#
# mode:
#   "det"   - deterministic, filterable directly, decrypts back to plaintext
#   "rand"  - randomized, not filterable directly
#   "none"  - not encrypted at all (here so stewards can mark a column
#             non-PHI to override a default)
#
# bidx_xform:
#   None    - no blind index
#   "year"/"zip3"/"icd3"/"lower"/"identity" - blind index with that coarsening
#
# A column can be `rand` + blind-indexed: the stored value is randomized
# (full protection at rest) but a side column enables coarse equality filters.
#
# Stewards can override per-dataset via virtual_tables[did]["phi_overrides"].

DEFAULT_PHI_COLUMNS = {
    # --- High-cardinality identifiers: deterministic, filterable ---
    "mrn":              ("det",  None),
    "ssn":              ("det",  None),
    "email":            ("det",  "lower"),
    "patient_id":       ("det",  None),
    "subject_id":       ("det",  None),
    "participant_id":   ("det",  None),
    "participant_name": ("det",  None),
    "patient_name":     ("det",  None),
    "first_name":       ("det",  None),
    "last_name":        ("det",  None),
    "full_name":        ("det",  None),
    "name":             ("det",  None),
    "phone":            ("det",  None),
    "phone_number":     ("det",  None),

    # --- Quasi-identifiers: randomized + bucketed blind index ---
    "dob":              ("rand", "year"),
    "date_of_birth":    ("rand", "year"),
    "birth_date":       ("rand", "year"),
    "zip":              ("rand", "zip3"),
    "zipcode":          ("rand", "zip3"),
    "zip_code":         ("rand", "zip3"),
    "postal_code":      ("rand", "zip3"),

    # --- Clinical values: randomized + category-level blind index ---
    "diagnosis":        ("rand", "icd3"),
    "diagnosis_code":   ("rand", "icd3"),
    "icd":              ("rand", "icd3"),
    "icd_code":         ("rand", "icd3"),
    "icd10":            ("rand", "icd3"),

    # --- Free text: randomized, no filtering ---
    "notes":            ("rand", None),
    "clinical_notes":   ("rand", None),
    "comments":         ("rand", None),
    "description":     ("rand", None),
}


def resolve_column_mode(col_name: str, overrides: Optional[dict] = None):
    """Return (mode, xform) for a column, or (None, None) if not PHI."""
    key = col_name.lower()
    if overrides and key in overrides:
        v = overrides[key]
        if v is None or v == "none":
            return (None, None)
        return v  # already (mode, xform)
    return DEFAULT_PHI_COLUMNS.get(key, (None, None))


def build_plan(schema_cols, overrides: Optional[dict] = None):
    """Given a list of (name, type) tuples, return a plan describing
    which columns get encrypted and which get blind-index side columns.

    Returns: list of dicts with keys:
      {col, mode, xform, bidx_col}
    bidx_col is None if no blind index applies.
    """
    plan = []
    for name, _ in schema_cols:
        mode, xform = resolve_column_mode(name, overrides)
        if mode is None:
            continue
        bidx_col = f"_bidx_{name.lower()}" if xform else None
        plan.append({"col": name, "mode": mode, "xform": xform, "bidx_col": bidx_col})
    return plan


def plan_bidx_columns(plan):
    """Return the list of new side-column names a plan introduces."""
    return [p["bidx_col"] for p in plan if p["bidx_col"]]


def encrypt_row(keys: dict, plan, row_values, col_index: dict):
    """Apply a plan to a single row (list/tuple). Returns a new list
    with PHI cells replaced by ciphertext and blind-index cells appended
    in plan order for columns that have a bidx_col."""
    new_row = list(row_values)
    for p in plan:
        i = col_index[p["col"]]
        raw = new_row[i]
        if p["mode"] == "det":
            new_row[i] = encrypt_det(keys["siv"], raw)
        elif p["mode"] == "rand":
            new_row[i] = encrypt_rand(keys["fernet"], raw)
    # Append blind-index columns in the order they appear in the plan.
    for p in plan:
        if p["bidx_col"]:
            # Blind index is computed from the ORIGINAL value, not the
            # already-encrypted one.
            i = col_index[p["col"]]
            new_row.append(blind_index(keys["bidx"], row_values[i], p["xform"]))
    return new_row


def decrypt_row(keys: dict, plan, row_values, col_index: dict, allowed_cols=None):
    """Decrypt PHI cells in-place for the allowed columns.
    `allowed_cols` is a set of lowercase column names the caller may see;
    if None, all PHI columns are decrypted. Columns not in the plan are
    untouched. Blind-index side columns are left as-is (they carry no
    recoverable information anyway)."""
    new_row = list(row_values)
    for p in plan:
        if allowed_cols is not None and p["col"].lower() not in allowed_cols:
            continue
        i = col_index.get(p["col"])
        if i is None or i >= len(new_row):
            continue
        v = new_row[i]
        try:
            if p["mode"] == "det":
                new_row[i] = decrypt_det(keys["siv"], v)
            elif p["mode"] == "rand":
                new_row[i] = decrypt_rand(keys["fernet"], v)
        except Exception:
            # Leave ciphertext in place on failure; caller sees the token
            # and can tell the data is protected.
            pass
    return new_row


# ---------------------------------------------------------------
# WHERE-clause rewriter for PHI predicates
# ---------------------------------------------------------------
# We support ONLY simple equality over a single column with a string or
# numeric literal:
#    col = 'value'
#    "col" = 'value'
#    col = 123
# Anything more complex (functions, LIKE, ranges) is left alone and will
# simply fail to match if the user tries to filter on an encrypted col.
#
# For `det` columns, the literal is replaced with the deterministic
# ciphertext of that value. For blind-indexed columns, the predicate is
# rewritten to use the `_bidx_<col>` side column and the HMAC of the
# coarsened value.
#
# `rand`-only columns (no blind index) cannot be filtered. Predicates on
# them are replaced with `FALSE` so the query returns no rows rather
# than silently returning everything.

_LITERAL = r"('(?:[^']|'')*'|[-+]?\d+(?:\.\d+)?)"

def _strip_quotes(lit: str):
    lit = lit.strip()
    if lit.startswith("'") and lit.endswith("'"):
        return lit[1:-1].replace("''", "'")
    return lit


def rewrite_phi_predicates(sql: str, keys: dict, plan) -> str:
    """Rewrite equality predicates on PHI columns. Returns the new SQL."""
    if not plan or " WHERE " not in sql.upper():
        return sql

    by_col = {p["col"].lower(): p for p in plan}

    def repl(match):
        col_raw = match.group("col")
        op = match.group("op")
        lit = match.group("lit")
        col_clean = col_raw.strip().strip('"').lower()
        p = by_col.get(col_clean)
        if not p:
            return match.group(0)
        val = _strip_quotes(lit)
        if p["mode"] == "det":
            ct = encrypt_det(keys["siv"], val)
            return f'"{col_clean}" {op} \'{ct}\''
        # rand (with or without bidx)
        if p["bidx_col"]:
            bi = blind_index(keys["bidx"], val, p["xform"])
            return f'"{p["bidx_col"]}" {op} \'{bi}\''
        # rand, no bidx -> predicate impossible to satisfy
        return "FALSE"

    pattern = re.compile(
        r'(?P<col>"[A-Za-z_][A-Za-z0-9_]*"|[A-Za-z_][A-Za-z0-9_]*)\s*'
        r'(?P<op>=)\s*'
        r'(?P<lit>' + _LITERAL + r')'
    )
    return pattern.sub(repl, sql)
