# -*- coding: utf-8 -*-
"""
Tests for column-level PHI encryption (column_crypto.py + app.py integration).
Run: cd backend && pytest test_column_crypto.py -v
"""
import pytest
import column_crypto as cc


# ---------------------------------------------------------------
# Primitive tests
# ---------------------------------------------------------------

@pytest.fixture
def keys():
    # Fixed DEK bytes so derived keys are stable across test runs.
    dek = b"\x01" * 32
    return cc.derive_column_keys(dek)


class TestDeterministic:
    def test_roundtrip(self, keys):
        ct = cc.encrypt_det(keys["siv"], "P0001")
        assert ct.startswith("det:")
        assert cc.decrypt_det(keys["siv"], ct) == "P0001"

    def test_deterministic_property(self, keys):
        """Same plaintext must produce the same ciphertext (enables equality filter)."""
        a = cc.encrypt_det(keys["siv"], "john@example.com")
        b = cc.encrypt_det(keys["siv"], "john@example.com")
        assert a == b

    def test_distinct_plaintexts_distinct(self, keys):
        a = cc.encrypt_det(keys["siv"], "mrn-1")
        b = cc.encrypt_det(keys["siv"], "mrn-2")
        assert a != b

    def test_none_passthrough(self, keys):
        assert cc.encrypt_det(keys["siv"], None) is None
        assert cc.decrypt_det(keys["siv"], None) is None


class TestRandomized:
    def test_roundtrip(self, keys):
        ct = cc.encrypt_rand(keys["fernet"], "diabetes mellitus type 2")
        assert ct.startswith("rnd:")
        assert cc.decrypt_rand(keys["fernet"], ct) == "diabetes mellitus type 2"

    def test_randomized_property(self, keys):
        """Same plaintext must produce DIFFERENT ciphertexts (blocks frequency analysis)."""
        a = cc.encrypt_rand(keys["fernet"], "hypertension")
        b = cc.encrypt_rand(keys["fernet"], "hypertension")
        assert a != b
        # But both decrypt to the same value.
        assert cc.decrypt_rand(keys["fernet"], a) == "hypertension"
        assert cc.decrypt_rand(keys["fernet"], b) == "hypertension"


class TestBlindIndex:
    def test_year_coarsening(self, keys):
        a = cc.blind_index(keys["bidx"], "1985-07-14", "year")
        b = cc.blind_index(keys["bidx"], "1985-12-31", "year")
        c = cc.blind_index(keys["bidx"], "1990-07-14", "year")
        assert a == b, "Same year -> same index"
        assert a != c, "Different year -> different index"

    def test_icd3_coarsening(self, keys):
        """E11.9 and E11.2 both collapse to the E11 diabetes family."""
        a = cc.blind_index(keys["bidx"], "E11.9", "icd3")
        b = cc.blind_index(keys["bidx"], "E11.2", "icd3")
        c = cc.blind_index(keys["bidx"], "I10", "icd3")
        assert a == b
        assert a != c

    def test_zip3_coarsening(self, keys):
        a = cc.blind_index(keys["bidx"], "02138", "zip3")
        b = cc.blind_index(keys["bidx"], "02139", "zip3")
        c = cc.blind_index(keys["bidx"], "10001", "zip3")
        assert a == b
        assert a != c


# ---------------------------------------------------------------
# Plan / row encryption tests
# ---------------------------------------------------------------

class TestRowEncryption:
    def test_plan_picks_up_phi_columns(self):
        schema = [("mrn", "VARCHAR"), ("dob", "DATE"), ("diagnosis", "VARCHAR"),
                  ("age", "INTEGER"), ("site_id", "VARCHAR")]
        plan = cc.build_plan(schema)
        cols = {p["col"] for p in plan}
        assert cols == {"mrn", "dob", "diagnosis"}
        assert "age" not in cols  # not PHI by default
        assert "site_id" not in cols

    def test_plan_modes(self):
        schema = [("mrn", "VARCHAR"), ("dob", "DATE"), ("diagnosis", "VARCHAR")]
        plan = cc.build_plan(schema)
        modes = {p["col"]: p["mode"] for p in plan}
        assert modes["mrn"] == "det"
        assert modes["dob"] == "rand"
        assert modes["diagnosis"] == "rand"
        # dob and diagnosis get blind-index side columns; mrn doesn't need one.
        bidx = {p["col"]: p["bidx_col"] for p in plan}
        assert bidx["mrn"] is None
        assert bidx["dob"] == "_bidx_dob"
        assert bidx["diagnosis"] == "_bidx_diagnosis"

    def test_row_roundtrip(self, keys):
        schema = [("mrn", "VARCHAR"), ("name", "VARCHAR"), ("diagnosis", "VARCHAR"),
                  ("age", "INTEGER")]
        plan = cc.build_plan(schema)
        col_index = {n: i for i, (n, _) in enumerate(schema)}
        original = ("MRN-42", "Jane Doe", "E11.9", 52)
        encrypted = cc.encrypt_row(keys, plan, original, col_index)
        # Length grew by the number of blind-index columns (diagnosis has one).
        assert len(encrypted) == len(original) + 1
        # Non-PHI column untouched.
        assert encrypted[col_index["age"]] == 52
        # PHI columns are ciphertext tokens.
        assert encrypted[col_index["mrn"]].startswith("det:")
        assert encrypted[col_index["diagnosis"]].startswith("rnd:")
        # Decrypt returns plaintext for allowed columns.
        decrypted = cc.decrypt_row(keys, plan, encrypted, col_index, allowed_cols=None)
        assert decrypted[col_index["mrn"]] == "MRN-42"
        assert decrypted[col_index["name"]] == "Jane Doe"
        assert decrypted[col_index["diagnosis"]] == "E11.9"
        assert decrypted[col_index["age"]] == 52

    def test_decrypt_respects_allowed_cols(self, keys):
        schema = [("mrn", "VARCHAR"), ("diagnosis", "VARCHAR")]
        plan = cc.build_plan(schema)
        col_index = {"mrn": 0, "diagnosis": 1}
        encrypted = cc.encrypt_row(keys, plan, ("MRN-1", "E11.9"), col_index)
        # Only "diagnosis" permitted.
        decrypted = cc.decrypt_row(keys, plan, encrypted, col_index, allowed_cols={"diagnosis"})
        assert decrypted[0].startswith("det:"), "MRN should remain encrypted"
        assert decrypted[1] == "E11.9"


# ---------------------------------------------------------------
# Predicate rewriter tests
# ---------------------------------------------------------------

class TestPredicateRewrite:
    def _plan(self):
        return cc.build_plan([("mrn", "VARCHAR"), ("diagnosis", "VARCHAR"),
                              ("dob", "DATE"), ("notes", "VARCHAR")])

    def test_det_equality_rewritten_to_ciphertext(self, keys):
        sql = "SELECT * FROM t WHERE mrn = 'MRN-42'"
        out = cc.rewrite_phi_predicates(sql, keys, self._plan())
        expected_ct = cc.encrypt_det(keys["siv"], "MRN-42")
        assert expected_ct in out
        assert "'MRN-42'" not in out

    def test_bidx_equality_rewritten_to_side_column(self, keys):
        sql = "SELECT * FROM t WHERE diagnosis = 'E11.9'"
        out = cc.rewrite_phi_predicates(sql, keys, self._plan())
        assert "_bidx_diagnosis" in out
        assert "'E11.9'" not in out
        # The rewritten literal is the HMAC of the coarsened value.
        expected_bi = cc.blind_index(keys["bidx"], "E11.9", "icd3")
        assert expected_bi in out

    def test_year_bidx_coarsening(self, keys):
        sql = "SELECT * FROM t WHERE dob = '1985-07-14'"
        out = cc.rewrite_phi_predicates(sql, keys, self._plan())
        assert "_bidx_dob" in out
        expected_bi = cc.blind_index(keys["bidx"], "1985-07-14", "year")
        assert expected_bi in out

    def test_rand_only_column_becomes_false(self, keys):
        """`notes` is randomized with no blind index; predicates on it cannot
        be satisfied and must be rewritten to FALSE rather than silently
        dropped (which would return everything)."""
        sql = "SELECT * FROM t WHERE notes = 'secret'"
        out = cc.rewrite_phi_predicates(sql, keys, self._plan())
        assert "FALSE" in out
        assert "'secret'" not in out

    def test_non_phi_column_untouched(self, keys):
        sql = "SELECT * FROM t WHERE age = 42"
        out = cc.rewrite_phi_predicates(sql, keys, self._plan())
        assert out == sql


# ---------------------------------------------------------------
# End-to-end: ingest -> query through the Flask app
# ---------------------------------------------------------------

class TestEndToEnd:
    def test_ingestion_encrypts_phi_in_duckdb(self, tmp_path):
        """After ingestion, the raw DuckDB table must store PHI as ciphertext."""
        import app as A
        A.app.config["TESTING"] = True
        with A.app.test_client() as c:
            c.post("/api/demo/reset")
            # Write a tiny CSV with PHI columns.
            csv_path = tmp_path / "patients.csv"
            csv_path.write_text(
                "mrn,diagnosis,age,site_id\n"
                "MRN-001,E11.9,52,SITE-A\n"
                "MRN-002,I10,64,SITE-B\n"
                "MRN-003,E11.2,47,SITE-A\n"
            )
            # Create dataset and ingest.
            A.S["dataset_seq"] += 1
            did = str(A.S["dataset_seq"])
            A.S["datasets"][did] = {
                "id": did, "name": "patients", "description": "",
                "schemaJson": "[]", "merkleRoot": "", "creator": A.STEWARD1,
                "sourceType": "LOCAL_FILE", "isConfidential": True, "active": True,
                "createdAt": 0, "lastIngestionAt": 0, "rowCount": 0,
            }
            A.S["custodians"][did] = []
            A.connect_source(did, "LOCAL_FILE", {"file_path": str(csv_path)}, "patients")
            A._post_ingest(did, "patients")

            # Pull raw rows straight from DuckDB and confirm PHI is encrypted at rest.
            raw = A.duck.execute('SELECT * FROM "patients"').fetchall()
            assert len(raw) == 3
            # Schema should now include blind-index side columns.
            desc = A.duck.execute('DESCRIBE "patients"').fetchall()
            names = [r[0] for r in desc]
            assert "_bidx_diagnosis" in names, "blind-index column not created"
            # Find column positions.
            idx = {n: i for i, n in enumerate(names)}
            for row in raw:
                assert row[idx["mrn"]].startswith("det:"), "MRN not deterministically encrypted"
                assert row[idx["diagnosis"]].startswith("rnd:"), "diagnosis not randomly encrypted"
                assert row[idx["age"]] in (52, 64, 47), "non-PHI column corrupted"
                assert row[idx["site_id"]].startswith("SITE-")

            # Public schema hides blind-index cols.
            pub = A.virtual_tables[did]["schema"]
            pub_names = [c["name"] for c in pub]
            assert "_bidx_diagnosis" not in pub_names
            assert "mrn" in pub_names
            assert "diagnosis" in pub_names

    def test_query_filter_on_det_column_returns_match(self, tmp_path):
        """Equality filter on a deterministic PHI column must still find rows."""
        import app as A
        A.app.config["TESTING"] = True
        with A.app.test_client() as c:
            c.post("/api/demo/reset")
            csv_path = tmp_path / "patients.csv"
            csv_path.write_text(
                "mrn,diagnosis,age\n"
                "MRN-001,E11.9,52\n"
                "MRN-002,I10,64\n"
            )
            A.S["dataset_seq"] += 1
            did = str(A.S["dataset_seq"])
            A.S["datasets"][did] = {
                "id": did, "name": "patients", "description": "",
                "schemaJson": "[]", "merkleRoot": "", "creator": A.STEWARD1,
                "sourceType": "LOCAL_FILE", "isConfidential": True, "active": True,
                "createdAt": 0, "lastIngestionAt": 0, "rowCount": 0,
            }
            A.S["custodians"][did] = [A.STEWARD1]
            A.S["roles"][A.STEWARD1] = "DATA_STEWARD"
            A.connect_source(did, "LOCAL_FILE", {"file_path": str(csv_path)}, "patients")
            A._post_ingest(did, "patients")
            # Steward grant is synthesized on-the-fly in execute_query.
            resp = c.post("/api/query", json={
                "querier": A.STEWARD1,
                "datasetId": did,
                "query": "SELECT mrn, age FROM {table} WHERE mrn = 'MRN-001'",
            })
            assert resp.status_code == 200, resp.get_json()
            data = resp.get_json()
            assert data["rowCount"] == 1
            # Result is decrypted for the authorized steward.
            assert data["rows"][0][0] == "MRN-001"
            assert data["rows"][0][1] == 52

    def test_query_filter_on_diagnosis_uses_blind_index(self, tmp_path):
        """Equality on a blind-indexed column must still return matching rows
        across the whole ICD-3 family."""
        import app as A
        A.app.config["TESTING"] = True
        with A.app.test_client() as c:
            c.post("/api/demo/reset")
            csv_path = tmp_path / "patients.csv"
            csv_path.write_text(
                "mrn,diagnosis,age\n"
                "MRN-001,E11.9,52\n"
                "MRN-002,I10,64\n"
                "MRN-003,E11.2,47\n"
            )
            A.S["dataset_seq"] += 1
            did = str(A.S["dataset_seq"])
            A.S["datasets"][did] = {
                "id": did, "name": "patients", "description": "",
                "schemaJson": "[]", "merkleRoot": "", "creator": A.STEWARD1,
                "sourceType": "LOCAL_FILE", "isConfidential": True, "active": True,
                "createdAt": 0, "lastIngestionAt": 0, "rowCount": 0,
            }
            A.S["custodians"][did] = [A.STEWARD1]
            A.S["roles"][A.STEWARD1] = "DATA_STEWARD"
            A.connect_source(did, "LOCAL_FILE", {"file_path": str(csv_path)}, "patients")
            A._post_ingest(did, "patients")
            resp = c.post("/api/query", json={
                "querier": A.STEWARD1,
                "datasetId": did,
                "query": "SELECT mrn, diagnosis FROM {table} WHERE diagnosis = 'E11.9'",
            })
            assert resp.status_code == 200, resp.get_json()
            data = resp.get_json()
            # Both E11.9 and E11.2 collapse to the E11 bucket.
            assert data["rowCount"] == 2
            diags = sorted(r[1] for r in data["rows"])
            assert diags == ["E11.2", "E11.9"]
