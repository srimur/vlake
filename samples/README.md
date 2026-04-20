# V-Lake demo sample files

Ready-to-use files for exploring the ingestion pipeline end-to-end. Pick
any file, drop it into the **Ingest Data** page (or stream the JSON ones
via the JSON Stream tab), and you'll see Merkle roots, predicate
injection, and OCR working on real data.

| File | Kind | Suggested target dataset | Notes |
|---|---|---|---|
| `enrollment_patients.csv` | Structured | `trial_enrollment` | 8 rows, PHI columns (name, phone, email) trigger column encryption |
| `lab_results.csv` | Structured | `lab_results` | 12 rows, glucose/HbA1c/lipid panels |
| `adverse_events.json` | Stream | `adverse_events` | Paste into JSON Stream tab, or upload as a file |
| `vitals.json` | Stream | `vitals_stream` | Device telemetry readings |
| `consent_form.pdf` | Document | `imaging_reports` | Text-heavy PDF, pypdf extracts cleanly |
| `radiology_report.txt` | Document | `imaging_reports` | Plain text, trivial extraction |
| `scan_image.png` | Document | `imaging_reports` | Image-only — exercises the OCR fallback (pytesseract) |

## Regenerating the binaries

The PDF and PNG are produced by `samples/_build_binaries.py`. Re-run it
after cloning if they're not present:

```
python samples/_build_binaries.py
```

## What the OCR fallback does

If a PDF has no extractable text (classic scanned document) or an image
is uploaded, `_extract_text` tries pytesseract. If tesseract isn't
installed on the host, or if it simply finds nothing, the file is still
ingested — a descriptive placeholder is stored as the row's text, so
the document remains Merkle-committed, access-controlled, and visible
in the audit trail.

The Docker image (`docker/Dockerfile.backend`) installs `tesseract-ocr`
and `poppler-utils` so the fallback works out of the box in the
container.
