# V-Lake demo sample files

Ready-to-use files for exploring the ingestion pipeline end-to-end. Pick
any file, drop it into the **Ingest Data** page (or stream the JSON ones
via the JSON Stream tab), and you'll see Merkle roots, predicate
injection, and OCR working on real data.

Clinical data splits cleanly by format in the real world: administrative
patient data is tabular (CSV), telemetry/streaming data is JSON, and
**reports** — radiology, pathology, consent, lab panels — arrive as PDFs
or as scanned images. The samples mirror that split exactly.

| File | Kind | Suggested target dataset | Notes |
|---|---|---|---|
| `enrollment_patients.csv` | CSV | `trial_enrollment` | 8 rows, PHI columns trigger column encryption |
| `adverse_events.json` | JSON stream | `adverse_events` | Paste into JSON Stream tab or upload as file |
| `vitals.json` | JSON stream | `vitals_stream` | Device telemetry readings |
| `consent_form.pdf` | PDF | `imaging_reports` | Informed-consent form, pypdf extracts cleanly |
| `radiology_report.pdf` | PDF | `imaging_reports` | Chest-CT read, text-heavy |
| `lab_report_P0001.pdf` | PDF | `lab_results` | Lab panel for P0001 |
| `lab_report_P0003.pdf` | PDF | `lab_results` | Lab panel for P0003 |
| `lab_scan_P0005.png` | PNG | `lab_results` | Scanned lab report — exercises OCR fallback |
| `scan_image.png` | PNG | `imaging_reports` | Scanned radiology image — exercises OCR fallback |

## Regenerating the binaries

`_build_binaries.py` produces every PDF and PNG from scratch. Re-run
it if you change the content or after cloning on a machine where the
binaries weren't checked in:

```
python samples/_build_binaries.py
```

Requires `reportlab` and `Pillow` on the host, which come in via
`pip install -r backend/requirements.txt`.

## What the OCR fallback does

When a PDF has no extractable text (classic scanned document) or you
upload an image, `_extract_text` in the backend tries pytesseract. If
tesseract isn't installed on the host, or if it simply finds nothing,
the file is still ingested — a descriptive placeholder is stored as
the row's text, so the document remains Merkle-committed,
access-controlled, and visible in the audit trail.

The Docker image (`docker/Dockerfile.backend`) installs `tesseract-ocr`
and `poppler-utils` so the fallback works out of the box in the
container.
