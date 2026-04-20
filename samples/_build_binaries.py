"""Regenerate the binary sample files.

Produces:
  consent_form.pdf      -  text-heavy informed-consent PDF
  radiology_report.pdf  -  text-heavy radiology-read PDF
  scan_image.png        -  image-only "scanned" report (exercises OCR path)

Clinical documents are almost never distributed as .txt. Reports that
reach a data lake come in as PDFs (most) or images (scanned / faxed).
Only tabular patient data -  registration, lab-result extracts, vitals
-  arrive as CSV or JSON.
"""
import os
from pathlib import Path

SAMPLES = Path(__file__).parent

def make_pdf():
    from reportlab.pdfgen.canvas import Canvas
    from reportlab.lib.pagesizes import letter
    out = SAMPLES / "consent_form.pdf"
    c = Canvas(str(out), pagesize=letter)
    W, H = letter
    y = H - 72
    def line(s, size=11, gap=16, bold=False):
        nonlocal y
        c.setFont("Helvetica-Bold" if bold else "Helvetica", size)
        c.drawString(72, y, s)
        y -= gap
    line("INFORMED CONSENT FORM", size=14, gap=24, bold=True)
    line("Protocol: VLAKE-DM-2025-001 (Diabetes Intervention, Phase II)", bold=True)
    line("Sponsor:  Pharma Co. / VIT Clinical Trials Consortium")
    line("Site:     City Hospital, Investigator Dr. A. Williams")
    y -= 12
    line("Participant ID:   P0001", bold=True)
    line("Participant Name: John Doe")
    line("Date of Signing:  2025-11-02")
    y -= 12
    line("PURPOSE OF THE STUDY", bold=True, size=12)
    for s in [
        "This study evaluates a new oral medication intended to improve",
        "glycemic control in adults with Type 2 diabetes. You have been",
        "asked to take part because you meet the inclusion criteria.",
    ]: line(s)
    y -= 6
    line("RISKS AND BENEFITS", bold=True, size=12)
    for s in [
        "Possible side effects include mild headache, nausea, and fatigue.",
        "Rare but serious events include hypoglycemia requiring intervention.",
        "You may withdraw at any time without affecting your medical care.",
    ]: line(s)
    y -= 6
    line("DATA HANDLING", bold=True, size=12)
    for s in [
        "Your data are stored in V-Lake, a verifiable lakehouse governed",
        "by the sponsor, site, and ethics board jointly. You may delegate",
        "access to your investigator, or revoke it at any time.",
    ]: line(s)
    y -= 18
    line("Signature: _______________________________   Date: ______________")
    c.save()
    print(f"[ok] {out}  ({out.stat().st_size} bytes)")

def make_radiology_pdf():
    from reportlab.pdfgen.canvas import Canvas
    from reportlab.lib.pagesizes import letter
    out = SAMPLES / "radiology_report.pdf"
    c = Canvas(str(out), pagesize=letter)
    W, H = letter
    y = H - 72
    def line(s, size=11, gap=15, bold=False):
        nonlocal y
        c.setFont("Helvetica-Bold" if bold else "Helvetica", size)
        c.drawString(72, y, s)
        y -= gap
    line("RADIOLOGY REPORT  -  Chest CT with Contrast", size=14, gap=24, bold=True)
    line("City Hospital Imaging Department", size=11, gap=18)
    line("Patient ID:    P0003", bold=True)
    line("Patient:       Miguel Alvarez   (age 61, M)")
    line("Date of Exam:  2025-11-15")
    line("Ordering MD:   Dr. A. Williams")
    line("Protocol:      CT thorax, IV iodinated contrast, axial 2.5 mm")
    y -= 10
    line("CLINICAL HISTORY", size=12, bold=True)
    for s in [
        "61-year-old male, enrolled in diabetes-intervention trial,",
        "presenting with persistent dry cough for 3 weeks.",
        "Evaluate for parenchymal disease.",
    ]: line(s)
    y -= 8
    line("FINDINGS", size=12, bold=True)
    for s in [
        "Lungs are clear bilaterally without consolidation, mass, or effusion.",
        "No evidence of pulmonary embolism on contrast-enhanced images.",
        "Hilar and mediastinal lymph nodes are within normal limits (<10 mm).",
        "Heart size is normal. No pericardial effusion.",
        "Visualised upper abdomen: hepatic steatosis, mild.",
        "Osseous structures: degenerative changes of the thoracic spine.",
    ]: line(s)
    y -= 8
    line("IMPRESSION", size=12, bold=True)
    for s in [
        "1. No acute intrathoracic abnormality. Negative for PE.",
        "2. Incidental hepatic steatosis  -  correlate clinically.",
        "3. Degenerative changes of the thoracic spine.",
    ]: line(s)
    y -= 16
    line("Electronically signed: Dr. A. Williams, MD  -  2025-11-15 14:32 UTC")
    c.save()
    print(f"[ok] {out}  ({out.stat().st_size} bytes)")

def make_png():
    from PIL import Image, ImageDraw, ImageFont
    out = SAMPLES / "scan_image.png"
    img = Image.new("RGB", (900, 420), "white")
    d = ImageDraw.Draw(img)
    try:
        font_big   = ImageFont.truetype("arial.ttf", 32)
        font_small = ImageFont.truetype("arial.ttf", 20)
    except Exception:
        font_big = ImageFont.load_default()
        font_small = ImageFont.load_default()
    d.text((30, 25),  "CITY HOSPITAL  -  RADIOLOGY", fill="black", font=font_big)
    d.text((30, 80),  "Patient: P0003  (Miguel Alvarez)", fill="black", font=font_small)
    d.text((30, 115), "Study:   CT Chest with Contrast",  fill="black", font=font_small)
    d.text((30, 150), "Date:    2025-11-15",              fill="black", font=font_small)
    d.text((30, 205), "IMPRESSION:",                       fill="black", font=font_small)
    d.text((30, 240), "  1. No acute intrathoracic abnormality.", fill="black", font=font_small)
    d.text((30, 270), "  2. Mild hepatic steatosis, incidental.",  fill="black", font=font_small)
    d.text((30, 300), "  3. Degenerative thoracic spine changes.", fill="black", font=font_small)
    d.text((30, 355), "Signed: Dr. A. Williams, MD", fill="black", font=font_small)
    img.save(out)
    print(f"[ok] {out}  ({out.stat().st_size} bytes)")

if __name__ == "__main__":
    make_pdf()
    make_radiology_pdf()
    make_png()
