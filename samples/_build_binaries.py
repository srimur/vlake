"""Regenerate the binary sample files.

Produces:
  consent_form.pdf         -  text-heavy informed-consent PDF
  radiology_report.pdf     -  text-heavy radiology-read PDF
  lab_report_P0001.pdf     -  per-patient lab-panel PDF (text-heavy)
  lab_report_P0003.pdf     -  per-patient lab-panel PDF (text-heavy)
  lab_scan_P0005.png       -  scanned lab-report image (exercises OCR)
  scan_image.png           -  image-only radiology artefact (exercises OCR)

Clinical documents are almost never distributed as .txt or .csv. Reports
that reach a data lake come in as PDFs (most) or images (scanned /
faxed). Only tabular *administrative* patient data -  registration,
roster, billing -  arrive as CSV. Lab results specifically are released
as signed PDF panels per-visit.
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

def make_lab_pdf(patient_id, patient_name, age, panel):
    """panel = list of (test_name, value, unit, ref_low, ref_high, flag)"""
    from reportlab.pdfgen.canvas import Canvas
    from reportlab.lib.pagesizes import letter
    out = SAMPLES / f"lab_report_{patient_id}.pdf"
    c = Canvas(str(out), pagesize=letter)
    W, H = letter
    y = H - 72
    def line(s, size=11, gap=15, bold=False):
        nonlocal y
        c.setFont("Helvetica-Bold" if bold else "Helvetica", size)
        c.drawString(72, y, s)
        y -= gap
    line("LABORATORY REPORT", size=14, gap=22, bold=True)
    line("City Hospital  -  Clinical Chemistry Lab", size=11, gap=18)
    line(f"Patient ID:   {patient_id}", bold=True)
    line(f"Patient:      {patient_name}   (age {age})")
    line(f"Collected:    2025-11-14  Reported: 2025-11-14")
    line(f"Ordering MD:  Dr. A. Williams")
    line(f"Accession:    LAB-{patient_id[1:]}-0014")
    y -= 8
    # Panel header
    c.setFont("Helvetica-Bold", 10)
    c.drawString(72,  y, "Test")
    c.drawString(260, y, "Result")
    c.drawString(330, y, "Unit")
    c.drawString(380, y, "Reference")
    c.drawString(480, y, "Flag")
    y -= 14
    c.line(72, y+4, 540, y+4)
    y -= 4
    for name, value, unit, lo, hi, flag in panel:
        c.setFont("Helvetica", 10)
        c.drawString(72,  y, name)
        c.drawString(260, y, str(value))
        c.drawString(330, y, unit)
        c.drawString(380, y, f"{lo} -- {hi}")
        if flag:
            c.setFillColorRGB(0.8, 0.1, 0.1)
            c.setFont("Helvetica-Bold", 10)
        c.drawString(480, y, flag or "")
        c.setFillColorRGB(0, 0, 0)
        y -= 14
    y -= 20
    line("INTERPRETATION", size=12, bold=True)
    flagged = [p for p in panel if p[5]]
    if flagged:
        for t, v, u, lo, hi, flag in flagged:
            line(f"  {t}: {v} {u} ({flag}) -- outside reference {lo}--{hi}.")
    else:
        line("  All results within reference range.")
    y -= 16
    line("Electronically signed: Dr. S. Patel, MD Pathologist", size=10)
    c.save()
    print(f"[ok] {out}  ({out.stat().st_size} bytes)")

def make_lab_scan_png(patient_id, patient_name):
    """Simulated scanned/faxed lab report -  exercises the OCR fallback."""
    from PIL import Image, ImageDraw, ImageFont
    out = SAMPLES / f"lab_scan_{patient_id}.png"
    img = Image.new("RGB", (900, 600), "white")
    d = ImageDraw.Draw(img)
    try:
        fb = ImageFont.truetype("arial.ttf", 28)
        fs = ImageFont.truetype("arial.ttf", 18)
    except Exception:
        fb = ImageFont.load_default(); fs = ImageFont.load_default()
    d.text((30,  25), "CITY HOSPITAL  -  LAB REPORT",            fill="black", font=fb)
    d.text((30,  80), f"Patient: {patient_id}  ({patient_name})", fill="black", font=fs)
    d.text((30, 110), "Collected: 2025-11-14",                    fill="black", font=fs)
    d.text((30, 140), "Panel: Basic Metabolic Panel",             fill="black", font=fs)
    d.text((30, 200), "Test          Result   Unit    Ref",       fill="black", font=fs)
    d.text((30, 230), "Glucose       101      mg/dL   70-99",     fill="black", font=fs)
    d.text((30, 260), "Creatinine    0.9      mg/dL   0.6-1.2",   fill="black", font=fs)
    d.text((30, 290), "Sodium        139      mmol/L  136-145",   fill="black", font=fs)
    d.text((30, 320), "Potassium     4.2      mmol/L  3.5-5.1",   fill="black", font=fs)
    d.text((30, 400), "Impression: Glucose marginally elevated.", fill="black", font=fs)
    d.text((30, 450), "Signed: Dr. S. Patel, MD",                 fill="black", font=fs)
    img.save(out)
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
    # Per-patient lab panels (PDF is how real labs release results).
    make_lab_pdf("P0001", "John Doe", 52, [
        ("Glucose (fasting)",   126, "mg/dL",   70,    99, "H"),
        ("HbA1c",              7.2, "%",       4.0,   5.6, "H"),
        ("LDL cholesterol",     145, "mg/dL",    0,   100, "H"),
        ("HDL cholesterol",      42, "mg/dL",   40,   100, ""),
        ("Creatinine",          1.0, "mg/dL",   0.6,  1.2, ""),
    ])
    make_lab_pdf("P0003", "Miguel Alvarez", 61, [
        ("Glucose (fasting)",   182, "mg/dL",   70,    99, "H"),
        ("HbA1c",              8.1, "%",       4.0,   5.6, "H"),
        ("LDL cholesterol",      92, "mg/dL",    0,   100, ""),
        ("HDL cholesterol",      38, "mg/dL",   40,   100, "L"),
        ("ALT",                  54, "U/L",      7,    56, ""),
    ])
    make_lab_scan_png("P0005", "Priya Sharma")
    make_png()
