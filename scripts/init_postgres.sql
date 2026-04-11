CREATE TABLE IF NOT EXISTS lab_results (
    lab_id VARCHAR(10) PRIMARY KEY,
    patient_id VARCHAR(10) NOT NULL,
    test_name VARCHAR(50) NOT NULL,
    result VARCHAR(50) NOT NULL,
    unit VARCHAR(20) DEFAULT '',
    reference_range VARCHAR(50) DEFAULT 'Normal',
    ordering_physician VARCHAR(100),
    test_date DATE NOT NULL,
    lab_location VARCHAR(50) DEFAULT 'Central Lab',
    is_critical BOOLEAN DEFAULT FALSE
);

INSERT INTO lab_results VALUES
    ('L001','P0001','CBC','Normal','','4.5-11.0','Dr. Williams','2025-01-20','Central Lab',FALSE),
    ('L002','P0001','ALT','28','U/L','7-56','Dr. Williams','2025-01-20','Central Lab',FALSE),
    ('L003','P0001','Creatinine','0.9','mg/dL','0.7-1.3','Dr. Williams','2025-02-15','Central Lab',FALSE),
    ('L004','P0002','HbA1c','5.8','%','<5.7','Dr. Williams','2025-02-01','Central Lab',FALSE),
    ('L005','P0002','Lipid Panel','Normal','mg/dL','<200','Dr. Williams','2025-02-01','Central Lab',FALSE),
    ('L006','P0003','Creatinine','1.4','mg/dL','0.7-1.3','Dr. Williams','2025-02-05','Central Lab',TRUE),
    ('L007','P0003','BUN','28','mg/dL','7-20','Dr. Williams','2025-02-05','Central Lab',TRUE),
    ('L008','P0004','CBC','Low WBC','','4.5-11.0','Dr. Garcia','2025-03-01','EU Satellite Lab',TRUE),
    ('L009','P0006','TSH','2.1','mIU/L','0.4-4.0','Dr. Garcia','2025-03-15','EU Satellite Lab',FALSE),
    ('L010','P0007','ALT','142','U/L','7-56','Dr. Williams','2025-03-20','Central Lab',TRUE),
    ('L011','P0007','AST','98','U/L','10-40','Dr. Williams','2025-03-20','Central Lab',TRUE),
    ('L012','P0007','Bilirubin','2.1','mg/dL','0.1-1.2','Dr. Williams','2025-03-22','Central Lab',TRUE),
    ('L013','P0008','CBC','Normal','','4.5-11.0','Dr. Garcia','2025-03-25','EU Satellite Lab',FALSE),
    ('L014','P0001','Troponin','<0.01','ng/mL','<0.04','Dr. Williams','2025-04-01','Central Lab',FALSE),
    ('L015','P0005','CBC','Normal','','4.5-11.0','Dr. Williams','2025-02-20','Central Lab',FALSE)
ON CONFLICT DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_lab_patient ON lab_results(patient_id);
