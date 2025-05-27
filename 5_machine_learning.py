from ml_tools.ensemble_learning import run_pipeline
import os

TARGET_COLS = [
    "Premature Rupture of Membranes",    # 胎膜早破
    "Fetal Distress",                    # 胎儿宫内窘迫
    "Macrosomia",                        # 巨大儿
    "Amniotic Fluid Contamination"       # 羊水污染
    ]

IMPUTED_DATASETS_DIR = os.path.join(os.getcwd(), "MICE", "Imputed_Datasets")
MODEL_DIR = os.path.join(os.getcwd(), "Model_Metrics")


run_pipeline(
    datasets_dir=IMPUTED_DATASETS_DIR,
    save_dir=MODEL_DIR,
    target_columns=TARGET_COLS,
    task="classification",
    resample_strategy="UNDERSAMPLE",
    save_model=False,
    L1_regularization=1.5,
    L2_regularization=1.5,
)
