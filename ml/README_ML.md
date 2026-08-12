# Agro-Climatic Zone Clustering & Crop Recommendation ML Module

This standalone Machine Learning module implements **Unsupervised K-Means Clustering** to segment cities into distinct **Agro-Climatic Zones** and generate **Crop Recommendations**.

It reads processed Gold daily weather features, scales variables with PySpark ML `StandardScaler`, fits a K-Means model ($K=5$), profiles cluster centroids, and persists both the trained model artifacts and zone output tables back to **S3 Gold**.

---

## 📁 File Structure

```text
agro/
└── ml/
    ├── train_agro_clustering.py     # Standalone PySpark ML Training & Inference script
    └── README_ML.md                 # Deployment & Execution Guide
```

---

## 🛠️ How It Works

1. **Season-Wise Feature Engineering**:
   - `kharif_temp`: Mean temperature during Kharif / Monsoon season (June – Oct)
   - `rabi_temp`: Mean temperature during Rabi / Winter season (Nov – March)
   - `zaid_temp`: Mean temperature during Zaid / Summer season (April – May)
   - `kharif_rainfall`: Total precipitation during Kharif season
   - `rabi_rainfall`: Total precipitation during Rabi season
   - `avg_humidity`: Mean relative humidity (%)
   - `temp_range`: Diurnal temperature variation ($\text{max} - \text{min}$)

2. **Standardization (`StandardScaler`)**:
   - Scales season-wise features into zero mean and unit variance ($Z$-scores) to ensure metric balance.

3. **Hyperparameter Tuning & Optimal $K$ Selection ($K=8$)**:
   - Evaluated 4 distinct cluster validation metrics across 4,502 district-level weather profiles:
     - **Silhouette Analysis**: Measures cluster cohesion vs separation (Peak score at $K=8$: **0.3397** vs $K=5$: 0.2951).
     - **Elbow Method (WSSE / Inertia)**: Evaluates variance reduction (31.5% reduction from $K=5$ to $K=8$: 10,835 $\rightarrow$ 7,420).
     - **Calinski-Harabasz Index**: Evaluates variance ratio criterion (Peak score at $K=6$: **1790.23**).
     - **Davies-Bouldin Index**: Evaluates cluster similarity ratio (Minimum score at $K=8$: **0.9819**).

| K | WSSE (Inertia) | Silhouette Score | Calinski-Harabasz | Davies-Bouldin | Optimal Evaluation |
|---|---|---|---|---|---|
| 3 | 16,357.15 | 0.2942 | 1,465.30 | 1.3300 | Baseline |
| 4 | 13,001.10 | 0.3137 | 1,615.79 | 1.1121 | Moderate |
| 5 | 10,835.05 | 0.2951 | 1,678.54 | 1.0098 | Under-segmented |
| 6 | 9,031.36 | 0.3283 | **1,790.23** (Peak) | 1.0225 | High CH Peak |
| 7 | 8,077.08 | 0.3296 | 1,756.26 | 0.9835 | Strong |
| **8** | **7,420.17** | **0.3397** (Peak) | 1,695.11 | **0.9819** (Best) | **★ OPTIMAL ($K=8$)** |
| 10 | 6,385.36 | 0.2945 | 1,612.28 | 1.0467 | Over-segmented |

4. **Dynamic Centroid Profiling (8 District-Level Micro-Climate Zones)**:
   - **Hot & Arid Desert Zone** (Thar/Western Rajasthan) $\rightarrow$ *Bajra, Jowar, Groundnut, Mustard*
   - **Semi-Arid Central Plateau** (MP, Maharashtra Vertisols) $\rightarrow$ *Cotton, Soybean, Pulses, Wheat*
   - **Sub-Humid Indo-Gangetic Plains** (Punjab, Haryana, UP) $\rightarrow$ *Wheat, Rice, Maize, Mustard*
   - **Humid Eastern Delta** (WB, Assam, Bihar) $\rightarrow$ *Rice, Jute, Tea, Boro Rice*
   - **Western Coastal High-Rainfall Zone** (Konkan, Goa, Kerala) $\rightarrow$ *Rubber, Coconut, Black Pepper, Rice*
   - **Eastern Coromandel Coastal Zone** (Coastal AP & TN) $\rightarrow$ *Rice, Groundnut, Spices, Sugarcane*
   - **Himalayan Montane Zone** (HP, Uttarakhand, J&K) $\rightarrow$ *Apple, Maize, Barley, Off-season Vegetables*
   - **Deccan Rain-Shadow Dry Zone** (North Karnataka, Rayalaseema) $\rightarrow$ *Ragi, Jowar, Sunflower, Pulses*

---

## 🚀 Execution Options

### Option 1: Run Hyperparameter Tuning
```bash
python3 ml/tune_agro_clustering.py
```

### Option 2: Train District-Level K-Means Model ($K=8$)
```bash
python3 ml/train_agro_clustering.py \
  --silver-bucket s3://krishna-agro-silver \
  --gold-bucket s3://krishna-agro-gold \
  --k-clusters 8
```

### Option 2: AWS SageMaker Processing Job (PySpark Estimator)
To run this script on AWS SageMaker as a serverless PySpark Processing Job:

```python
import sagemaker
from sagemaker.spark.processing import PySparkProcessor

role = sagemaker.get_execution_role()

spark_processor = PySparkProcessor(
    base_job_name="agro-clustering-job",
    role=role,
    instance_type="ml.m5.xlarge",
    instance_count=2,
    framework_version="3.3"
)

spark_processor.run(
    submit_app="ml/train_agro_clustering.py",
    arguments=[
        "--silver-bucket", "s3://krishna-agro-silver",
        "--gold-bucket", "s3://krishna-agro-gold",
        "--k-clusters", "5"
    ]
)
```

---

## 📊 Outputs Saved to S3 Gold

* **Trained K-Means Model**: `s3://krishna-agro-gold/models/agro_kmeans_model/`
* **Agro-Climatic Zone & Recommendations Table**: `s3://krishna-agro-gold/ml_outputs/agro_climatic_zones/`
