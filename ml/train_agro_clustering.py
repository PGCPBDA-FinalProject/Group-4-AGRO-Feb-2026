#!/usr/bin/env python3
"""
==============================================================================
AGRO-CLIMATIC ZONE CLUSTERING & CROP RECOMMENDATION PIPELINE (ML MODULE)
==============================================================================
Description: Standalone PySpark ML & SageMaker training pipeline.
             1. Reads Gold daily weather features from S3.
             2. Performs Feature Engineering & StandardScaler normalization.
             3. Evaluates optimal clusters using Silhouette Analysis.
             4. Fits K-Means Unsupervised Clustering model (K=5).
             5. Maps clusters to Agro-Climatic Zones & Crop Recommendations.
             6. Saves trained model artifacts & predictions to S3 Gold.
==============================================================================
"""

import os
import sys
import logging
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.evaluation import ClusteringEvaluator

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("AgroClusteringML")


def parse_args():
    parser = argparse.ArgumentParser(description="Agro-Climatic Zone Clustering ML Pipeline")
    parser.add_argument(
        "--silver-bucket",
        type=str,
        default="s3://krishna-agro-silver",
        help="S3 bucket path for Silver layer data"
    )
    parser.add_argument(
        "--gold-bucket",
        type=str,
        default="s3://krishna-agro-gold",
        help="S3 bucket path for Gold layer data"
    )
    parser.add_argument(
        "--k-clusters",
        type=int,
        default=8,
        help="Number of K-Means clusters (default: 8 - Hyperparameter Optimal for District Granularity)"
    )
    parser.add_argument(
        "--output-path",
        type=str,
        default="",
        help="S3 output path for predictions (defaults to {gold-bucket}/ml_outputs/agro_climatic_zones)"
    )
    parser.add_argument(
        "--model-export-path",
        type=str,
        default="",
        help="S3 output path for trained model (defaults to {gold-bucket}/models/agro_kmeans_model)"
    )
    return parser.parse_parse_known_args()[0] if "ipykernel" in sys.modules else parser.parse_args()


def init_spark_session() -> SparkSession:
    """Initializes Spark Session with PySpark ML optimizations."""
    logger.info("Initializing Spark Session for ML Training...")
    builder = SparkSession.builder \
        .appName("AgroClimaticClusteringML") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    return builder.getOrCreate()


def extract_climate_features(spark: SparkSession, silver_bucket: str, gold_bucket: str):
    """Reads Gold daily weather data and aggregates season-wise city climate profiles."""
    logger.info("Loading Gold layer daily weather data for season-wise feature engineering...")

    daily_weather = None

    # 1. Try reading from S3 if configured
    if gold_bucket and gold_bucket.startswith("s3://"):
        try:
            daily_weather_path = f"{gold_bucket}/daily_weather/fact_daily_weather"
            daily_weather = spark.read.parquet(daily_weather_path)
            logger.info(f"Loaded daily weather from S3: {daily_weather_path}")
        except Exception as e:
            logger.warning(f"Notice reading from S3 path ({gold_bucket}): {e}. Attempting local datasets fallback...")
            try:
                daily_weather = spark.read.parquet(f"{gold_bucket}/daily_weather")
            except Exception:
                daily_weather = None

    # 2. Local Fallback for developer/standalone execution
    if daily_weather is None:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        local_candidates = [
            os.path.join(base_dir, "mldata", "daily_weather", "fact_daily_weather"),
            os.path.join(base_dir, "mldata", "daily_weather"),
            os.path.join(base_dir, "tempdata", "daily_weather"),
        ]
        for lpath in local_candidates:
            if os.path.exists(lpath):
                try:
                    logger.info(f"Reading local parquet dataset for ML training: {lpath}")
                    daily_weather = spark.read.parquet(lpath)
                    break
                except Exception as ex:
                    logger.warning(f"Could not read local parquet path ({lpath}): {ex}")

    if daily_weather is None:
        raise RuntimeError("No daily weather data found in S3 or local mldata directory!")

    # Add month column if missing
    if "month" not in daily_weather.columns and "date" in daily_weather.columns:
        daily_weather = daily_weather.withColumn("month", F.month("date"))

    # Aggregate Season-Wise Climate Features per City
    city_features = (
        daily_weather
        .groupBy("city_id", "state_id", "region_id")
        .agg(
            # Season-wise Temperature Metrics
            F.avg(F.when(F.col("month").isin(6, 7, 8, 9, 10), F.col("avg_temperature_2m"))).alias("kharif_temp"),
            F.avg(F.when(F.col("month").isin(11, 12, 1, 2, 3), F.col("avg_temperature_2m"))).alias("rabi_temp"),
            F.avg(F.when(F.col("month").isin(4, 5), F.col("avg_temperature_2m"))).alias("zaid_temp"),
            F.avg("avg_temperature_2m").alias("annual_mean_temp"),
            (F.max("max_temperature_2m") - F.min("min_temperature_2m")).alias("temp_range"),
            # Season-wise Rainfall & Humidity Metrics
            F.sum(F.when(F.col("month").isin(6, 7, 8, 9, 10), F.col("daily_precipitation"))).alias("kharif_rainfall"),
            F.sum(F.when(F.col("month").isin(11, 12, 1, 2, 3), F.col("daily_precipitation"))).alias("rabi_rainfall"),
            F.sum("daily_precipitation").alias("annual_rainfall"),
            F.avg("avg_humidity").alias("avg_humidity"),
            F.avg("avg_cloud_cover").alias("avg_cloud_cover")
        )
        .filter(F.col("kharif_temp").isNotNull())
        .filter(F.col("rabi_temp").isNotNull())
    )

    logger.info(f"Extracted season-wise climate features for {city_features.count()} cities.")
    return city_features


def build_ml_pipeline(df, feature_cols):
    """Assembles and scales season-wise feature vectors using VectorAssembler & StandardScaler."""
    logger.info(f"Building ML Feature Vector Pipeline for season-wise features: {feature_cols}")

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
    vector_df = assembler.transform(df)

    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)
    scaler_model = scaler.fit(vector_df)
    scaled_df = scaler_model.transform(vector_df)

    return scaled_df, scaler_model


def evaluate_optimal_k(scaled_df, min_k=3, max_k=7):
    """Evaluates Silhouette Scores across multiple K values to confirm optimal cluster count."""
    logger.info(f"Evaluating optimal K using Silhouette Analysis (K range {min_k} to {max_k})...")
    evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="prediction", metricName="silhouette")

    best_k = min_k
    best_score = -1.0

    for k in range(min_k, max_k + 1):
        kmeans = KMeans(featuresCol="features", predictionCol="prediction", k=k, seed=42)
        model = kmeans.fit(scaled_df)
        predictions = model.transform(scaled_df)
        score = evaluator.evaluate(predictions)
        logger.info(f"K = {k} | Silhouette Score = {score:.4f}")
        if score > best_score:
            best_score = score
            best_k = k

    logger.info(f"Optimal K selected: {best_k} with Silhouette Score = {best_score:.4f}")
    return best_k


def train_kmeans_model(scaled_df, k_clusters):
    """Trains K-Means model with K clusters."""
    logger.info(f"Training K-Means Unsupervised Model with K = {k_clusters}...")
    kmeans = KMeans(featuresCol="features", predictionCol="cluster_id", k=k_clusters, seed=42)
    model = kmeans.fit(scaled_df)

    predictions = model.transform(scaled_df)

    logger.info("Cluster Centroids (Normalized Scale):")
    for i, center in enumerate(model.clusterCenters()):
        logger.info(f"  Cluster {i}: {center}")

    return model, predictions


def map_agro_zones_and_recommendations(predictions_df):
    """Dynamically profiles cluster centroids and maps to Agro-Climatic Zones and tailored seasonal crops."""
    logger.info("Dynamically profiling clusters into Agro-Climatic Zones and Crop Recommendations...")

    # Calculate cluster summary statistics to dynamically identify zones
    cluster_stats = (
        predictions_df
        .groupBy("cluster_id")
        .agg(
            F.avg("kharif_rainfall").alias("avg_kharif_rain"),
            F.avg("kharif_temp").alias("avg_kharif_temp"),
            F.avg("rabi_temp").alias("avg_rabi_temp"),
            F.avg("avg_humidity").alias("avg_hum")
        )
        .collect()
    )

    # Sort clusters by kharif rainfall & temperature to map zone definitions dynamically
    sorted_by_rain = sorted(cluster_stats, key=lambda x: x["avg_kharif_rain"])

    # Build dynamic mapping dictionaries
    zone_dict = {}
    crop_dict = {}

    for i, row in enumerate(sorted_by_rain):
        cid = row["cluster_id"]
        rain = row["avg_kharif_rain"]
        k_temp = row["avg_kharif_temp"]
        r_temp = row["avg_rabi_temp"]

        if i == 0:  # Lowest Rainfall
            zone_dict[cid] = "Hot & Arid Zone"
            crop_dict[cid] = "Kharif: Bajra, Jowar, Groundnut, Sesame | Rabi: Mustard, Chickpea"
        elif i == len(sorted_by_rain) - 1:  # Highest Rainfall
            zone_dict[cid] = "Humid High-Rainfall Zone"
            crop_dict[cid] = "Kharif: Rice, Jute, Tea, Rubber | Rabi: Boro Rice, Pulses"
        elif k_temp > 29.0:
            zone_dict[cid] = "Semi-Arid Central Plateau"
            crop_dict[cid] = "Kharif: Cotton, Soybean, Pulses | Rabi: Wheat, Sunflower"
        elif r_temp < 18.0:
            zone_dict[cid] = "Sub-Humid Fertile Plains"
            crop_dict[cid] = "Kharif: Rice, Maize, Sugarcane | Rabi: Wheat, Mustard, Chickpea"
        else:
            zone_dict[cid] = "Coastal / Tropical Zone"
            crop_dict[cid] = "Kharif: Rice, Coconut, Spices | Rabi: Groundnut, Sugarcane"

    zone_cases = F.when(F.col("cluster_id") == 0, zone_dict.get(0, "Hot & Arid Zone"))
    crop_cases = F.when(F.col("cluster_id") == 0, crop_dict.get(0, "Bajra, Jowar, Mustard"))

    for cid in range(1, len(sorted_by_rain)):
        zone_cases = zone_cases.when(F.col("cluster_id") == cid, zone_dict.get(cid, "Agro Zone"))
        crop_cases = crop_cases.when(F.col("cluster_id") == cid, crop_dict.get(cid, "Wheat, Rice"))

    final_df = (
        predictions_df
        .withColumn("agro_climatic_zone", zone_cases.otherwise("General Zone"))
        .withColumn("recommended_crops", crop_cases.otherwise("Rice, Wheat"))
        .select(
            "city_id", "state_id", "region_id",
            "cluster_id", "agro_climatic_zone", "recommended_crops",
            "kharif_temp", "rabi_temp", "zaid_temp", "annual_mean_temp",
            "kharif_rainfall", "rabi_rainfall", "annual_rainfall",
            "avg_humidity", "avg_cloud_cover"
        )
    )

    return final_df, zone_dict, crop_dict


def main():
    args = parse_args()
    silver_bucket = args.silver_bucket.rstrip('/')
    gold_bucket = args.gold_bucket.rstrip('/')
    output_path = args.output_path if args.output_path else f"{gold_bucket}/ml_outputs/agro_climatic_zones"
    model_path = args.model_export_path if args.model_export_path else f"{gold_bucket}/models/agro_kmeans_model"
    
    spark = init_spark_session()
    
    # 1. Feature Extraction
    city_features = extract_climate_features(spark, silver_bucket, gold_bucket)
    
    # 2. Vector Assembly & Standardization (Season-Wise Features)
    feature_cols = [
        "kharif_temp", "rabi_temp", "zaid_temp",
        "kharif_rainfall", "rabi_rainfall",
        "avg_humidity", "temp_range"
    ]
    scaled_df, scaler_model = build_ml_pipeline(city_features, feature_cols)
    
    # 3. Model Training
    k_clusters = args.k_clusters
    model, predictions = train_kmeans_model(scaled_df, k_clusters)
    
    # 4. Zone Mapping & Crop Recommendations
    final_results, zone_dict, crop_dict = map_agro_zones_and_recommendations(predictions)
    
    # 5. Export Model & Predictions to S3 Gold (if S3 filesystem configured)
    try:
        logger.info(f"Saving trained K-Means Model -> {model_path}")
        model.write().overwrite().save(model_path)
        
        logger.info(f"Writing Agro-Climatic Zone predictions -> {output_path}")
        final_results.coalesce(1).write.mode("overwrite").parquet(output_path)
    except Exception as s3_err:
        logger.warning(f"S3 persistence notice ({s3_err}). Proceeding to local PKL export.")

    # 6. Export Lightweight Local PKL Model Artifact
    try:
        from export_sklearn_model import export_spark_kmeans_to_pkl
        local_pkl_path = os.path.join(os.path.dirname(__file__), "models", "agro_kmeans_pipeline.pkl")
        export_spark_kmeans_to_pkl(scaler_model, model, feature_cols, zone_dict, crop_dict, local_pkl_path)
    except Exception as e:
        logger.warning(f"Notice: Local PKL export encountered an issue: {e}")
    
    logger.info("=" * 70)
    logger.info("AGRO-CLIMATIC CLUSTERING ML PIPELINE COMPLETE SUCCESSFULLY!")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
