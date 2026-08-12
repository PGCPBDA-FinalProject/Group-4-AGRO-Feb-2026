#!/usr/bin/env python3
"""
==============================================================================
LOCAL PKL MODEL EXPORTER FOR AGRO-CLIMATIC CLUSTERING PIPELINE
==============================================================================
Description: Converts PySpark StandardScaler and KMeansModel parameters into
             a lightweight Scikit-Learn pipeline and serializes it to a local
             .pkl file for instant microsecond inference without PySpark.
==============================================================================
"""

import os
import pickle
import logging
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("AgroPKLExporter")

DEFAULT_PKL_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "models",
    "agro_kmeans_pipeline.pkl"
)


def export_spark_kmeans_to_pkl(
    scaler_model,
    kmeans_model,
    feature_cols,
    zone_dict=None,
    crop_dict=None,
    output_path=DEFAULT_PKL_PATH
):
    """
    Extracts PySpark StandardScaler & KMeansModel parameters and serializes
    them into a lightweight Scikit-Learn compatible .pkl artifact locally.
    """
    try:
        from sklearn.preprocessing import StandardScaler
        from sklearn.clustering import KMeans
    except ImportError:
        logger.warning("scikit-learn not installed. Creating native numpy/pickle fallback artifact.")
        StandardScaler = None
        KMeans = None

    logger.info("Extracting PySpark model parameters for local PKL serialization...")

    # Extract Mean and Standard Deviation from PySpark StandardScalerModel
    means = np.array(scaler_model.mean)
    stds = np.array(scaler_model.std)

    # Extract Cluster Centroids from PySpark KMeansModel
    centroids = np.array([list(c) for c in kmeans_model.clusterCenters()])
    k_clusters = len(centroids)

    if StandardScaler is not None and KMeans is not None:
        # Reconstruct Scikit-Learn StandardScaler
        sk_scaler = StandardScaler()
        sk_scaler.mean_ = means
        sk_scaler.scale_ = stds
        sk_scaler.var_ = stds ** 2
        sk_scaler.n_features_in_ = len(means)

        # Reconstruct Scikit-Learn KMeans
        sk_kmeans = KMeans(n_clusters=k_clusters, random_state=42)
        sk_kmeans.cluster_centers_ = centroids
        sk_kmeans._n_threads = 1
    else:
        sk_scaler = {"mean": means, "std": stds}
        sk_kmeans = {"centroids": centroids}

    # Construct complete local serving artifact
    pipeline_artifact = {
        "scaler": sk_scaler,
        "kmeans": sk_kmeans,
        "feature_cols": feature_cols,
        "means": means,
        "stds": stds,
        "centroids": centroids,
        "k_clusters": k_clusters,
        "zone_dict": zone_dict or {},
        "crop_dict": crop_dict or {}
    }

    # Ensure target output directory exists locally
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "wb") as f:
        pickle.dump(pipeline_artifact, f)

    logger.info(f"Successfully exported local PKL model artifact -> {output_path}")
    return output_path


def load_local_pkl_model(pkl_path=DEFAULT_PKL_PATH):
    """Loads and returns the local PKL model artifact for microsecond inference."""
    if not os.path.exists(pkl_path):
        raise FileNotFoundError(f"Local PKL model artifact not found at: {pkl_path}")

    with open(pkl_path, "rb") as f:
        artifact = pickle.load(f)

    return artifact


def predict_cluster_local(features_vector, pkl_artifact):
    """Performs instant inference using the loaded PKL model artifact."""
    features = np.array(features_vector).reshape(1, -1)
    means = pkl_artifact["means"]
    stds = pkl_artifact["stds"]
    centroids = pkl_artifact["centroids"]

    # Scale features: (X - mean) / std
    scaled_features = np.where(stds != 0, (features - means) / stds, 0.0)

    # Find nearest cluster centroid via Euclidean distance
    distances = np.linalg.norm(centroids - scaled_features, axis=1)
    cluster_id = int(np.argmin(distances))

    zone = pkl_artifact["zone_dict"].get(cluster_id, "General Agro Zone")
    crops = pkl_artifact["crop_dict"].get(cluster_id, "Wheat, Rice")

    return {
        "cluster_id": cluster_id,
        "agro_climatic_zone": zone,
        "recommended_crops": crops,
        "distance_to_centroid": float(distances[cluster_id])
    }


if __name__ == "__main__":
    logger.info("Local PKL Exporter module ready.")
