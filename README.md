# Multimodal House Price Prediction

A machine learning pipeline for predicting real estate prices using satellite imagery and tabular data.

## Overview

This project combines traditional property features (bedrooms, square footage, location) with satellite imagery using a dual-input deep learning architecture. The model uses ResNet50 for image feature extraction and a dense neural network for numerical data, fusing both modalities for improved prediction accuracy.

## Prerequisites

- Python 3.8+
- Mapbox Access Token
- GPU recommended for training

## Installation

```bash
git clone https://github.com/BakaPranjal/CDC_Open_Project_2025.git
cd property-price-predictor
pip install -r requirements.txt
```

**Dependencies:**
```bash
pip install pandas numpy tensorflow scikit-learn matplotlib seaborn xgboost opencv-python requests
```

## Usage

### 1. Fetch Satellite Images

Replace the Mapbox token in `map_fetcher.py` and run:

```bash
python map_fetcher.py
```

Images will be saved to `Map_Images_Train/` and `Map_Images_Test/` directories.

### 2. Preprocess Data

Run `preprocessing.ipynb` to:
- Clean missing values
- Apply log transformation to prices
- Scale numerical features
- Validate image paths

### 3. Train Model

Execute `model_training.ipynb` to train the multimodal model. The notebook includes:
- Data loading and augmentation
- Model architecture setup
- Training with early stopping
- Performance evaluation

## Model Architecture

**Image Branch:**
- ResNet50 (pre-trained on ImageNet)
- Global Average Pooling
- Output: 2048-dimensional feature vector

**Numerical Branch:**
- Dense layers with Dropout and Batch Normalization
- Output: 128-dimensional feature vector

**Fusion:**
- Concatenation of both branches
- Dense layers for final prediction

**Training:**
- Loss: Huber loss
- Optimizer: Adam
- Callbacks: Early stopping, learning rate reduction

## Evaluation Metrics

- Root Mean Squared Error (RMSE)
- Mean Absolute Error (MAE)
- R-squared (R²)
- Mean Absolute Percentage Error (MAPE)