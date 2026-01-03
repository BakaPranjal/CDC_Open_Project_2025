# Multimodal Property Price Predictor
This repository contains a machine learning pipeline for predicting real estate prices using a multimodal approach. The system combines traditional tabular data (house features like bedrooms, square footage, etc.) with satellite imagery of the properties to improve prediction accuracy.

## Project Overview
The project is divided into three main components:

**Map Data Acquisition:** A script to automate the retrieval of satellite imagery based on property coordinates.

**Data Preprocessing:** A notebook for cleaning tabular data and preparing image paths for training.

**Model Training:** A deep learning pipeline that uses a dual-input architecture (CNN for images and a Dense network for numerical data).

## Project Structure
**map_fetcher.py:** Python script utilizing the Mapbox API to fetch static satellite images for properties using latitude and longitude.

**preprocessing.ipynb:** Jupyter notebook for data cleaning, logarithmic price transformation, and feature scaling.

**model_training.ipynb:** Jupyter notebook for building, training, and evaluating the multimodal ResNet50-based neural network and XGBoost models.

## Setup Instructions
*Prerequisites*
Python 3.8 or higher

A Mapbox Access Token (for image retrieval)

A GPU is recommended for model training (TensorFlow/Keras)

## Installation
Clone the repository:

```
git clone https://github.com/your-username/property-price-predictor.git
cd property-price-predictor
```

## Install the required dependencies:
```
pip install pandas numpy tensorflow scikit-learn matplotlib seaborn xgboost opencv-python requests
```
## Usage
1. Fetching Map Data
Before training, you must fetch the satellite images. Open map_fetcher.py and replace "your access token" with your actual Mapbox API key. Run the script to download images:

```
python map_fetcher.py
```
Images will be saved to the Map_Images_Test or Map_Images_Train directories by default.

2. Data Preprocessing
Run the **_preprocessing.ipynb_** notebook to:

* Clean missing values from the dataset.
* Apply log transformation to the price column to handle skewness.
* Scale numerical features using StandardScaler.
* Verify that all image_path entries correspond to downloaded files.

3. Training the Model
The **_model_training.ipynb_** notebook handles the multimodal learning process:

* **Image Branch:** Uses a ResNet50 backbone (pre-trained on ImageNet) for feature extraction from property images.
* **Numerical Branch:** A fully connected neural network for tabular data features.
* **Fusion Layer:** Concatenates both branches into a single output layer for price regression.

To train the model, ensure the paths to your processed CSV files are correctly set in the notebook.

## Model Architecture
The deep learning model uses a late-fusion strategy:

* **Visual Features:** Extracted via ResNet50 with Global Average Pooling.

* **Numerical Features:** Processed through multiple Dense layers with Dropout and Batch Normalization.

_Optimization:_ Adam optimizer with Huber loss for robustness against outliers.

## Evaluation Metrics
The pipeline evaluates performance using:

* Root Mean Squared Error (RMSE)
* Mean Absolute Error (MAE)
* R-squared (R2) Score
* Mean Absolute Percentage Error (MAPE)