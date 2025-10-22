"""
Configuration settings
- db credentials, file paths
- edit with my mysql credentials 
"""

import os
from pathlib import Path

# Database Configuration for MySQL
DATABASE_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '3306'),
    'database': os.getenv('DB_NAME', 'fitness_nutrition_db'),
    'username': os.getenv('DB_USER', 'your_username'),
    'password': os.getenv('DB_PASSWORD', 'your_password')
}

# Data Paths Configuration
DATA_PATHS = {
    'base_path': Path('./data'),
    'fitbit_path': Path('./data/fitbit'),
    'gym_members_file': Path('./data/gym_members_exercise_tracking.csv'),
    'mendeley_file': Path('./data/gym_recommendation.xlsx'),
    'nutrition_file': Path('./data/nutrition_dataset.xlsx'),
    'output_path': Path('./output')
}

# Fitness Goals Mapping
FITNESS_GOALS = {
    'lose_weight': ['lose', 'weight loss', 'fat loss', 'cut'],
    'build_muscle': ['muscle', 'strength', 'hypertrophy', 'build', 'gain'],
    'endurance': ['endurance', 'cardio', 'running', 'cycling', 'marathon'],
    'maintain_health': ['maintain', 'health', 'wellness', 'balance']
}

# NOT USED
# ETL Configuration
ETL_CONFIG = {
    'batch_size': 1000,
    'max_retries': 3,
    'timeout_seconds': 300,
    'log_level': 'INFO'
}

# Data Quality Thresholds
QUALITY_THRESHOLDS = {
    'minimum_age': 13,
    'maximum_age': 100,
    'minimum_weight': 30,  # kg
    'maximum_weight': 300,  # kg
    'minimum_height': 1.0,  # meters
    'maximum_height': 2.5,  # meters
    'completeness_threshold': 0.7  # 70% data completeness required
}