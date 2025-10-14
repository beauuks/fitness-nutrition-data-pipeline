# Smart Fitness & Nutrition Recommendation Pipeline

## Project Overview

This project builds a comprehensive ETL pipeline that integrates multiple fitness and nutrition datasets to provide personalized health recommendations. The system focuses on four main fitness goals: lose weight, build muscle, train for endurance, and maintain a healthy lifestyle.

## Part 1 Deliverable 

This repository contains the initial implementation focusing on data integration, cleaning, and database setup using MySQL.

### Datasets Integrated

1. **Fitbit Dataset** (CSV)
   - Daily activity, heart rate, calories, weight, and sleep data
   - Source: https://www.kaggle.com/datasets/arashnic/fitbit

2. **Gym Members Dataset** (CSV)
   - Member profiles with workout preferences and metrics
   - Source: https://www.kaggle.com/datasets/valakhorasani/gym-members-exercise-dataset

3. **Mendeley Health Dataset** (CSV)
   - Health conditions, fitness goals, and workout preferences
   - Source: https://data.mendeley.com/datasets/zw8mtbm5b9/1

4. **USDA Nutrition Dataset** (JSON)
   - Comprehensive food nutritional information
   - Source: https://www.kaggle.com/datasets/gokulprasantht/nutrition-dataset

## Architecture

### ETL Pipeline Components

1. **Extract**: Read data from CSV and JSON sources
2. **Transform**: 
   - Data cleaning and standardization
   - User mapping across datasets
   - Metric calculations and aggregations
3. **Load**: Store in MySQL with optimized schema

### Database Schema

- **user_profiles**: Unified user information from all sources
- **workout_sessions**: Individual workout records and metrics
- **health_metrics**: Time-series health data (sleep, heart rate, etc.)
- **nutrition_data**: Master food database
- **nutrition_logs**: User food consumption tracking
- **user_recommendations**: Personalized recommendations (future use)

## Installation & Setup

### Prerequisites

- Python 3.8+
- MySQL 8.0+
- Required Python packages (see requirements.txt)

### Installation Steps