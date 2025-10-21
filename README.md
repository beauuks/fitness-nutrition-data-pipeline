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

1. **Extract**: read the raw data
2. **Transform** the data into the Snowflake schema
   - Unifying users
   - Parsing text blobs into keys
   - Creating fact and dimension dataframes
3. **Load** the data into the data warehouse.

### Database Schema

This project builds an **Online Analytical Processing (OLAP)** data warehouse using a **Snowflake Schema**.

This model is optimized for high-speed, complex `GROUP BY` and `JOIN` queries essential for analytics and machine learning.
- **Fact Tables (Green)**: Store the "measures" or numbers (e.g., Weight, CaloriesBurned).
- **Dimension Tables (Blue)**: Store the "context" or categories (e.g., UserName, ExerciseName).
- **Bridge Tables (Yellow)**: Connect dimensions with many-to-many relationships (e.g., a User can have many Health Conditions).
