"""
Smart Fitness & Nutrition Recommendation Pipeline - Initial ETL Script
Author: Team Project
Date: October 2024
Purpose: Part 1 Deliverable - Data extraction, cleaning, and integration
Database: MySQL
"""

import pandas as pd
import numpy as np
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
import mysql.connector
from sqlalchemy import create_engine, text
import warnings
warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class FitnessNutritionETL:
    """
    Main ETL class for Smart Fitness & Nutrition Recommendation Pipeline
    Handles extraction, transformation, and loading of multiple data sources
    """
    
    def __init__(self, config):
        self.config = config
        self.data_sources = {}
        self.processed_data = {}
        self.engine = None
        
    def setup_database_connection(self):
        """Setup MySQL database connection"""
        try:
            connection_string = f"mysql+pymysql://{self.config['db_user']}:{self.config['db_password']}@{self.config['db_host']}:{self.config['db_port']}/{self.config['db_name']}?charset=utf8mb4"
            self.engine = create_engine(connection_string)
            logger.info("MySQL database connection established successfully")
        except Exception as e:
            logger.error(f"Failed to connect to MySQL database: {e}")
            raise
    
    def extract_fitbit_data(self):
        """Extract and process Fitbit datasets"""
        logger.info("Starting Fitbit data extraction...")
        
        fitbit_files = {
            'daily_activity': 'dailyActivity_merged.csv',
            'heartrate': 'heartrate_seconds_merged.csv',
            'hourly_calories': 'hourlyCalories_merged.csv',
            'weight_log': 'weightLogInfo_merged.csv',
            'sleep_minutes': 'minuteSleep_merged.csv'
        }
        
        fitbit_data = {}
        
        for key, filename in fitbit_files.items():
            try:
                filepath = Path(self.config['data_path']) / 'fitbit' / filename
                if not filepath.exists():
                    logger.warning(f"File not found: {filepath}, skipping...")
                    continue
                    
                df = pd.read_csv(filepath)
                
                # Data cleaning based on requirements
                if key == 'daily_activity':
                    # Remove weather_conditions and date if they exist
                    columns_to_remove = ['weather_conditions', 'date']
                    df = df.drop(columns=[col for col in columns_to_remove if col in df.columns])
                    
                elif key == 'heartrate':
                    # Convert to datetime and calculate daily averages
                    df['Time'] = pd.to_datetime(df['Time'])
                    df['Date'] = df['Time'].dt.date
                    df = df.groupby(['Id', 'Date'])['Value'].mean().reset_index()
                    df.rename(columns={'Value': 'avg_heartrate'}, inplace=True)
                    
                elif key == 'hourly_calories':
                    # Calculate daily calorie totals
                    df['ActivityHour'] = pd.to_datetime(df['ActivityHour'])
                    df['Date'] = df['ActivityHour'].dt.date
                    df = df.groupby(['Id', 'Date'])['Calories'].sum().reset_index()
                    df.rename(columns={'Calories': 'daily_calories'}, inplace=True)
                    
                elif key == 'sleep_minutes':
                    # Calculate hours of sleep per day
                    df['date'] = pd.to_datetime(df['date'])
                    df = df.groupby(['Id', 'date'])['value'].sum().reset_index()
                    df['hours_sleep'] = df['value'] / 60  # Convert minutes to hours
                    df = df[['Id', 'date', 'hours_sleep']]
                
                # Remove rows with all NaN values
                df = df.dropna(how='all')
                
                fitbit_data[key] = df
                logger.info(f"Processed {key}: {len(df)} records")
                
            except Exception as e:
                logger.error(f"Error processing {filename}: {e}")
                continue
        
        self.data_sources['fitbit'] = fitbit_data
        return fitbit_data
    
    def extract_gym_members_data(self):
        """Extract and process gym members dataset"""
        logger.info("Starting gym members data extraction...")
        
        try:
            filepath = Path(self.config['data_path']) / 'gym_members_exercise_dataset.csv'
            if not filepath.exists():
                logger.warning(f"File not found: {filepath}, skipping...")
                return None
                
            df = pd.read_csv(filepath)
            
            # Clean and standardize column names
            df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '').str.replace('/', '_')
            
            # Handle missing values
            numeric_columns = df.select_dtypes(include=[np.number]).columns
            for col in numeric_columns:
                df[col].fillna(df[col].mean(), inplace=True)
            
            # Standardize categorical data
            if 'gender' in df.columns:
                df['gender'] = df['gender'].str.lower()
            
            if 'workout_type' in df.columns:
                df['workout_type'] = df['workout_type'].str.lower()
            
            self.data_sources['gym_members'] = df
            logger.info(f"Processed gym members data: {len(df)} records")
            return df
            
        except Exception as e:
            logger.error(f"Error processing gym members data: {e}")
            return None
    
    def extract_mendeley_health_data(self):
        """Extract and process Mendeley health dataset"""
        logger.info("Starting Mendeley health data extraction...")
        
        try:
            filepath = Path(self.config['data_path']) / 'mendeley_health_fitness.csv'
            if not filepath.exists():
                logger.warning(f"File not found: {filepath}, skipping...")
                return None
                
            df = pd.read_csv(filepath)
            
            # Clean column names
            df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('/', '_')
            
            # Standardize fitness goals
            if 'fitness_goals' in df.columns:
                df['fitness_goals'] = df['fitness_goals'].str.lower()
                
            # Standardize workout types
            if 'workout' in df.columns:
                df['workout'] = df['workout'].str.lower()
            
            # Handle missing values
            essential_cols = [col for col in ['age', 'weight', 'height'] if col in df.columns]
            if essential_cols:
                df = df.dropna(subset=essential_cols)  # Keep records with essential info
            
            self.data_sources['mendeley_health'] = df
            logger.info(f"Processed Mendeley health data: {len(df)} records")
            return df
            
        except Exception as e:
            logger.error(f"Error processing Mendeley health data: {e}")
            return None
    
    def extract_nutrition_data(self):
        """Extract and process nutrition JSON dataset"""
        logger.info("Starting nutrition data extraction...")
        
        try:
            filepath = Path(self.config['data_path']) / 'nutrition_dataset.json'
            if not filepath.exists():
                logger.warning(f"File not found: {filepath}, skipping...")
                return None
                
            with open(filepath, 'r') as f:
                nutrition_data = json.load(f)
            
            # Convert to DataFrame
            if isinstance(nutrition_data, list):
                df = pd.DataFrame(nutrition_data)
            else:
                df = pd.json_normalize(nutrition_data)
            
            # Keep only specified nutritional columns
            nutrition_columns = [
                'name', 'protein', 'carbs', 'fats', 'fiber', 
                'vitamin_b', 'vitamin_d', 'calcium', 'iron', 
                'magnesium', 'zinc', 'potassium', 'calories'
            ]
            
            # Filter columns that exist in the dataset
            available_columns = [col for col in nutrition_columns if col in df.columns]
            if available_columns:
                df = df[available_columns]
            
            # Clean and standardize
            if 'name' in df.columns:
                df = df.dropna(subset=['name'])  # Remove entries without food names
                df['name'] = df['name'].str.lower().str.strip()
            
            # Handle missing nutritional values
            numeric_columns = df.select_dtypes(include=[np.number]).columns
            for col in numeric_columns:
                df[col].fillna(0, inplace=True)
            
            self.data_sources['nutrition'] = df
            logger.info(f"Processed nutrition data: {len(df)} records")
            return df
            
        except Exception as e:
            logger.error(f"Error processing nutrition data: {e}")
            return None
    
    def create_user_mapping(self):
        """Create unified user mapping across datasets"""
        logger.info("Creating user mapping...")
        
        user_mapping = {}
        next_user_id = 1
        
        # Process Fitbit users
        if 'fitbit' in self.data_sources:
            fitbit_users = set()
            for dataset in self.data_sources['fitbit'].values():
                if 'Id' in dataset.columns:
                    fitbit_users.update(dataset['Id'].unique())
            
            for user_id in fitbit_users:
                user_mapping[f"fitbit_{user_id}"] = {
                    'unified_id': next_user_id,
                    'source': 'fitbit',
                    'original_id': user_id
                }
                next_user_id += 1
        
        # Process gym members (create synthetic IDs if not present)
        if 'gym_members' in self.data_sources:
            gym_df = self.data_sources['gym_members']
            for idx in gym_df.index:
                user_mapping[f"gym_{idx}"] = {
                    'unified_id': next_user_id,
                    'source': 'gym',
                    'original_id': idx
                }
                next_user_id += 1
        
        # Process Mendeley users
        if 'mendeley_health' in self.data_sources:
            mendeley_df = self.data_sources['mendeley_health']
            for idx in mendeley_df.index:
                user_mapping[f"mendeley_{idx}"] = {
                    'unified_id': next_user_id,
                    'source': 'mendeley',
                    'original_id': idx
                }
                next_user_id += 1
        
        self.user_mapping = user_mapping
        logger.info(f"Created user mapping for {len(user_mapping)} users")
        return user_mapping
    
    def transform_and_integrate_data(self):
        """Transform and integrate all datasets"""
        logger.info("Starting data transformation and integration...")
        
        # Create unified user profiles
        user_profiles = []
        
        # Process Fitbit data
        if 'fitbit' in self.data_sources:
            fitbit_data = self.data_sources['fitbit']
            
            # Merge Fitbit datasets by user ID
            if 'daily_activity' in fitbit_data and 'weight_log' in fitbit_data:
                merged_fitbit = fitbit_data['daily_activity'].merge(
                    fitbit_data['weight_log'], on='Id', how='outer'
                )
                
                for _, row in merged_fitbit.iterrows():
                    profile = {
                        'unified_user_id': self.user_mapping.get(f"fitbit_{row['Id']}", {}).get('unified_id'),
                        'source': 'fitbit',
                        'age': None,  # Not available in Fitbit data
                        'weight': row.get('WeightKg'),
                        'height': None,  # Not available
                        'bmi': row.get('BMI'),
                        'total_steps': row.get('TotalSteps'),
                        'total_distance': row.get('TotalDistance'),
                        'calories_burned': row.get('Calories'),
                        'fitness_goal': 'maintain_health'  # Default for Fitbit users
                    }
                    user_profiles.append(profile)
            elif 'daily_activity' in fitbit_data:
                # Process only daily activity if weight log is not available
                for _, row in fitbit_data['daily_activity'].iterrows():
                    profile = {
                        'unified_user_id': self.user_mapping.get(f"fitbit_{row['Id']}", {}).get('unified_id'),
                        'source': 'fitbit',
                        'age': None,
                        'weight': None,
                        'height': None,
                        'bmi': None,
                        'total_steps': row.get('TotalSteps'),
                        'total_distance': row.get('TotalDistance'),
                        'calories_burned': row.get('Calories'),
                        'fitness_goal': 'maintain_health'
                    }
                    user_profiles.append(profile)
        
        # Process gym members data
        if 'gym_members' in self.data_sources:
            gym_df = self.data_sources['gym_members']
            
            for idx, row in gym_df.iterrows():
                profile = {
                    'unified_user_id': self.user_mapping.get(f"gym_{idx}", {}).get('unified_id'),
                    'source': 'gym',
                    'age': row.get('age'),
                    'weight': row.get('weight_kg'),
                    'height': row.get('height_m'),
                    'bmi': row.get('bmi'),
                    'max_bpm': row.get('max_bpm'),
                    'avg_bpm': row.get('avg_bpm'),
                    'resting_bpm': row.get('resting_bpm'),
                    'session_duration': row.get('session_duration_hours'),
                    'calories_burned': row.get('calories_burned'),
                    'workout_type': row.get('workout_type'),
                    'experience_level': row.get('experience_level'),
                    'workout_frequency': row.get('workout_frequency_days_week'),
                    'fitness_goal': self._standardize_fitness_goal(row.get('workout_type'))
                }
                user_profiles.append(profile)
        
        # Process Mendeley health data
        if 'mendeley_health' in self.data_sources:
            mendeley_df = self.data_sources['mendeley_health']
            
            for idx, row in mendeley_df.iterrows():
                profile = {
                    'unified_user_id': self.user_mapping.get(f"mendeley_{idx}", {}).get('unified_id'),
                    'source': 'mendeley',
                    'age': row.get('age'),
                    'gender': row.get('sex'),
                    'weight': row.get('weight'),
                    'height': row.get('height'),
                    'health_conditions': row.get('health_conditions'),
                    'fitness_goal': self._standardize_fitness_goal(row.get('fitness_goals')),
                    'fitness_type': row.get('fitness_type'),
                    'workout_preference': row.get('workout'),
                    'diet_preference': row.get('diet_nutritions')
                }
                user_profiles.append(profile)
        
        self.processed_data['user_profiles'] = pd.DataFrame(user_profiles)
        
        # Process workout sessions
        self._create_workout_sessions()
        
        # Process health metrics
        self._create_health_metrics()
        
        logger.info("Data transformation and integration completed")
    
    def _standardize_fitness_goal(self, goal_text):
        """Standardize fitness goals to our 4 main categories"""
        if not goal_text:
            return 'maintain_health'
        
        goal_text = str(goal_text).lower()
        
        if any(word in goal_text for word in ['lose', 'weight loss', 'fat loss']):
            return 'lose_weight'
        elif any(word in goal_text for word in ['muscle', 'strength', 'hypertrophy', 'build']):
            return 'build_muscle'
        elif any(word in goal_text for word in ['endurance', 'cardio', 'running', 'cycling']):
            return 'endurance'
        else:
            return 'maintain_health'
    
    def _create_workout_sessions(self):
        """Create unified workout sessions data"""
        workout_sessions = []
        
        # From gym members data
        if 'gym_members' in self.data_sources:
            gym_df = self.data_sources['gym_members']
            
            for idx, row in gym_df.iterrows():
                session = {
                    'unified_user_id': self.user_mapping.get(f"gym_{idx}", {}).get('unified_id'),
                    'workout_type': row.get('workout_type'),
                    'duration_hours': row.get('session_duration_hours'),
                    'calories_burned': row.get('calories_burned'),
                    'experience_level': row.get('experience_level'),
                    'frequency_per_week': row.get('workout_frequency_days_week'),
                    'session_date': datetime.now().date()  # Synthetic date for now
                }
                workout_sessions.append(session)
        
        # From Fitbit daily activity
        if 'fitbit' in self.data_sources and 'daily_activity' in self.data_sources['fitbit']:
            daily_activity = self.data_sources['fitbit']['daily_activity']
            
            for _, row in daily_activity.iterrows():
                session = {
                    'unified_user_id': self.user_mapping.get(f"fitbit_{row['Id']}", {}).get('unified_id'),
                    'workout_type': 'mixed',
                    'total_steps': row.get('TotalSteps'),
                    'total_distance': row.get('TotalDistance'),
                    'calories_burned': row.get('Calories'),
                    'active_minutes': row.get('VeryActiveMinutes', 0) + row.get('FairlyActiveMinutes', 0),
                    'session_date': datetime.now().date()
                }
                workout_sessions.append(session)
        
        self.processed_data['workout_sessions'] = pd.DataFrame(workout_sessions)
    
    def _create_health_metrics(self):
        """Create unified health metrics data"""
        health_metrics = []
        
        # From Fitbit sleep and heart rate data
        if 'fitbit' in self.data_sources:
            fitbit_data = self.data_sources['fitbit']
            
            if 'sleep_minutes' in fitbit_data:
                sleep_df = fitbit_data['sleep_minutes']
                for _, row in sleep_df.iterrows():
                    metric = {
                        'unified_user_id': self.user_mapping.get(f"fitbit_{row['Id']}", {}).get('unified_id'),
                        'metric_type': 'sleep',
                        'value': row.get('hours_sleep'),
                        'unit': 'hours',
                        'measurement_date': row.get('date')
                    }
                    health_metrics.append(metric)
            
            if 'heartrate' in fitbit_data:
                hr_df = fitbit_data['heartrate']
                for _, row in hr_df.iterrows():
                    metric = {
                        'unified_user_id': self.user_mapping.get(f"fitbit_{row['Id']}", {}).get('unified_id'),
                        'metric_type': 'heart_rate',
                        'value': row.get('avg_heartrate'),
                        'unit': 'bpm',
                        'measurement_date': row.get('Date')
                    }
                    health_metrics.append(metric)
        
        self.processed_data['health_metrics'] = pd.DataFrame(health_metrics)
    
    def create_database_schema(self):
        """Create MySQL database schema"""
        logger.info("Creating MySQL database schema...")
        
        schema_sql = """
        -- Drop existing tables if they exist
        DROP TABLE IF EXISTS health_metrics;
        DROP TABLE IF EXISTS workout_sessions;
        DROP TABLE IF EXISTS nutrition_data;
        DROP TABLE IF EXISTS user_profiles;
        
        -- Create user_profiles table
        CREATE TABLE user_profiles (
            unified_user_id INT AUTO_INCREMENT PRIMARY KEY,
            source VARCHAR(50),
            age INT,
            gender VARCHAR(10),
            weight DECIMAL(5,2),
            height DECIMAL(5,2),
            bmi DECIMAL(5,2),
            fitness_goal VARCHAR(50),
            fitness_type VARCHAR(50),
            workout_preference TEXT,
            diet_preference TEXT,
            health_conditions TEXT,
            experience_level VARCHAR(20),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Create workout_sessions table
        CREATE TABLE workout_sessions (
            session_id INT AUTO_INCREMENT PRIMARY KEY,
            unified_user_id INT,
            workout_type VARCHAR(50),
            duration_hours DECIMAL(4,2),
            calories_burned INT,
            total_steps INT,
            total_distance DECIMAL(6,2),
            active_minutes INT,
            frequency_per_week INT,
            session_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (unified_user_id) REFERENCES user_profiles(unified_user_id)
        );
        
        -- Create health_metrics table
        CREATE TABLE health_metrics (
            metric_id INT AUTO_INCREMENT PRIMARY KEY,
            unified_user_id INT,
            metric_type VARCHAR(50),
            value DECIMAL(10,2),
            unit VARCHAR(20),
            measurement_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (unified_user_id) REFERENCES user_profiles(unified_user_id)
        );
        
        -- Create nutrition_data table
        CREATE TABLE nutrition_data (
            food_id INT AUTO_INCREMENT PRIMARY KEY,
            food_name VARCHAR(200),
            calories DECIMAL(8,2),
            protein DECIMAL(6,2),
            carbs DECIMAL(6,2),
            fats DECIMAL(6,2),
            fiber DECIMAL(6,2),
            vitamin_b DECIMAL(6,2),
            vitamin_d DECIMAL(6,2),
            calcium DECIMAL(6,2),
            iron DECIMAL(6,2),
            magnesium DECIMAL(6,2),
            zinc DECIMAL(6,2),
            potassium DECIMAL(8,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Create indexes for better performance
        CREATE INDEX idx_user_profiles_goal ON user_profiles(fitness_goal);
        CREATE INDEX idx_workout_sessions_user ON workout_sessions(unified_user_id);
        CREATE INDEX idx_workout_sessions_date ON workout_sessions(session_date);
        CREATE INDEX idx_health_metrics_user ON health_metrics(unified_user_id);
        CREATE INDEX idx_health_metrics_type ON health_metrics(metric_type);
        CREATE INDEX idx_nutrition_name ON nutrition_data(food_name);
        """
        
        try:
            # Split the schema into individual statements
            statements = [stmt.strip() for stmt in schema_sql.split(';') if stmt.strip()]
            
            with self.engine.connect() as connection:
                for statement in statements:
                    if statement:
                        connection.execute(text(statement))
                connection.commit()
            logger.info("MySQL database schema created successfully")
        except Exception as e:
            logger.error(f"Error creating MySQL database schema: {e}")
            raise
    
    def load_data_to_database(self):
        """Load processed data to MySQL database"""
        logger.info("Loading data to MySQL database...")
        
        try:
            # Load user profiles
            if 'user_profiles' in self.processed_data and not self.processed_data['user_profiles'].empty:
                # Clean the dataframe for MySQL
                user_df = self.processed_data['user_profiles'].copy()
                
                # Select only the columns that exist in the database schema
                db_columns = ['unified_user_id', 'source', 'age', 'gender', 'weight', 'height', 
                             'bmi', 'fitness_goal', 'fitness_type', 'workout_preference', 
                             'diet_preference', 'health_conditions', 'experience_level']
                
                # Keep only columns that exist in the dataframe
                available_columns = [col for col in db_columns if col in user_df.columns]
                user_df = user_df[available_columns]
                
                user_df.to_sql('user_profiles', self.engine, if_exists='append', index=False)
                logger.info(f"Loaded {len(user_df)} user profiles")
            
            # Load workout sessions
            if 'workout_sessions' in self.processed_data and not self.processed_data['workout_sessions'].empty:
                workout_df = self.processed_data['workout_sessions'].copy()
                
                # Select only the columns that exist in the database schema
                db_columns = ['unified_user_id', 'workout_type', 'duration_hours', 'calories_burned',
                             'total_steps', 'total_distance', 'active_minutes', 'frequency_per_week', 'session_date']
                
                available_columns = [col for col in db_columns if col in workout_df.columns]
                workout_df = workout_df[available_columns]
                
                workout_df.to_sql('workout_sessions', self.engine, if_exists='append', index=False)
                logger.info(f"Loaded {len(workout_df)} workout sessions")
            
            # Load health metrics
            if 'health_metrics' in self.processed_data and not self.processed_data['health_metrics'].empty:
                health_df = self.processed_data['health_metrics'].copy()
                
                db_columns = ['unified_user_id', 'metric_type', 'value', 'unit', 'measurement_date']
                available_columns = [col for col in db_columns if col in health_df.columns]
                health_df = health_df[available_columns]
                
                health_df.to_sql('health_metrics', self.engine, if_exists='append', index=False)
                logger.info(f"Loaded {len(health_df)} health metrics")
            
            # Load nutrition data
            if 'nutrition' in self.data_sources and not self.data_sources['nutrition'].empty:
                nutrition_df = self.data_sources['nutrition'].copy()
                nutrition_df.rename(columns={'name': 'food_name'}, inplace=True)
                
                # Select only the columns that exist in the database schema
                db_columns = ['food_name', 'calories', 'protein', 'carbs', 'fats', 'fiber',
                             'vitamin_b', 'vitamin_d', 'calcium', 'iron', 'magnesium', 'zinc', 'potassium']
                
                available_columns = [col for col in db_columns if col in nutrition_df.columns]
                nutrition_df = nutrition_df[available_columns]
                
                nutrition_df.to_sql('nutrition_data', self.engine, if_exists='append', index=False)
                logger.info(f"Loaded {len(nutrition_df)} nutrition records")
            
        except Exception as e:
            logger.error(f"Error loading data to MySQL database: {e}")
            raise
    
    def validate_data_quality(self):
        """Perform data quality validation"""
        logger.info("Performing data quality validation...")
        
        validation_results = {}
        
        try:
            with self.engine.connect() as connection:
                # Check record counts
                tables = ['user_profiles', 'workout_sessions', 'health_metrics', 'nutrition_data']
                
                for table in tables:
                    result = connection.execute(text(f"SELECT COUNT(*) FROM {table}"))
                    count = result.fetchone()[0]
                    validation_results[f"{table}_count"] = count
                    logger.info(f"{table}: {count} records")
                
                # Check for duplicates in user profiles
                result = connection.execute(text("""
                    SELECT COUNT(*) - COUNT(DISTINCT unified_user_id) as duplicates 
                    FROM user_profiles
                """))
                duplicates = result.fetchone()[0]
                validation_results['user_profile_duplicates'] = duplicates
                
                if duplicates > 0:
                    logger.warning(f"Found {duplicates} duplicate user profiles")
                
                # Check data completeness
                result = connection.execute(text("""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(age) as age_filled,
                        COUNT(weight) as weight_filled,
                        COUNT(fitness_goal) as goal_filled
                    FROM user_profiles
                """))
                completeness = result.fetchone()
                validation_results['data_completeness'] = {
                    'age_completeness': completeness[1] / completeness[0] if completeness[0] > 0 else 0,
                    'weight_completeness': completeness[2] / completeness[0] if completeness[0] > 0 else 0,
                    'goal_completeness': completeness[3] / completeness[0] if completeness[0] > 0 else 0
                }
                
                logger.info(f"Data validation completed: {validation_results}")
                
        except Exception as e:
            logger.error(f"Error during data validation: {e}")
        
        return validation_results
    
    def generate_summary_report(self):
        """Generate summary report for the ETL process"""
        logger.info("Generating summary report...")
        
        report = {
            'etl_timestamp': datetime.now(),
            'data_sources_processed': list(self.data_sources.keys()),
            'total_users': len(self.user_mapping) if hasattr(self, 'user_mapping') else 0,
            'processing_summary': {}
        }
        
        # Add processing details for each data source
        for source, data in self.data_sources.items():
            if isinstance(data, dict):
                report['processing_summary'][source] = {
                    'datasets': list(data.keys()),
                    'total_records': sum(len(df) for df in data.values())
                }
            else:
                report['processing_summary'][source] = {
                    'records': len(data)
                }
        
        # Add validation results if available
        validation_results = self.validate_data_quality()
        report['validation_results'] = validation_results
        
        # Save report as JSON
        report_path = Path(self.config['output_path']) / f"etl_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        # Convert datetime objects to strings for JSON serialization
        report_json = json.dumps(report, default=str, indent=2)
        
        with open(report_path, 'w') as f:
            f.write(report_json)
        
        logger.info(f"Summary report saved to {report_path}")
        return report
    
    def run_full_etl_pipeline(self):
        """Execute the complete ETL pipeline"""
        logger.info("Starting full ETL pipeline...")
        
        try:
            # Step 1: Setup database connection
            self.setup_database_connection()
            
            # Step 2: Extract data from all sources
            self.extract_fitbit_data()
            self.extract_gym_members_data()
            self.extract_mendeley_health_data()
            self.extract_nutrition_data()
            
            # Step 3: Create user mapping
            self.create_user_mapping()
            
            # Step 4: Transform and integrate data
            self.transform_and_integrate_data()
            
            # Step 5: Create database schema
            self.create_database_schema()
            
            # Step 6: Load data to database
            self.load_data_to_database()
            
            # Step 7: Validate data quality
            self.validate_data_quality()
            
            # Step 8: Generate summary report
            self.generate_summary_report()
            
            logger.info("ETL pipeline completed successfully!")
            
        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            raise

def main():
    """Main function to run the ETL pipeline"""
    
    # Configuration
    config = {
        'data_path': './data',  # Path to your data files
        'output_path': './output',
        'db_host': 'localhost',
        'db_port': '3306',
        'db_name': 'fitness_nutrition_db',
        'db_user': 'your_username',
        'db_password': 'your_password'
    }
    
    # Create output directory if it doesn't exist
    Path(config['output_path']).mkdir(parents=True, exist_ok=True)
    
    # Initialize and run ETL pipeline
    etl = FitnessNutritionETL(config)
    etl.run_full_etl_pipeline()

if __name__ == "__main__":
    main()