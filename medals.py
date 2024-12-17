# -*- coding: utf-8 -*-
"""
Created on Fri Dec 13 15:34:10 2024

@author: Asus
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import random
import time

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 0,
    'retry_delay': timedelta(minutes=0),
}

def pick_medal():
    return random.choice(['calc_Bronze', 'calc_Silver', 'calc_Gold'])

def generate_delay():    
    time.sleep(10)  # Затримка на 10 секунд
    
# Назва з'єднання з базою даних MySQL
connection_name = "goit_mysql_db_iyanc"

with DAG(
    'medal_count_dag_ivan_y',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=["ivan_y"]  # Теги для класифікації DAG
) as dag:
    
    # Завдання для створення схеми бази даних (якщо не існує)
    create_schema = MySqlOperator(
        task_id='create_schema',
        mysql_conn_id=connection_name,
        sql="""
        CREATE DATABASE IF NOT EXISTS ivan_y;
        """
    )

    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id=connection_name,
        sql="""
        CREATE TABLE IF NOT EXISTS ivan_y.medals (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    pick_medal_task = BranchPythonOperator(
        task_id='pick_medal',
        python_callable=pick_medal
    )

    calc_Bronze = MySqlOperator(
        task_id='calc_Bronze',
        mysql_conn_id=connection_name,
        sql="""
        INSERT INTO ivan_y.medals (medal_type, count)
        SELECT 'Bronze', COUNT(*) FROM olympic_dataset.athlete_event_results
        WHERE medal='Bronze';
        """
    )

    calc_Silver = MySqlOperator(
        task_id='calc_Silver',
        mysql_conn_id=connection_name,
        sql="""
        INSERT INTO ivan_y.medals (medal_type, count)
        SELECT 'Silver', COUNT(*) FROM olympic_dataset.athlete_event_results
        WHERE medal='Silver';
        """
    )

    calc_Gold = MySqlOperator(
        task_id='calc_Gold',
        mysql_conn_id=connection_name,
        sql="""
        INSERT INTO ivan_y.medals (medal_type, count)
        SELECT 'Gold', COUNT(*) FROM olympic_dataset.athlete_event_results
        WHERE medal='Gold';
        """
    )
    
    generate_delay_task = PythonOperator(
        task_id='generate_delay',
        python_callable=generate_delay,
        trigger_rule=TriggerRule.ONE_SUCCESS   # має викликатись від успіху будь якої попередньої таски
    )

    check_for_correctness = SqlSensor(
        task_id='check_for_correctness',
        conn_id=connection_name,
        sql="""
        SELECT MAX(created_at) >= DATE_SUB(NOW(), INTERVAL 30 SECOND) FROM ivan_y.medals;
        """
    )

    create_schema >> create_table >> pick_medal_task >> [calc_Bronze, calc_Silver, calc_Gold]
    [calc_Bronze, calc_Silver, calc_Gold] >> generate_delay_task
    generate_delay_task >> check_for_correctness