#Librerias

from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func, extract
import csv
import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine


app = Flask(__name__)
# Specify the database URI
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///C:/Users/Jose Jerkovich/Desktop/upload test/data.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

UPLOAD_FOLDER = 'C:\\Users\Jose Jerkovich\\Desktop\\upload test'
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Define the Department model
class Department(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    department = db.Column(db.String(255), nullable=False)

    def __repr__(self):
        return f'<Department {self.id}: {self.department}>'

# Define the Job model
class Job(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    job = db.Column(db.String(255), nullable=False)

    def __repr__(self):
        return f'<Job {self.id}: {self.job}>'

# Define the HiredEmployee model
class HiredEmployee(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    datetime = db.Column(db.String(255), nullable=False)  # ISO format datetime
    department_id = db.Column(db.Integer, db.ForeignKey('department.id'), nullable=False, default=-1)
    job_id = db.Column(db.Integer, db.ForeignKey('job.id'), nullable=False, default=-1)

    def __repr__(self):
        return f'<HiredEmployee {self.id}: {self.name}>'


@app.route('/upload', methods=['POST'])
def upload_files():
    if 'files' not in request.files:
        return 'No files part', 400
    files = request.files.getlist('files')
    for file in files:
        file.save(os.path.join(app.config['UPLOAD_FOLDER'], file.filename))
    return 'Files uploaded successfully', 200


@app.route('/insert_batch', methods=['POST'])
def insert_batch():
    data = request.get_json()
    if not data or not isinstance(data, list):
        return 'Invalid data', 400
    
    try:
        batch_size = min(len(data), 1000)  # Limit batch size to maximum of 1000 rows

        # Prepare a list of HiredEmployee objects to be added in bulk
        employees_to_add = []
        for i in range(batch_size):
            item = data[i]
            employee = HiredEmployee(
                id=item.get('id'),
                name=item.get('name'),
                datetime=item.get('datetime'),
                department_id=item.get('department_id'),
                job_id=item.get('job_id')
            )
            employees_to_add.append(employee)

        # Bulk insert the employees
        db.session.bulk_save_objects(employees_to_add)
        db.session.commit()

        return 'Batch inserted successfully', 200
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500


# Endpoint to handle migration of all CSV files
@app.route('/migrate_all_csv', methods=['POST'])
def upload_all_csv():
    try:
        # Paths to your CSV files
        departments_csv_path = "C:\\Users\\Jose Jerkovich\\Desktop\\upload test\\departments.csv"
        jobs_csv_path = "C:\\Users\\Jose Jerkovich\\Desktop\\upload test\\jobs.csv"
        hired_employees_csv_path = "C:\\Users\\Jose Jerkovich\\Desktop\\upload test\\hired_employees.csv"

        # Process departments.csv
        with open(departments_csv_path, 'r', encoding='utf-8') as f:
            csv_reader = csv.reader(f)
            for row in csv_reader:
                if row:  # Check if row is not empty
                    department_id = int(row[0])
                    department_name = row[1]
                    # Create a Department object and add it to the database
                    department = Department(id=department_id, department=department_name)
                    db.session.add(department)
            db.session.commit()

        # Process jobs.csv
        with open(jobs_csv_path, 'r', encoding='utf-8') as f:
            csv_reader = csv.reader(f)
            for row in csv_reader:
                if row:  # Check if row is not empty
                    job_id = int(row[0])
                    job_title = row[1]
                    # Create a Job object and add it to the database
                    job = Job(id=job_id, job=job_title)
                    db.session.add(job)
            db.session.commit()

        # Process hired_employees.csv
        with open(hired_employees_csv_path, 'r', encoding='utf-8') as f:
            csv_reader = csv.reader(f)
            for row in csv_reader:
                if row:  # Check if row is not empty
                    employee_id = int(row[0])
                    employee_name = row[1]
                    hire_datetime = row[2]
                    department_id = int(row[3]) if row[3] else -1
                    job_id = int(row[4]) if row[4] else -1
                    # Create a HiredEmployee object and add it to the database
                    hired_employee = HiredEmployee(id=employee_id, name=employee_name, datetime=hire_datetime,
                                                   department_id=department_id, job_id=job_id)
                    db.session.add(hired_employee)
            db.session.commit()

        return jsonify({'message': 'All CSV files migrated successfully'}), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
    
@app.route('/employees_hired_by_quarter', methods=['GET'])
def employees_hired_by_quarter():
    try:
        job_name = request.args.get('job')
        department_name = request.args.get('department')

        base_query = db.session.query(
            Department.department,
            Job.job,
            extract('quarter', HiredEmployee.datetime).label('quarter'),
            func.count(HiredEmployee.id).label('count')
        ).join(
            Department, HiredEmployee.department_id == Department.id
        ).join(
            Job, HiredEmployee.job_id == Job.id
        ).filter(
            extract('year', HiredEmployee.datetime) == 2021
        ).group_by(
            Department.department, Job.job, extract('quarter', HiredEmployee.datetime)
        ).order_by(
            Department.department, Job.job, extract('quarter', HiredEmployee.datetime)
        )

        if job_name:
            base_query = base_query.filter(Job.job == job_name)

        if department_name:
            base_query = base_query.filter(Department.department == department_name)

        query_result = base_query.all()

        result = {}
        for department, job, quarter, count in query_result:
            if (department, job) not in result:
                result[(department, job)] = [0, 0, 0, 0]
            result[(department, job)][quarter - 1] = count

        formatted_result = [
            {
                'department': department,
                'job': job,
                'Q1': counts[0],
                'Q2': counts[1],
                'Q3': counts[2],
                'Q4': counts[3]
            }
            for (department, job), counts in result.items()
        ]

        return jsonify(formatted_result), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
    
# Function to calculate the average number of employees hired in 2021 across all departments
def calculate_average_hires():
    try:
        avg_hires_query = db.session.query(
            func.avg(subquery.c.count_employees)
        ).select_from(
            db.session.query(
                HiredEmployee.department_id,
                func.count(HiredEmployee.id).label('count_employees')
            ).filter(
                extract('year', HiredEmployee.datetime) == 2021
            ).group_by(
                HiredEmployee.department_id
            ).subquery().alias('subquery')
        )
        
        average_hires = avg_hires_query.scalar()
        
        return average_hires
    
    except Exception as e:
        print(f"Error calculating average hires: {e}")
        return None

# Endpoint to list departments that hired more employees than the mean in 2021, ordered by number of employees hired (descending)
@app.route('/departments_above_mean_hires', methods=['GET'])
def departments_above_mean_hires():
    try:
        # Calculate the average number of employees hired in 2021
        average_hires = calculate_average_hires()
        
        if average_hires is None:
            return jsonify({'error': 'Failed to calculate average number of hires.'}), 500

        # Query departments that hired more employees than the mean
        above_mean_departments_query = db.session.query(
            Department.id,
            Department.department,
            func.count(HiredEmployee.id).label('hired')
        ).join(
            HiredEmployee, HiredEmployee.department_id == Department.id
        ).filter(
            extract('year', HiredEmployee.datetime) == 2021
        ).group_by(
            HiredEmployee.department_id
        ).having(
            func.count(HiredEmployee.id) > average_hires
        ).order_by(
            func.count(HiredEmployee.id).desc()
        ).all()

        result = []
        for department_id, department_name, hired_count in above_mean_departments_query:
            result.append({
                'id': department_id,
                'department': department_name,
                'hired': hired_count
            })

        return jsonify(result), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    with app.app_context():
        # Create all tables if they do not exist
        db.create_all()
    # Run the Flask application in debug mode
    app.run(debug=True)
