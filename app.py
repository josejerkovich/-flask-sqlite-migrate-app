#Librerias
from flask import Flask, request, jsonify  # Importa Flask y sus módulos para crear una aplicación web y manejar solicitudes y respuestas JSON.
from flask_sqlalchemy import SQLAlchemy  # Importa SQLAlchemy para integrar SQLAlchemy con Flask.
from sqlalchemy import func, extract  # Importa funciones de SQLAlchemy para realizar consultas avanzadas.
import csv  # Importa la librería csv para leer y escribir archivos CSV.
import os  # Importa la librería os para interactuar con el sistema operativo.
import pandas as pd  # Importa pandas para manipular y analizar datos.
import numpy as np  # Importa numpy para operaciones numéricas.
from sqlalchemy import create_engine  # Importa create_engine para crear una conexión con la base de datos.

# Crear la aplicación Flask
app = Flask(__name__)

# Especifica la URI de la base de datos
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///C:/Users/Jose Jerkovich/Desktop/upload test/data.db'  # Configura la URI de la base de datos SQLite.
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False  # Desactiva el seguimiento de modificaciones para mejorar el rendimiento.
db = SQLAlchemy(app)  # Crea una instancia de SQLAlchemy asociada con la aplicación Flask.

# Configuración de la carpeta de subida de archivos
UPLOAD_FOLDER = 'C:\\Users\\Jose Jerkovich\\Desktop\\upload test'
if not os.path.exists(UPLOAD_FOLDER):  # Verifica si la carpeta de subida existe.
    os.makedirs(UPLOAD_FOLDER)  # Crea la carpeta si no existe.
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER  # Configura la carpeta de subida en la aplicación Flask.

# Definir el modelo Department
class Department(db.Model):
    id = db.Column(db.Integer, primary_key=True)  # Define la columna id como clave primaria.
    department = db.Column(db.String(255), nullable=False)  # Define la columna department como un string no nulo.

    def __repr__(self):
        return f'<Department {self.id}: {self.department}>'  # Representación string del modelo.

# Definir el modelo Job
class Job(db.Model):
    id = db.Column(db.Integer, primary_key=True)  # Define la columna id como clave primaria.
    job = db.Column(db.String(255), nullable=False)  # Define la columna job como un string no nulo.

    def __repr__(self):
        return f'<Job {self.id}: {self.job}>'  # Representación string del modelo.

# Definir el modelo HiredEmployee
class HiredEmployee(db.Model):
    id = db.Column(db.Integer, primary_key=True)  # Define la columna id como clave primaria.
    name = db.Column(db.String(255), nullable=False)  # Define la columna name como un string no nulo.
    datetime = db.Column(db.String(255), nullable=False)  # Define la columna datetime como un string no nulo para almacenar fechas en formato ISO.
    department_id = db.Column(db.Integer, db.ForeignKey('department.id'), nullable=False, default=-1)  # Define la columna department_id como clave foránea referenciando a department.
    job_id = db.Column(db.Integer, db.ForeignKey('job.id'), nullable=False, default=-1)  # Define la columna job_id como clave foránea referenciando a job.

    def __repr__(self):
        return f'<HiredEmployee {self.id}: {self.name}>'  # Representación string del modelo.

# Ruta para subir archivos
@app.route('/upload', methods=['POST'])
def upload_files():
    if 'files' not in request.files:  # Verifica si no se enviaron archivos.
        return 'No files part', 400  # Retorna un error si no hay archivos.
    files = request.files.getlist('files')  # Obtiene la lista de archivos.
    for file in files:
        file.save(os.path.join(app.config['UPLOAD_FOLDER'], file.filename))  # Guarda cada archivo en la carpeta de subida.
    return 'Files uploaded successfully', 200  # Retorna un mensaje de éxito.

# Ruta para insertar un lote de datos
@app.route('/insert_batch', methods=['POST'])
def insert_batch():
    data = request.get_json()  # Obtiene los datos JSON de la solicitud.
    if not data or not isinstance(data, list):  # Verifica si los datos son válidos.
        return 'Invalid data', 400  # Retorna un error si los datos no son válidos.
    
    try:
        batch_size = min(len(data), 1000)  # Limita el tamaño del lote a un máximo de 1000 filas.

        # Prepara una lista de objetos HiredEmployee para añadir en bloque
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

        # Inserta en bloque los empleados
        db.session.bulk_save_objects(employees_to_add)
        db.session.commit()

        return 'Batch inserted successfully', 200
    except Exception as e:
        db.session.rollback()  # Revierte la transacción en caso de error.
        return jsonify({'error': str(e)}), 500  # Retorna un mensaje de error.

# Ruta para manejar la migración de todos los archivos CSV
@app.route('/migrate_all_csv', methods=['POST'])
def upload_all_csv():
    try:
        # Rutas a tus archivos CSV
        departments_csv_path = "C:\\Users\\Jose Jerkovich\\Desktop\\upload test\\departments.csv"
        jobs_csv_path = "C:\\Users\\Jose Jerkovich\\Desktop\\upload test\\jobs.csv"
        hired_employees_csv_path = "C:\\Users\\Jose Jerkovich\\Desktop\\upload test\\hired_employees.csv"

        # Procesa departments.csv
        with open(departments_csv_path, 'r', encoding='utf-8') as f:
            csv_reader = csv.reader(f)
            for row in csv_reader:
                if row:  # Verifica si la fila no está vacía
                    department_id = int(row[0])
                    department_name = row[1]
                    # Crea un objeto Department y añádelo a la base de datos
                    department = Department(id=department_id, department=department_name)
                    db.session.add(department)
            db.session.commit()

        # Procesa jobs.csv
        with open(jobs_csv_path, 'r', encoding='utf-8') as f:
            csv_reader = csv.reader(f)
            for row in csv_reader:
                if row:  # Verifica si la fila no está vacía
                    job_id = int(row[0])
                    job_title = row[1]
                    # Crea un objeto Job y añádelo a la base de datos
                    job = Job(id=job_id, job=job_title)
                    db.session.add(job)
            db.session.commit()

        # Procesa hired_employees.csv
        with open(hired_employees_csv_path, 'r', encoding='utf-8') as f:
            csv_reader = csv.reader(f)
            for row in csv_reader:
                if row:  # Verifica si la fila no está vacía
                    employee_id = int(row[0])
                    employee_name = row[1]
                    hire_datetime = row[2]
                    department_id = int(row[3]) if row[3] else -1
                    job_id = int(row[4]) if row[4] else -1
                    # Crea un objeto HiredEmployee y añádelo a la base de datos
                    hired_employee = HiredEmployee(id=employee_id, name=employee_name, datetime=hire_datetime,
                                                   department_id=department_id, job_id=job_id)
                    db.session.add(hired_employee)
            db.session.commit()

        return jsonify({'message': 'All CSV files migrated successfully'}), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Ruta para obtener empleados contratados por trimestre
@app.route('/employees_hired_by_quarter', methods=['GET'])
def employees_hired_by_quarter():
    try:
        job_name = request.args.get('job')  # Obtiene el nombre del trabajo de los parámetros de la solicitud.
        department_name = request.args.get('department')  # Obtiene el nombre del departamento de los parámetros de la solicitud.

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

# Función para calcular el número promedio de empleados contratados en 2021 en todos los departamentos
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

# Ruta para listar los departamentos que contrataron más empleados que la media en 2021, ordenados por número de empleados contratados (descendente)
@app.route('/departments_above_mean_hires', methods=['GET'])
def departments_above_mean_hires():
    try:
        # Calcula el número promedio de empleados contratados en 2021
        average_hires = calculate_average_hires()
        
        if average_hires is None:
            return jsonify({'error': 'Failed to calculate average number of hires.'}), 500

        # Consulta los departamentos que contrataron más empleados que la media
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
        # Crea todas las tablas si no existen
        db.create_all()
    # Ejecuta la aplicación Flask en modo depuración
    app.run(debug=True)

