import pytest
from app import app, db, Department, Job, HiredEmployee

@pytest.fixture
def client():
    app.config['TESTING'] = True
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
    with app.test_client() as client:
        with app.app_context():
            db.create_all()
        yield client

def test_upload_files(client):
    response = client.post('/upload', data={'files': []})
    assert response.status_code == 400
    assert b'No files part' in response.data

def test_insert_batch(client):
    data = [
        {
            "id": 1,
            "name": "John Doe",
            "datetime": "2021-01-01T12:00:00",
            "department_id": 1,
            "job_id": 1
        }
    ]
    response = client.post('/insert_batch', json=data)
    assert response.status_code == 200