pip install fastapi uvicorn faker
python generate_data.py
uvicorn payments_api:app --reload --port 8000
