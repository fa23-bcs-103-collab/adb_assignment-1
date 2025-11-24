import pytest
from fastapi.testclient import TestClient
from app.main import app
from app.database import get_database

client = TestClient(app)

def test_root():
    response = client.get("/")
    assert response.status_code == 200
    assert "message" in response.json()

def test_list_books():
    response = client.get("/books")
    assert response.status_code == 200
    data = response.json()
    assert "items" in data
    assert "page" in data
    assert "total" in data

def test_list_books_with_filters():
    response = client.get("/books?q=orwell&min_avg=3.5&page_size=5")
    assert response.status_code == 200

def test_get_book():
    # First, get a book ID from the list
    list_response = client.get("/books?page_size=1")
    if list_response.json()["items"]:
        book_id = list_response.json()["items"][0]["book_id"]
        
        response = client.get(f"/books/{book_id}")
        assert response.status_code == 200
        assert response.json()["book_id"] == book_id

def test_get_nonexistent_book():
    response = client.get("/books/999999")
    assert response.status_code == 404

def test_get_ratings_summary():
    # Get a book that likely has ratings
    response = client.get("/books?page_size=1")
    if response.json()["items"]:
        book_id = response.json()["items"][0]["book_id"]
        
        summary_response = client.get(f"/books/{book_id}/ratings/summary")
        # This might be 404 if no ratings exist, which is valid
        assert summary_response.status_code in [200, 404]

def test_health_check():
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"

def test_metrics():
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "books_total" in response.json()

def test_unauthorized_rating_post():
    response = client.post("/ratings", json={
        "user_id": 1,
        "book_id": 1,
        "rating": 5
    })
    assert response.status_code == 401

def test_authorized_rating_post():
    # This will fail if book doesn't exist, but tests auth
    response = client.post(
        "/ratings", 
        json={"user_id": 1, "book_id": 999999, "rating": 5},
        headers={"x-api-key": "dev-key-123"}
    )
    # Should be 404 (book not found) not 401 (unauthorized)
    assert response.status_code != 401