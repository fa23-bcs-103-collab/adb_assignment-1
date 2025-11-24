from fastapi import FastAPI, Query, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from pymongo import DESCENDING, ASCENDING
import time
import os
import logging
from typing import Optional

from app.database import get_database, create_indexes
from app.models import (
    BookResponse, RatingIn, RatingResponse, Tag, 
    PaginatedResponse, RatingSummary
)
from app.auth import verify_api_key

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="GoodBooks API (MongoDB)",
    description="A REST API for book ratings and recommendations",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = (time.time() - start_time) * 1000
    
    logger.info({
        "route": str(request.url.path),
        "params": dict(request.query_params),
        "status": response.status_code,
        "latency_ms": round(process_time, 2),
        "client_ip": request.client.host,
        "ts": time.time()
    })
    
    return response

@app.on_event("startup")
async def startup_event():
    create_indexes()

@app.get("/")
async def root():
    return {"message": "GoodBooks API", "version": "1.0.0"}

@app.get("/books", response_model=PaginatedResponse)
async def list_books(
    q: Optional[str] = None,
    tag: Optional[str] = None,
    min_avg: Optional[float] = None,
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
    sort: str = Query("avg", regex="^(avg|ratings_count|year|title)$"),
    order: str = Query("desc", regex="^(asc|desc)$"),
    page: int = 1,
    page_size: int = Query(20, le=100)
):
    db = get_database()
    filter_query = {}
    
    if q:
        filter_query["$or"] = [
            {"title": {"$regex": q, "$options": "i"}},
            {"authors": {"$regex": q, "$options": "i"}}
        ]
    
    if min_avg is not None:
        filter_query["average_rating"] = {"$gte": min_avg}

    year_filter = {}
    if year_from is not None:
        year_filter["$gte"] = year_from
    if year_to is not None:
        year_filter["$lte"] = year_to
    if year_filter:
        filter_query["original_publication_year"] = year_filter
    
    if tag:
        tag_doc = db.tags.find_one({"tag_name": {"$regex": tag, "$options": "i"}})
        if tag_doc:
            tag_id = tag_doc["tag_id"]
            book_tags = list(db.book_tags.find({"tag_id": tag_id}))
            goodreads_ids = [bt["goodreads_book_id"] for bt in book_tags]
            filter_query["goodreads_book_id"] = {"$in": goodreads_ids}
    
    sort_map = {
        "avg": "average_rating",
        "ratings_count": "ratings_count", 
        "year": "original_publication_year",
        "title": "title"
    }
    sort_field = sort_map.get(sort, "average_rating")
    sort_direction = DESCENDING if order == "desc" else ASCENDING
    
    total = db.books.count_documents(filter_query)
    
    skip = (page - 1) * page_size

    books_cursor = db.books.find(filter_query).sort(sort_field, sort_direction).skip(skip).limit(page_size)
    books = list(books_cursor)
    
    for book in books:
        book["_id"] = str(book["_id"])
    
    return {
        "items": books,
        "page": page,
        "page_size": page_size,
        "total": total
    }

@app.get("/books/{book_id}", response_model=BookResponse)
async def get_book(book_id: int):
    db = get_database()
    book = db.books.find_one({"book_id": book_id})
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")
    book["_id"] = str(book["_id"])
    return book

@app.get("/books/{book_id}/tags", response_model=list)
async def get_book_tags(book_id: int):
    db = get_database()
    

    book = db.books.find_one({"book_id": book_id})
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")
    
    goodreads_id = book["goodreads_book_id"]
    
    pipeline = [
        {"$match": {"goodreads_book_id": goodreads_id}},
        {"$lookup": {
            "from": "tags",
            "localField": "tag_id", 
            "foreignField": "tag_id",
            "as": "tag_info"
        }},
        {"$unwind": "$tag_info"},
        {"$project": {
            "tag_id": 1,
            "tag_name": "$tag_info.tag_name",
            "count": 1
        }}
    ]
    
    tags = list(db.book_tags.aggregate(pipeline))

    for tag in tags:
        if "_id" in tag:
            tag["_id"] = str(tag["_id"])
    
    return tags

@app.get("/authors/{author_name}/books", response_model=PaginatedResponse)
async def get_author_books(
    author_name: str,
    page: int = 1,
    page_size: int = Query(20, le=100)
):
    db = get_database()
    
    filter_query = {"authors": {"$regex": author_name, "$options": "i"}}
    
    total = db.books.count_documents(filter_query)
    skip = (page - 1) * page_size
    
    books = list(db.books.find(filter_query)
                 .sort("average_rating", DESCENDING)
                 .skip(skip)
                 .limit(page_size))
    
    for book in books:
        book["_id"] = str(book["_id"])
    
    return {
        "items": books,
        "page": page,
        "page_size": page_size,
        "total": total
    }

@app.get("/tags", response_model=PaginatedResponse)
async def get_tags(
    page: int = 1,
    page_size: int = Query(20, le=100)
):
    db = get_database()

    pipeline = [
        {"$group": {
            "_id": "$tag_id",
            "book_count": {"$sum": 1},
            "total_uses": {"$sum": "$count"}
        }},
        {"$lookup": {
            "from": "tags",
            "localField": "_id",
            "foreignField": "tag_id",
            "as": "tag_info"
        }},
        {"$unwind": "$tag_info"},
        {"$project": {
            "tag_id": "$_id",
            "tag_name": "$tag_info.tag_name",
            "book_count": 1,
            "total_uses": 1
        }},
        {"$sort": {"total_uses": DESCENDING}},
        {"$skip": (page - 1) * page_size},
        {"$limit": page_size}
    ]
    
    tags = list(db.book_tags.aggregate(pipeline))
    total = db.tags.count_documents({})
    
    for tag in tags:
        if "_id" in tag:
            del tag["_id"]
    
    return {
        "items": tags,
        "page": page,
        "page_size": page_size,
        "total": total
    }

@app.get("/users/{user_id}/to-read", response_model=list)
async def get_user_to_read(user_id: int):
    db = get_database()
    
    to_read_books = list(db.to_read.find({"user_id": user_id}))
    
    book_ids = [tr["book_id"] for tr in to_read_books]
    books = list(db.books.find({"book_id": {"$in": book_ids}}))
    
    for book in books:
        book["_id"] = str(book["_id"])
    
    return books

@app.get("/books/{book_id}/ratings/summary", response_model=RatingSummary)
async def get_ratings_summary(book_id: int):
    db = get_database()

    pipeline = [
        {"$match": {"book_id": book_id}},
        {"$group": {
            "_id": "$book_id",
            "average_rating": {"$avg": "$rating"},
            "ratings_count": {"$sum": 1},
            "histogram": {
                "$push": "$rating"
            }
        }}
    ]
    
    result = list(db.ratings.aggregate(pipeline))
    
    if not result:
        raise HTTPException(status_code=404, detail="No ratings found for this book")

    ratings = result[0]["histogram"]
    histogram = {i: 0 for i in range(1, 6)}
    for rating in ratings:
        histogram[rating] += 1
    
    return RatingSummary(
        book_id=book_id,
        average_rating=round(result[0]["average_rating"], 2),
        ratings_count=result[0]["ratings_count"],
        histogram=histogram
    )

@app.post("/ratings", response_model=dict)
async def upsert_rating(rating: RatingIn, authorized: bool = Depends(verify_api_key)):
    db = get_database()

    book = db.books.find_one({"book_id": rating.book_id})
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")

    result = db.ratings.update_one(
        {"user_id": rating.user_id, "book_id": rating.book_id},
        {"$set": rating.dict()},
        upsert=True
    )
    
    if result.upserted_id:
        return {"status": "created", "message": "Rating created successfully"}
    else:
        return {"status": "updated", "message": "Rating updated successfully"}

@app.get("/healthz")
async def health_check():
    db = get_database()
    try:
        db.command('ping')
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database connection failed: {str(e)}")

@app.get("/metrics")
async def get_metrics():
    db = get_database()
    
    book_count = db.books.count_documents({})
    rating_count = db.ratings.count_documents({})
    user_count = len(db.ratings.distinct("user_id"))
    
    return {
        "books_total": book_count,
        "ratings_total": rating_count,
        "users_total": user_count
    }