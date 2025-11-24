from pymongo import MongoClient
import os

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "goodbooks")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

def get_database():
    return db

def create_indexes():
    # Books indexes
    db.books.create_index([("title", 1), ("authors", 1)])
    db.books.create_index([("average_rating", -1)])
    db.books.create_index([("book_id", 1)])
    
    # Ratings indexes
    db.ratings.create_index([("book_id", 1)])
    db.ratings.create_index([("user_id", 1), ("book_id", 1)], unique=True)
    
    # Tags indexes
    db.tags.create_index([("tag_id", 1)])
    db.tags.create_index([("tag_name", 1)])
    
    # Book tags indexes
    db.book_tags.create_index([("tag_id", 1)])
    db.book_tags.create_index([("goodreads_book_id", 1)])
    
    # To-read indexes
    db.to_read.create_index([("user_id", 1), ("book_id", 1)], unique=True)
    
    print("All indexes created successfully")