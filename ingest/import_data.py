import pandas as pd
import os
import sys
from pymongo import UpdateOne
from app.database import get_database

def load_sample_data():
    """Load sample data from GitHub URLs"""
    base_url = "https://raw.githubusercontent.com/zygmuntz/goodbooks-10k/master/samples/"
    
    print("Loading sample data from GitHub...")
    
    # Load CSVs
    books_df = pd.read_csv(f"{base_url}books.csv")
    ratings_df = pd.read_csv(f"{base_url}ratings.csv")
    tags_df = pd.read_csv(f"{base_url}tags.csv")
    book_tags_df = pd.read_csv(f"{base_url}book_tags.csv")
    to_read_df = pd.read_csv(f"{base_url}to_read.csv")
    
    return books_df, ratings_df, tags_df, book_tags_df, to_read_df

def load_full_data():
    """Load full dataset (when available)"""
    base_url = "https://raw.githubusercontent.com/zygmuntz/goodbooks-10k/master/"
    
    print("Loading full dataset from GitHub...")
    
    try:
        books_df = pd.read_csv(f"{base_url}books.csv")
        ratings_df = pd.read_csv(f"{base_url}ratings.csv")
        tags_df = pd.read_csv(f"{base_url}tags.csv")
        book_tags_df = pd.read_csv(f"{base_url}book_tags.csv")
        to_read_df = pd.read_csv(f"{base_url}to_read.csv")
        
        return books_df, ratings_df, tags_df, book_tags_df, to_read_df
    except Exception as e:
        print(f"Error loading full dataset: {e}")
        print("Falling back to sample data...")
        return load_sample_data()

def clean_dataframe(df):
    """Clean DataFrame by replacing NaN values"""
    return df.fillna("")

def import_to_mongodb(use_full_data=False):
    """Import CSV data to MongoDB"""
    db = get_database()
    
    if use_full_data:
        books_df, ratings_df, tags_df, book_tags_df, to_read_df = load_full_data()
    else:
        books_df, ratings_df, tags_df, book_tags_df, to_read_df = load_sample_data()
    
    books_df = clean_dataframe(books_df)
    tags_df = clean_dataframe(tags_df)
    
    print(f"Importing {len(books_df)} books...")
    print(f"Importing {len(ratings_df)} ratings...")
    print(f"Importing {len(tags_df)} tags...")
    print(f"Importing {len(book_tags_df)} book tags...")
    print(f"Importing {len(to_read_df)} to-read entries...")
    
    books_operations = []
    for _, row in books_df.iterrows():
        books_operations.append(
            UpdateOne(
                {"book_id": row["book_id"]},
                {"$set": row.to_dict()},
                upsert=True
            )
        )
    
    if books_operations:
        db.books.bulk_write(books_operations)
    
    ratings_operations = []
    for _, row in ratings_df.iterrows():
        ratings_operations.append(
            UpdateOne(
                {"user_id": row["user_id"], "book_id": row["book_id"]},
                {"$set": row.to_dict()},
                upsert=True
            )
        )
    
    if ratings_operations:
        db.ratings.bulk_write(ratings_operations)
    tags_operations = []
    for _, row in tags_df.iterrows():
        tags_operations.append(
            UpdateOne(
                {"tag_id": row["tag_id"]},
                {"$set": row.to_dict()},
                upsert=True
            )
        )
    
    if tags_operations:
        db.tags.bulk_write(tags_operations)
    
    book_tags_operations = []
    for _, row in book_tags_df.iterrows():
        book_tags_operations.append(
            UpdateOne(
                {"goodreads_book_id": row["goodreads_book_id"], "tag_id": row["tag_id"]},
                {"$set": row.to_dict()},
                upsert=True
            )
        )
    
    if book_tags_operations:
        db.book_tags.bulk_write(book_tags_operations)
    
    to_read_operations = []
    for _, row in to_read_df.iterrows():
        to_read_operations.append(
            UpdateOne(
                {"user_id": row["user_id"], "book_id": row["book_id"]},
                {"$set": row.to_dict()},
                upsert=True
            )
        )
    
    if to_read_operations:
        db.to_read.bulk_write(to_read_operations)
    
    print("Data import completed successfully!")

if __name__ == "__main__":
    use_full = "--full" in sys.argv
    import_to_mongodb(use_full_data=use_full)