from pymongo import MongoClient
from datetime import datetime

# Connect to the MongoDB instance
client = MongoClient('mongodb://localhost:27017/')

# Select the database to use.
db = client['PlaygroundDB']

# Insert a few documents into the sales collection.
db.sales.insert_many([
  { 'item': 'abc', 'price': 10, 'quantity': 2, 'date': datetime(2014, 3, 1, 8, 0, 0) },
  { 'item': 'jkl', 'price': 20, 'quantity': 1, 'date': datetime(2014, 3, 1, 9, 0, 0) },
  { 'item': 'xyz', 'price': 5, 'quantity': 10, 'date': datetime(2014, 3, 15, 9, 0, 0) },
  { 'item': 'xyz', 'price': 5, 'quantity': 20, 'date': datetime(2014, 4, 4, 11, 21, 39, 736000) },
  { 'item': 'abc', 'price': 10, 'quantity': 10, 'date': datetime(2014, 4, 4, 21, 23, 13, 331000) },
  { 'item': 'def', 'price': 7.5, 'quantity': 5, 'date': datetime(2015, 6, 4, 5, 8, 13) },
  { 'item': 'def', 'price': 7.5, 'quantity': 10, 'date': datetime(2015, 9, 10, 8, 43) },
  { 'item': 'abc', 'price': 10, 'quantity': 5, 'date': datetime(2016, 2, 6, 20, 20, 13) },
])

# Run a find command to view items sold on April 4th, 2014.
sales_on_april4th = db.sales.find({
  'date': { '$gte': datetime(2014, 4, 4), '$lt': datetime(2014, 4, 5) }
})
sales_count = db.sales.count_documents({
  'date': { '$gte': datetime(2014, 4, 4), '$lt': datetime(2014, 4, 5) }
})

# Print a message to the output window.
print(f"{sales_count} sales occurred on April 4th, 2014.")

# Here we run an aggregation and open a cursor to the results.

cursor = db.sales.aggregate([
  # Find all of the sales that occurred in 2014.
  { '$match': { 'date': { '$gte': datetime(2014, 1, 1), '$lt': datetime(2015, 1, 1) } } },
  # Group the total sales for each product.
  { '$group': { '_id': '$item', 'totalSaleAmount': { '$sum': { '$multiply': [ '$price', '$quantity' ] } } } }
])

# Print the result of the aggregation.
for document in cursor:
    print(document)
