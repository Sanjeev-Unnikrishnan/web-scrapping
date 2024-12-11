{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "6e8334d9-c8cb-44fa-9482-2552ba736032",
   "metadata": {},
   "outputs": [],
   "source": [
    "#Importing the libraries\n",
    "import sqlite3\n",
    "import pandas as pd\n",
    "import os"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 14,
   "id": "752e5358-cfd6-4fd9-a154-fc2496caeb64",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Connecting to the SQLite database\n",
    "db_name = 'LibraryDatabase.db'\n",
    "datacon = sqlite3.connect(db_name)\n",
    "cursor = datacon.cursor()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 16,
   "id": "73f89840-3b82-401c-a3d8-ad815292d351",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Creating the tables for the Library Database\n",
    "# Users Table\n",
    "cursor.execute('''\n",
    "CREATE TABLE IF NOT EXISTS Users (\n",
    "    user_id INTEGER PRIMARY KEY,\n",
    "    full_name TEXT NOT NULL,\n",
    "    enabled TEXT NOT NULL,\n",
    "    last_login TEXT\n",
    ")\n",
    "''')\n",
    "\n",
    "# Addresses Table\n",
    "cursor.execute('''\n",
    "CREATE TABLE IF NOT EXISTS Addresses (\n",
    "    user_id INTEGER,\n",
    "    street TEXT,\n",
    "    city TEXT,\n",
    "    state TEXT,\n",
    "    FOREIGN KEY(user_id) REFERENCES Users(user_id)\n",
    ")\n",
    "''')\n",
    "\n",
    "# Books Table\n",
    "cursor.execute('''\n",
    "CREATE TABLE IF NOT EXISTS Books (\n",
    "    book_id INTEGER PRIMARY KEY,\n",
    "    title TEXT NOT NULL,\n",
    "    author TEXT,\n",
    "    published_date TEXT,\n",
    "    isbn TEXT\n",
    ")\n",
    "''')\n",
    "\n",
    "# Checkouts Table\n",
    "cursor.execute('''\n",
    "CREATE TABLE IF NOT EXISTS Checkouts (\n",
    "    checkout_id INTEGER PRIMARY KEY,\n",
    "    user_id INTEGER,\n",
    "    book_id INTEGER,\n",
    "    checkout_date TEXT,\n",
    "    return_date TEXT,\n",
    "    FOREIGN KEY(user_id) REFERENCES Users(user_id),\n",
    "    FOREIGN KEY(book_id) REFERENCES Books(book_id)\n",
    ")\n",
    "''')\n",
    "\n",
    "# Reviews Table\n",
    "cursor.execute('''\n",
    "CREATE TABLE IF NOT EXISTS Reviews (\n",
    "    review_id INTEGER PRIMARY KEY,\n",
    "    book_id INTEGER,\n",
    "    reviewer_name TEXT,\n",
    "    content TEXT,\n",
    "    rating INTEGER,\n",
    "    published_date TEXT,\n",
    "    FOREIGN KEY(book_id) REFERENCES Books(book_id)\n",
    ")\n",
    "''')\n",
    "\n",
    "# Logs Table (for storing checkout logs)\n",
    "cursor.execute('''\n",
    "CREATE TABLE IF NOT EXISTS Logs (\n",
    "    log_id INTEGER PRIMARY KEY AUTOINCREMENT,\n",
    "    user_id INTEGER,\n",
    "    checkout_date TEXT,\n",
    "    FOREIGN KEY(user_id) REFERENCES Users(user_id)\n",
    ")\n",
    "''')\n",
    "\n",
    "datacon.commit()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 47,
   "id": "c3dd8dc6-0063-45b2-9e2e-22e255c23db1",
   "metadata": {},
   "outputs": [],
   "source": [
    "cursor.execute('''\n",
    "CREATE TRIGGER IF NOT EXISTS log_checkout\n",
    "AFTER INSERT ON Checkouts\n",
    "FOR EACH ROW\n",
    "BEGIN\n",
    "    INSERT INTO Logs (user_id, checkout_date) VALUES (NEW.user_id, NEW.checkout_date);\n",
    "END;\n",
    "''')\n",
    "\n",
    "datacon.commit()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 49,
   "id": "24bd0e31-9495-4db9-aae7-e11df8452461",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Inserting data into Users table\n",
    "cursor.execute(\"INSERT OR IGNORE INTO Users (user_id, full_name, enabled, last_login) VALUES (1, 'John Smith', 'f', '2017-10-25 10:26:10.015152')\")\n",
    "cursor.execute(\"INSERT OR IGNORE INTO Users (user_id, full_name, enabled, last_login) VALUES (2, 'Alice Walker', 't', '2017-10-25 10:26:50.294561')\")\n",
    "cursor.execute(\"INSERT OR IGNORE INTO Users (user_id, full_name, enabled, last_login) VALUES (3, 'Harry Potter', 't', '2017-10-25 10:26:50.294561')\")\n",
    "cursor.execute(\"INSERT OR IGNORE INTO Users (user_id, full_name, enabled, last_login) VALUES (5, 'Jane Smith', 't', '2017-10-25 10:36:43.324015')\")\n",
    "\n",
    "# Inserting data into Addresses table\n",
    "cursor.execute(\"INSERT OR IGNORE INTO Addresses (user_id, street, city, state) VALUES (1, '1 Market Street', 'San Francisco', 'CA')\")\n",
    "cursor.execute(\"INSERT OR IGNORE INTO Addresses (user_id, street, city, state) VALUES (2, '2 Elm Street', 'San Francisco', 'CA')\")\n",
    "cursor.execute(\"INSERT OR IGNORE INTO Addresses (user_id, street, city, state) VALUES (3, '3 Main Street', 'Boston', 'MA')\")\n",
    "\n",
    "# Inserting data into Books table\n",
    "cursor.execute(\"INSERT OR IGNORE INTO Books (book_id, title, author, published_date, isbn) VALUES (1, 'My First SQL book', 'Mary Parker', '2012-02-22 12:08:17.320053-03', '981483029127')\")\n",
    "cursor.execute(\"INSERT OR IGNORE INTO Books (book_id, title, author, published_date, isbn) VALUES (2, 'My Second SQL book', 'John Mayer', '1972-07-03 09:22:45.050088-07', '857300923713')\")\n",
    "cursor.execute(\"INSERT OR IGNORE INTO Books (book_id, title, author, published_date, isbn) VALUES (3, 'My Third SQL book', 'Cary Flint', '2015-10-18 14:05:44.547516-07', '523120967812')\")\n",
    "\n",
    "# Inserting data into Checkouts table\n",
    "cursor.execute(\"INSERT OR IGNORE INTO Checkouts (checkout_id, user_id, book_id, checkout_date, return_date) VALUES (1, 1, 1, '2017-10-15 14:43:18.095143-07', '2017-10-13 13:05:12.673382-05')\")\n",
    "cursor.execute(\"INSERT OR IGNORE INTO Checkouts (checkout_id, user_id, book_id, checkout_date, return_date) VALUES (2, 2, 2, '2017-10-05 16:22:44.593188-07', '2017-10-13 15:05:12.673382-05')\")\n",
    "cursor.execute(\"INSERT OR IGNORE INTO Checkouts (checkout_id, user_id, book_id, checkout_date, return_date) VALUES (3, 2, 2, '2017-10-15 11:11:24.994973-07', '2017-10-22 17:47:10.407569-07')\")\n",
    "cursor.execute(\"INSERT OR IGNORE INTO Checkouts (checkout_id, user_id, book_id, checkout_date, return_date) VALUES (4, 5, 3, '2017-10-15 09:27:07.215217-07', NULL)\")\n",
    "\n",
    "# Inserting data into Reviews table\n",
    "cursor.execute(\"INSERT OR IGNORE INTO Reviews (review_id, book_id, reviewer_name, content, rating, published_date) VALUES (1, 1, 'John Smith', 'My first review', 4, '2017-12-10 05:50:11.127281-02')\")\n",
    "cursor.execute(\"INSERT OR IGNORE INTO Reviews (review_id, book_id, reviewer_name, content, rating, published_date) VALUES (2, 2, 'John Smith', 'My second review', 5, '2017-10-13 15:05:12.673382-05')\")\n",
    "cursor.execute(\"INSERT OR IGNORE INTO Reviews (review_id, book_id, reviewer_name, content, rating, published_date) VALUES (3, 2, 'Alice Walker', 'Another review', 1, '2017-10-22 23:47:10.407569-07')\")\n",
    "\n",
    "datacon.commit()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 51,
   "id": "b30d2461-5098-4a7d-b965-0181a8277662",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "\n",
      "Books checked out by John Smith:\n",
      "('My First SQL book', 'Mary Parker', '981483029127')\n"
     ]
    }
   ],
   "source": [
    "# 1st Query- Find the title, authors, and isbn of the books that 'John Smith' has checked out\n",
    "query_1 = '''\n",
    "SELECT Books.title, Books.author, Books.isbn\n",
    "FROM Checkouts\n",
    "JOIN Users ON Users.user_id = Checkouts.user_id\n",
    "JOIN Books ON Books.book_id = Checkouts.book_id\n",
    "WHERE Users.full_name = 'John Smith';\n",
    "'''\n",
    "cursor.execute(query_1)\n",
    "print(\"\\nBooks checked out by John Smith:\")\n",
    "for row in cursor.fetchall():\n",
    "    print(row)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 61,
   "id": "b901e4e9-c846-4997-8c1d-823beeb180e3",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Reviewers for 'My Third SQL book':\n",
      "None\n"
     ]
    }
   ],
   "source": [
    "# 2nd Query- Find all reviewers for the book \"My Third SQL book\" (Replacing blank output with 'None')\n",
    "query_2 = '''\n",
    "SELECT reviewer_name\n",
    "FROM Books\n",
    "LEFT JOIN Reviews ON Books.book_id = Reviews.book_id\n",
    "WHERE Books.title = 'My Third SQL book';\n",
    "'''\n",
    "cursor.execute(query_2)\n",
    "print(\"Reviewers for 'My Third SQL book':\")\n",
    "for row in cursor.fetchall():\n",
    "        print(row[0])"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 65,
   "id": "a92de86e-d951-40fb-afef-9aadf5d16545",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Users with no books checked out:\n",
      "('Harry Potter',)\n"
     ]
    }
   ],
   "source": [
    "# 3rd Query- Find the users that have no books checked out\n",
    "query_3 = '''\n",
    "SELECT full_name\n",
    "FROM Users\n",
    "WHERE user_id NOT IN (\n",
    "    SELECT user_id FROM Checkouts\n",
    ");\n",
    "'''\n",
    "cursor.execute(query_3)\n",
    "print(\"Users with no books checked out:\")\n",
    "for row in cursor.fetchall():\n",
    "    print(row)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 75,
   "id": "bed7a7f7-1c19-4558-8326-08bb14e92a01",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Logs Tables:-\n",
      "(1, 1, '2017-10-15 14:43:18.095143-07')\n",
      "(2, 2, '2017-10-05 16:22:44.593188-07')\n",
      "(3, 2, '2017-10-15 11:11:24.994973-07')\n",
      "(4, 5, '2017-10-15 09:27:07.215217-07')\n"
     ]
    }
   ],
   "source": [
    "# 4th Query- Show all records from Logs table\n",
    "query_4 = 'SELECT * FROM Logs;'\n",
    "cursor.execute(query_4)\n",
    "print(\"Logs Tables:-\")\n",
    "for row in cursor.fetchall():\n",
    "    print(row)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 77,
   "id": "d2f6a5b1-2044-4f5b-b76c-697a511c66d2",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Books checked out by John Smith (Pandas DataFrame):\n",
      "               title       author          isbn\n",
      "0  My First SQL book  Mary Parker  981483029127\n"
     ]
    }
   ],
   "source": [
    "# Pandas Corresponding to 1st Query\n",
    "df_query_1 = pd.read_sql_query(query_1, connection)\n",
    "print(\"Books checked out by John Smith (Pandas DataFrame):\")\n",
    "print(df_query_1)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 79,
   "id": "203d4d12-7274-413c-b653-fc5769c44bfd",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Reviewers for 'My Third SQL book' (Pandas DataFrame):\n",
      "  reviewer_name\n",
      "0           N/A\n"
     ]
    }
   ],
   "source": [
    "# Pandas Corresponding to 2nd Query\n",
    "df_query_2 = pd.read_sql_query(query_2, connection)\n",
    "print(\"Reviewers for 'My Third SQL book' (Pandas DataFrame):\")\n",
    "print(df_query_2.fillna('N/A'))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 81,
   "id": "18ddc26c-b54b-440e-b13c-5d394dec3048",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Users with no books checked out (Pandas DataFrame):\n",
      "      full_name\n",
      "0  Harry Potter\n"
     ]
    }
   ],
   "source": [
    "# Pandas Corresponding to 3rd Query\n",
    "df_query_3 = pd.read_sql_query(query_3, connection)\n",
    "print(\"Users with no books checked out (Pandas DataFrame):\")\n",
    "print(df_query_3)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 83,
   "id": "2bcfde98-1312-47fd-9108-ba8c45a0e902",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Logs Tables (Pandas DataFrame):\n",
      "   log_id  user_id                  checkout_date\n",
      "0       1        1  2017-10-15 14:43:18.095143-07\n",
      "1       2        2  2017-10-05 16:22:44.593188-07\n",
      "2       3        2  2017-10-15 11:11:24.994973-07\n",
      "3       4        5  2017-10-15 09:27:07.215217-07\n"
     ]
    }
   ],
   "source": [
    "# Pandas Corresponding to 4th Query\n",
    "df_query_4 = pd.read_sql_query(query_4, connection)\n",
    "print(\"Logs Tables (Pandas DataFrame):\")\n",
    "print(df_query_4)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 85,
   "id": "c1e3cd0f-41f6-4013-8b5e-e1f8c3110a42",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Closing of the connection\n",
    "datacon.close()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "aa8150fc-2cf7-4e55-8bda-3ba7260844ee",
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.12.4"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
