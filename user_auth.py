# This file handles all user authentication logic and database interaction.

import bcrypt
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from db_config import get_user_sql_engine # Import our new User DB engine

def hash_password(password):
    """Hashes a password for storing."""
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())

def check_password(plain_password, hashed_password):
    """Checks a plain password against a stored hash."""
    # hashed_password from DB might be string, needs encoding
    return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password.encode('utf-8'))

def register_user(username, password, email):
    """
    Registers a new user in the User Database.
    Returns (True, "Success Message") or (False, "Error Message")
    """
    if not username or not password or not email:
        return (False, "Username, password, and email cannot be empty.")

    if len(password) < 8:
        return (False, "Password must be at least 8 characters long.")

    hashed_password = hash_password(password)
    engine = get_user_sql_engine()

    # Check if engine connection failed
    if engine is None:
        return (False, "Database connection failed. Cannot register user.")

    try:
        with engine.connect() as conn:
            with conn.begin() as trans: # Start a transaction
                sql = text("""
                INSERT INTO Users (Username, Email, PasswordHash)
                VALUES (:username, :email, :password_hash)
                """)
                conn.execute(sql, {
                    "username": username,
                    "email": email,
                    "password_hash": hashed_password.decode('utf-8') # Store hash as string
                })
                # Transaction is automatically committed here if no error
            return (True, "User registered successfully! You can now log in.")

    except IntegrityError as e:
        # This error is raised by SQLAlchemy when a UNIQUE constraint is violated
        if 'users_username_key' in str(e.orig):
            return (False, "This username is already taken. Please choose another.")
        elif 'users_email_key' in str(e.orig):
            return (False, "This email is already registered. Please use another.")
        else:
            # Mask detailed DB errors from the user for security
            return (False, f"Database error. Please try again.")

    except Exception as error:
        # Mask detailed errors
        print(f"Unexpected registration error: {error}") # Log for debugging
        return (False, f"An unexpected error occurred. Please try again later.")

def login_user(username, password):
    """
    Logs in a user by checking credentials against the User Database.
    Returns (True, user_id, "Success Message") or (False, None, "Error Message")
    """
    if not username or not password:
        return (False, None, "Username and password cannot be empty.")

    engine = get_user_sql_engine()

    # Check if engine connection failed
    if engine is None:
        return (False, None, "Database connection failed. Cannot log in.")

    try:
        with engine.connect() as conn:
            sql = text("SELECT UserID, PasswordHash FROM Users WHERE Username = :username")
            result = conn.execute(sql, {"username": username}).fetchone() # Use fetchone() safely

            if result:
                user_id, hashed_password_from_db = result
                # Check the provided password against the stored hash
                # Need to encode DB hash if it's stored as plain string
                if check_password(password, hashed_password_from_db.encode('utf-8')):
                    # --- Return user_id on success ---
                    return (True, user_id, "Login successful!")
                else:
                    return (False, None, "Invalid username or password.")
            else:
                # User not found
                return (False, None, "Invalid username or password.")

    except Exception as error:
        # Mask detailed errors
        print(f"Unexpected login error: {error}") # Log for debugging
        return (False, None, f"An error occurred. Please try again later.")