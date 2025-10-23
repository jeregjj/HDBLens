# This file handles all user authentication logic and database interaction.

import bcrypt
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from db_config import get_sql_engine # Import our new User DB engine

def hash_password(password: str) -> str:
    """Hashes a password for storing (returns UTF-8 string)."""
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def check_password(plain_password, hashed_password) -> bool:
    """
    Safely compare plain password vs stored hash.
    Accepts str/bytes/memoryview for hashed_password.
    """
    def to_bytes(v):
        if isinstance(v, bytes):
            return v
        if isinstance(v, memoryview):
            return v.tobytes()
        if isinstance(v, str):
            return v.encode("utf-8")
        return bytes(v)

    return bcrypt.checkpw(to_bytes(plain_password), to_bytes(hashed_password))


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
    engine = get_sql_engine()

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
                    "password_hash": hashed_password # Store hash as string
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

    engine = get_sql_engine()

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
                hashed_bytes = hashed_password_from_db.encode("utf-8") if isinstance(hashed_password_from_db, str) else hashed_password_from_db
                if check_password(password, hashed_bytes):
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
    
def get_user_by_id(user_id: int):
    engine = get_sql_engine()
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT userid, username, email, passwordhash
            FROM users
            WHERE userid = :uid
        """), {"uid": user_id}).fetchone()
    return row  # or None

def update_user_email(user_id: int, new_email: str) -> tuple[bool, str]:
    if not new_email:
        return (False, "Email cannot be empty.")
    engine = get_sql_engine()
    try:
        with engine.connect() as conn, conn.begin():
            conn.execute(text("""
                UPDATE users
                SET email = :email
                WHERE userid = :uid
            """), {"email": new_email, "uid": user_id})
        return (True, "Email updated.")
    except IntegrityError as e:
        if "users_email_key" in str(e.orig):
            return (False, "This email is already in use.")
        return (False, "Could not update email. Try again.")

def update_user_password(user_id: int, new_password: str, confirm_password: str) -> tuple[bool, str]:
    """
    Updates the user's password without requiring the current password.
    Used for password reset / change flows.
    """
    if not new_password or len(new_password) < 8:
        return (False, "New password must be at least 8 characters long.")
    if new_password != confirm_password:
        return (False, "New passwords do not match.")

    new_hash = hash_password(new_password)  # returns str (we store as TEXT)
    engine = get_sql_engine()

    try:
        with engine.connect() as conn, conn.begin():
            conn.execute(text("""
                UPDATE users
                SET passwordhash = :ph
                WHERE userid = :uid
            """), {"ph": new_hash, "uid": user_id})
        return (True, "Password has been reset successfully.")
    except Exception as e:
        print(f"Password update error: {e}")
        return (False, "An unexpected error occurred while updating your password.")

def delete_user(user_id: int, password: str) -> tuple[bool, str]:
    """
    Hard-delete the user row. Requires password confirmation.
    NOTE: This will fail if there are foreign-key references without ON DELETE CASCADE.
    """
    engine = get_sql_engine()
    with engine.connect() as conn:
        row = conn.execute(text("SELECT passwordhash FROM users WHERE userid = :uid"), {"uid": user_id}).fetchone()
        if not row:
            return (False, "User not found.")
        stored_hash = row[0]

    if not check_password(password, stored_hash):
        return (False, "Password is incorrect.")

    try:
        with engine.connect() as conn, conn.begin():
            conn.execute(text("DELETE FROM users WHERE userid = :uid"), {"uid": user_id})
        return (True, "Account deleted.")
    except IntegrityError as e:
        # Likely FK violation; suggest soft delete if you hit this
        return (False, "Account could not be deleted due to linked data. Consider soft-delete or cascade rules.")