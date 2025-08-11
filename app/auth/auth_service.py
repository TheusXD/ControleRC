# Authentication logic
import os
import hashlib
from typing import Dict, Any, Optional, Tuple

from app.services.firebase_service import FirebaseService


class AuthService:
    """
    A service class for handling user authentication.
    """

    def __init__(self, firebase_service: FirebaseService):
        """
        Initializes the AuthService.

        Args:
            firebase_service (FirebaseService): An instance of the FirebaseService.
        """
        self.firebase_service = firebase_service

    def _hash_password(self, password: str, salt: Optional[bytes] = None) -> Tuple[bytes, bytes]:
        """Hashes a password with a salt."""
        if salt is None:
            salt = os.urandom(16)
        hashed_password = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt, 100000)
        return hashed_password, salt

    def _check_password(self, stored_password: bytes, salt: bytes, provided_password: str) -> bool:
        """Verifies a password against a stored hash and salt."""
        if salt is None or stored_password is None:
            return False
        return stored_password == self._hash_password(provided_password, salt)[0]

    def login(self, username: str, password: str) -> Optional[Dict[str, Any]]:
        """
        Authenticates a user.

        Args:
            username (str): The username.
            password (str): The password.

        Returns:
            Optional[Dict[str, Any]]: The user's data if authentication is successful, otherwise None.
        """
        user_df = self.firebase_service.query_docs("users", "username", "==", username)
        if not user_df.empty:
            user_data = user_df.iloc[0].to_dict()
            stored_password = user_data.get('password')
            salt = user_data.get('salt')
            # The stored password and salt might be in bytes format from firestore
            if isinstance(stored_password, str):
                stored_password = stored_password.encode('latin1')
            if isinstance(salt, str):
                salt = salt.encode('latin1')

            if self._check_password(stored_password, salt, password):
                return user_data
        return None

    def register_user(self, username: str, password: str, is_gestor: bool) -> Optional[Dict[str, Any]]:
        """
        Registers a new user.
        """
        existing_user = self.firebase_service.query_docs("users", "username", "==", username)
        if not existing_user.empty:
            raise ValueError("Este nome de utilizador já existe.")

        all_users = self.firebase_service.get_all_docs("users")
        role = "admin" if all_users.empty else "gestor" if is_gestor else "user"
        status = "active" if role == "admin" or not is_gestor else "pending"

        hashed_pw, salt = self._hash_password(password)
        user_data = {
            "username": username,
            "password": hashed_pw,
            "salt": salt,
            "role": role,
            "status": status
        }

        if self.firebase_service.add_doc("users", user_data):
            return user_data
        return None