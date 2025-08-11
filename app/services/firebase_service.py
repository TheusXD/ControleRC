# Firebase service logic
import firebase_admin
from firebase_admin import credentials, firestore, storage
import streamlit as st
import pandas as pd
from datetime import datetime
import time
from typing import List, Dict, Any, Optional, Tuple

class FirebaseService:
    """
    A service class to handle all Firebase interactions.
    """

    def __init__(self, creds: Dict[str, Any]):
        """
        Initializes the Firebase connection.

        Args:
            creds (Dict[str, Any]): Firebase credentials dictionary.
        """
        try:
            if not firebase_admin._apps:
                cred_dict = creds.copy()
                if 'private_key' not in cred_dict or not cred_dict['private_key'].startswith('-----BEGIN PRIVATE KEY-----'):
                    st.error("ERRO DE FORMATAÇÃO EM secrets.toml!")
                    st.warning("A sua 'private_key' parece estar incorreta ou mal formatada.")
                    st.stop()
                cred_dict['private_key'] = cred_dict['private_key'].replace('\n', '\n')
                cred = credentials.Certificate(cred_dict)
                firebase_admin.initialize_app(cred, {
                    'storageBucket': f'{cred_dict["project_id"]}.appspot.com'
                })
            self.db = firestore.client()
            self.bucket = storage.bucket()
        except Exception as e:
            st.error(f"Erro ao inicializar o Firebase: {e}")
            st.stop()

    def get_all_docs(self, collection_name: str) -> pd.DataFrame:
        """
        Fetches all documents from a collection.
        """
        try:
            docs = self.db.collection(collection_name).stream()
            data = []
            for doc in docs:
                doc_data = doc.to_dict()
                doc_data['id'] = doc.id
                data.append(doc_data)
            if not data:
                return pd.DataFrame()
            return pd.DataFrame(data)
        except Exception as e:
            st.error(f"Erro ao buscar dados de '{collection_name}': {e}")
            return pd.DataFrame()

    def add_doc(self, collection_name: str, data_dict: Dict[str, Any]) -> bool:
        """
        Adds a new document to a collection.
        """
        try:
            data_dict['created_at'] = datetime.now()
            self.db.collection(collection_name).add(data_dict)
            return True
        except Exception as e:
            st.error(f"Erro ao adicionar documento: {e}")
            return False

    def update_doc(self, collection_name: str, doc_id: str, data_dict: Dict[str, Any]) -> bool:
        """
        Updates an existing document.
        """
        try:
            self.db.collection(collection_name).document(doc_id).update(data_dict)
            return True
        except Exception as e:
            st.error(f"Erro ao atualizar documento: {e}")
            return False

    def delete_doc(self, collection_name: str, doc_id: str) -> bool:
        """
        Deletes a document.
        """
        try:
            self.db.collection(collection_name).document(doc_id).delete()
            return True
        except Exception as e:
            st.error(f"Erro ao excluir documento: {e}")
            return False

    def query_docs(self, collection_name: str, field: str, op: str, value: Any) -> pd.DataFrame:
        """
        Queries a collection based on a field, operator, and value.
        """
        try:
            docs = self.db.collection(collection_name).where(filter=firestore.FieldFilter(field, op, value)).stream()
            data = []
            for doc in docs:
                doc_data = doc.to_dict()
                doc_data['id'] = doc.id
                data.append(doc_data)
            if not data:
                return pd.DataFrame()
            return pd.DataFrame(data)
        except Exception as e:
            st.error(f"Erro ao fazer a query: {e}")
            return pd.DataFrame()

    def upload_file(self, file_obj, file_name: str) -> Optional[str]:
        """
        Uploads a file to Firebase Storage.
        """
        try:
            path = f"uploads/{int(time.time())}_{file_name}"
            blob = self.bucket.blob(path)
            blob.upload_from_string(file_obj.getvalue(), content_type=file_obj.type)
            blob.make_public()
            return blob.public_url
        except Exception as e:
            st.error(f"Erro ao fazer upload para Firebase Storage: {e}")
            return None