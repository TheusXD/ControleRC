def __init__(self, creds: Dict[str, Any]):
    if not firebase_admin._apps:
        cred_dict = creds
        cred_dict['private_key'] = cred_dict['private_key'].replace('\\n', '\n')
        cert = credentials.Certificate(cred_dict)
        firebase_admin.initialize_app(cert, {
            'storageBucket': f'{cred_dict["project_id"]}.appspot.com'
        })
    self.db = firestore.client()
    self.bucket = storage.bucket()  # ✅ ESSENCIAL: inicializa o bucket