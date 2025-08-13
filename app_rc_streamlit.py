import streamlit as st
import pandas as pd
import os
import time
import hashlib
from datetime import datetime, timedelta, date
import io
import openpyxl
import firebase_admin
from firebase_admin import credentials, firestore
import plotly.express as px
from openpyxl.styles import Font, Alignment, Border, Side, PatternFill
from openpyxl.utils import get_column_letter
from pydantic import BaseModel, Field, constr, ValidationError
from typing import List, Dict, Any, Optional, Tuple
import re
import logging
import json
import base64

# Configurar o logging para monitorizar a aplicação
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(page_title="Controle de Compras", layout="wide")


# -----------------------------------------------------------------------------
# 1. MODELS (VALIDAÇÃO DE DADOS COM PYDANTIC)
# -----------------------------------------------------------------------------

class Demanda(BaseModel):
    """Schema de validação para uma Demanda."""
    solicitante_demanda: constr(min_length=1)
    descricao_necessidade: constr(min_length=5)
    tipo: str  # Campo para "Material" ou "Serviço"
    categoria: constr(min_length=1)
    # Armazena o nome, tipo e os dados do anexo em Base64
    anexo: Optional[Dict[str, str]] = None
    status_demanda: str = "Aberta"
    created_at: datetime = Field(default_factory=datetime.now)
    closed_at: Optional[datetime] = None
    historico: List[str] = []


class Requisicao(BaseModel):
    """Schema de validação para uma Requisição."""
    solicitante: constr(min_length=1)
    demanda_id: Optional[str] = None
    numero_rc: Optional[str] = None
    valor: float = Field(..., gt=0)
    status: str = "Aberto"
    created_at: datetime = Field(default_factory=datetime.now)
    historico: List[str] = []


class Pedido(BaseModel):
    """Schema de validação para um Pedido."""
    requisicao_id: constr(min_length=1)
    solicitante: constr(min_length=1)
    valor: float = Field(..., gt=0)
    numero_pedido: Optional[str] = None
    status: str = "Em Processamento"
    created_at: datetime = Field(default_factory=datetime.now)
    observacao: Optional[str] = None
    data_entrega: Optional[datetime] = None
    historico: List[str] = []


# -----------------------------------------------------------------------------
# 2. SERVICES (LÓGICA DE NEGÓCIOS E ACESSO A DADOS)
# -----------------------------------------------------------------------------

class FirebaseService:
    """Classe para encapsular todas as interações com o Firebase."""

    def __init__(self, creds: Dict[str, Any]):
        """Inicializa a conexão com o Firebase."""
        if not firebase_admin._apps:
            cred_dict = creds
            cred_dict['private_key'] = cred_dict['private_key'].replace('\\n', '\n')
            cert = credentials.Certificate(cred_dict)
            # A inicialização do Storage foi removida para não causar erro.
            firebase_admin.initialize_app(cert)
        self.db = firestore.client()
        self.bucket = None  # Bucket não será usado nesta versão.
        logger.info("Firebase Service inicializado sem Storage Bucket.")

    def get_doc(self, collection: str, doc_id: str) -> Optional[Dict[str, Any]]:
        """Busca um único documento pelo ID."""
        try:
            doc = self.db.collection(collection).document(doc_id).get()
            if doc.exists:
                doc_data = doc.to_dict()
                doc_data['id'] = doc.id
                return doc_data
            return None
        except Exception as e:
            logger.error(f"Erro ao buscar documento {doc_id} de '{collection}': {e}", exc_info=True)
            return None

    def get_docs(self, collection: str, filters: Optional[List[Tuple]] = None) -> pd.DataFrame:
        """Busca documentos de uma coleção, com a opção de aplicar filtros."""
        try:
            query = self.db.collection(collection)
            if filters:
                for f in filters:
                    query = query.where(filter=firestore.FieldFilter(f[0], f[1], f[2]))

            docs = query.stream()
            data = []
            for doc in docs:
                doc_data = doc.to_dict()
                doc_data['id'] = doc.id
                for key, value in doc_data.items():
                    if isinstance(value, datetime):
                        doc_data[key] = value
                data.append(doc_data)

            df = pd.DataFrame(data) if data else pd.DataFrame()
            if 'created_at' in df.columns:
                df['created_at'] = pd.to_datetime(df['created_at'])
                df = df.sort_values(by='created_at', ascending=False)
            return df
        except Exception as e:
            logger.error(f"Erro ao obter dados de '{collection}': {e}", exc_info=True)
            st.error(f"Erro ao buscar dados de '{collection}': {e}")
            return pd.DataFrame()

    def add_doc(self, collection: str, data: Dict[str, Any]) -> bool:
        """Adiciona um novo documento a uma coleção."""
        try:
            self.db.collection(collection).add(data)
            return True
        except Exception as e:
            logger.error(f"Erro ao adicionar documento a '{collection}': {e}", exc_info=True)
            st.error(f"Erro ao adicionar em '{collection}': {e}")
            return False

    def update_doc(self, collection: str, doc_id: str, new_data: Dict[str, Any], username: str) -> bool:
        """Atualiza um documento e registra as alterações no histórico."""
        try:
            doc_ref = self.db.collection(collection).document(doc_id)
            current_doc = doc_ref.get()
            if not current_doc.exists:
                st.error("Documento não encontrado para atualização.")
                return False

            old_data = current_doc.to_dict()
            history_log = old_data.get('historico', [])
            now_str = datetime.now().strftime('%d/%m/%Y %H:%M')

            for key, value in new_data.items():
                old_value = old_data.get(key)
                if (old_value or "") != (value or ""):
                    log_entry = f"'{key.replace('_', ' ').capitalize()}' alterado de '{old_value}' para '{value}' por {username} em {now_str}"
                    history_log.append(log_entry)

            new_data['historico'] = history_log
            doc_ref.update(new_data)
            return True
        except Exception as e:
            logger.error(f"Erro ao atualizar documento ID: {doc_id} em '{collection}': {e}", exc_info=True)
            st.error(f"Erro ao atualizar em '{collection}': {e}")
            return False

    def delete_doc(self, collection: str, doc_id: str) -> bool:
        """Exclui um documento."""
        try:
            self.db.collection(collection).document(doc_id).delete()
            return True
        except Exception as e:
            logger.error(f"Erro ao excluir documento ID: {doc_id} de '{collection}': {e}", exc_info=True)
            st.error(f"Erro ao excluir de '{collection}': {e}")
            return False

    def restore_from_backup_data(self, backup_data: Dict[str, Any]) -> bool:
        """Restaura o banco de dados a partir de um dicionário de dados."""
        try:
            collections_to_restore = ["users", "demandas", "requisicoes", "pedidos"]
            for col in collections_to_restore:
                docs_stream = self.db.collection(col).stream()
                for doc in docs_stream:
                    doc.reference.delete()

            for collection_name, documents in backup_data.items():
                if collection_name not in collections_to_restore:
                    continue
                for doc_data in documents:
                    if 'created_at' in doc_data and isinstance(doc_data['created_at'], str):
                        doc_data['created_at'] = datetime.fromisoformat(doc_data['created_at'])
                    if 'data_entrega' in doc_data and isinstance(doc_data.get('data_entrega'), str):
                        doc_data['data_entrega'] = datetime.fromisoformat(doc_data['data_entrega']) if doc_data[
                            'data_entrega'] else None
                    if 'password' in doc_data and isinstance(doc_data['password'], str):
                        doc_data['password'] = base64.b64decode(doc_data['password'])
                    if 'salt' in doc_data and isinstance(doc_data['salt'], str):
                        doc_data['salt'] = base64.b64decode(doc_data['salt'])
                    self.db.collection(collection_name).add(doc_data)

            logger.info("Backup restaurado com sucesso!")
            return True
        except Exception as e:
            logger.error(f"Erro ao restaurar o backup: {e}", exc_info=True)
            st.error(f"Falha ao restaurar o backup: {e}")
            return False


class AuthService:
    """Classe para gerenciar a autenticação de usuários."""
    SESSION_TIMEOUT_MINUTES = 30

    def __init__(self, db_service: FirebaseService):
        self.db = db_service

    def _hash_password(self, password: str, salt: Optional[bytes] = None) -> Tuple[bytes, bytes]:
        if salt is None:
            salt = os.urandom(16)
        hashed_password = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt, 100000)
        return hashed_password, salt

    def _check_password(self, stored_password, salt, provided_password: str) -> bool:
        if salt is None or stored_password is None: return False
        if isinstance(salt, str):
            try:
                salt = base64.b64decode(salt)
            except (ValueError, TypeError):
                return False
        if isinstance(stored_password, str):
            try:
                stored_password = base64.b64decode(stored_password)
            except (ValueError, TypeError):
                return False
        return stored_password == hashlib.pbkdf2_hmac('sha256', provided_password.encode('utf-8'), salt, 100000)

    def _validate_password_strength(self, password: str) -> bool:
        if len(password) < 8: return False
        if not re.search(r"[A-Z]", password): return False
        if not re.search(r"[a-z]", password): return False
        if not re.search(r"[0-9]", password): return False
        return True

    def register_user(self, username, password, is_gestor):
        if not self._validate_password_strength(password):
            st.error("A senha deve ter no mínimo 8 caracteres, com uma letra maiúscula, uma minúscula e um número.")
            return

        if not self.db.get_docs("users", [("username", "==", username)]).empty:
            st.error("Este nome de usuário já existe.");
            return

        role = "admin" if self.db.get_docs("users").empty else "gestor" if is_gestor else "user"
        status = "active" if role == "admin" or not is_gestor else "pending"
        hashed_pw, salt = self._hash_password(password)
        user_data = {"username": username, "password": hashed_pw, "salt": salt, "role": role, "status": status,
                     "created_at": datetime.now()}

        if self.db.add_doc("users", user_data):
            st.success(f"Usuário '{username}' registrado como '{role}'. Status: {status}")
            time.sleep(2);
            st.session_state.page = "Login";
            st.rerun()

    def login_user(self, username, password):
        user_df = self.db.get_docs("users", [("username", "==", username)])
        if not user_df.empty:
            user_data = user_df.iloc[0]
            if user_data['status'] == 'pending':
                st.warning("Sua conta está aguardando aprovação.")
            elif self._check_password(user_data['password'], user_data['salt'], password):
                st.session_state.logged_in = True
                st.session_state.username = user_data['username']
                st.session_state.role = user_data['role']
                st.session_state.last_activity = time.time()
                st.rerun()
            else:
                st.error("Usuário ou senha incorretos.")
        else:
            st.error("Usuário ou senha incorretos.")

    def check_session_timeout(self):
        if 'last_activity' in st.session_state:
            if time.time() - st.session_state.last_activity > self.SESSION_TIMEOUT_MINUTES * 60:
                for key in list(st.session_state.keys()): del st.session_state[key]
                st.warning("Sessão expirada. Faça login novamente.");
                time.sleep(3);
                st.rerun()
        st.session_state.last_activity = time.time()

    def change_password(self, username, old_password, new_password):
        user_df = self.db.get_docs("users", [("username", "==", username)])
        if user_df.empty: st.error("Usuário não encontrado."); return False
        user_data = user_df.iloc[0]
        if not self._check_password(user_data['password'], user_data['salt'], old_password):
            st.error("A senha antiga está incorreta.");
            return False
        if not self._validate_password_strength(new_password):
            st.error("A nova senha não é forte o suficiente.");
            return False
        hashed_pw, salt = self._hash_password(new_password)
        if self.db.update_doc("users", user_data['id'], {"password": hashed_pw, "salt": salt}, username):
            st.success("Senha alterada com sucesso!");
            return True
        return False

    def reset_password_by_admin(self, user_id, new_password):
        if not self._validate_password_strength(new_password):
            st.error("A nova senha não é forte o suficiente.");
            return False
        hashed_pw, salt = self._hash_password(new_password)
        return self.db.update_doc("users", user_id, {"password": hashed_pw, "salt": salt}, st.session_state.username)


# -----------------------------------------------------------------------------
# 3. UI / VIEWS (LÓGICA DE APRESENTAÇÃO)
# -----------------------------------------------------------------------------

def parse_brazilian_float(text: str) -> float:
    """Converte uma string de número no formato brasileiro para float."""
    if not isinstance(text, str) or not text:
        return 0.0
    try:
        # Remove pontos (separador de milhar) e substitui vírgula (decimal) por ponto
        cleaned_text = text.replace('.', '').replace(',', '.')
        return float(cleaned_text)
    except ValueError:
        st.error(f"Valor '{text}' inválido. Use o formato numérico correto, como 1.234,56")
        raise ValueError("Formato de valor inválido")


def format_brazilian_currency(value: float) -> str:
    """Formata um número float para a convenção de moeda brasileira (R$ 1.234,56)."""
    if not isinstance(value, (int, float)):
        return "R$ 0,00"
    # Formata com _ para milhar e . para decimal, depois substitui
    formatted_value = f"{value:_.2f}".replace('.', ',').replace('_', '.')
    return f"R$ {formatted_value}"


def to_excel(df: pd.DataFrame, title: str = "Relatório") -> bytes:
    output = io.BytesIO()
    df_copy = df.copy()
    for col in df_copy.select_dtypes(include=['datetimetz']).columns:
        df_copy[col] = df_copy[col].dt.tz_localize(None)
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_copy.to_excel(writer, index=False, sheet_name=title)
        workbook, worksheet = writer.book, writer.sheets[title]
        header_fill = PatternFill(start_color="4F81BD", end_color="4F81BD", fill_type="solid")
        header_font = Font(color="FFFFFF", bold=True)
        border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'),
                        bottom=Side(style='thin'))
        alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
        for col_num, col_name in enumerate(df_copy.columns, 1):
            cell = worksheet.cell(row=1, column=col_num)
            cell.fill, cell.font, cell.border, cell.alignment = header_fill, header_font, border, alignment
            column_letter = get_column_letter(col_num)
            max_len = max(df_copy[col_name].astype(str).map(len).max(), len(col_name)) + 2
            worksheet.column_dimensions[column_letter].width = min(max_len, 50)
        for row in range(2, len(df_copy) + 2):
            for col in range(1, len(df_copy.columns) + 1):
                cell = worksheet.cell(row=row, column=col)
                cell.border = border
                cell.alignment = Alignment(horizontal='left', vertical='center')
                if 'valor' in df_copy.columns[col - 1].lower(): cell.number_format = 'R$ #,##0.00'
        status_col_name = next((col for col in ['status', 'status_demanda'] if col in df_copy.columns), None)
        if status_col_name:
            fills = {
                'green': PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid"),
                'red': PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid"),
                'yellow': PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid"),
                'blue': PatternFill(start_color="DDEBF7", end_color="DDEBF7", fill_type="solid")
            }
            status_col_index = df_copy.columns.get_loc(status_col_name) + 1
            for row in range(2, len(df_copy) + 2):
                cell = worksheet.cell(row=row, column=status_col_index)
                if cell.value in ['Finalizado', 'Entregue', 'Fechada']:
                    cell.fill = fills['green']
                elif cell.value in ['Cancelado', 'Rejeitado']:
                    cell.fill = fills['red']
                elif cell.value in ['Em Processamento', 'Em Atendimento', 'Em Transporte', 'Pedido Gerado']:
                    cell.fill = fills['yellow']
                elif cell.value in ['Aberto', 'Aberta']:
                    cell.fill = fills['blue']
        worksheet.freeze_panes = 'A2'
        worksheet.auto_filter.ref = worksheet.dimensions
    return output.getvalue()


class ViewManager:
    def __init__(self, auth_service: AuthService, db_service: FirebaseService):
        self.auth, self.db = auth_service, db_service
        self._init_session_state()

    def _init_session_state(self):
        defaults = {'logged_in': False, 'username': "", 'role': "", 'page': "Login", 'confirm_delete': {},
                    'edit_id': None, 'confirm_delete_user': {}, 'reset_password_for_user': {}, 'focus_item': None,
                    'view_history_id': None, 'confirm_restore': None, 'show_notifications': False, 'notifications': []}
        for key, value in defaults.items():
            if key not in st.session_state: st.session_state[key] = value

    def run(self):
        if not st.session_state.logged_in:
            self.render_login_page()
        else:
            self.auth.check_session_timeout(); self.render_main_app()

    def render_login_page(self):
        _, col2, _ = st.columns([1, 2, 1])
        with col2:
            st.markdown(
                """<div style="text-align: center; margin-bottom: 2rem;"><span style="font-family: sans-serif; font-size: 4rem; font-weight: 900; color: var(--text-color);">ATIBAIA</span><span style="font-family: sans-serif; font-size: 4rem; font-weight: 900; color: #00AEEF;">💧</span><div style="font-family: sans-serif; font-size: 2.5rem; color: #00AEEF; letter-spacing: 0.1rem; margin-top: -1rem;">SANEAMENTO</div></div>""",
                unsafe_allow_html=True)
            if st.session_state.page == "Login":
                self._render_login_form()
                if st.button("Não tem conta? Registre-se"): st.session_state.page = "Registro"; st.rerun()
            else:
                self._render_registration_form()
                if st.button("Já tem conta? Faça login"): st.session_state.page = "Login"; st.rerun()

    def _render_login_form(self):
        st.title("🔐 Login do Sistema")
        with st.form("login_form"):
            username, password = st.text_input("Nome de Usuário"), st.text_input("Senha", type="password")
            if st.form_submit_button("Entrar", type="primary"): self.auth.login_user(username, password)

    def _render_registration_form(self):
        st.title("📝 Registro de Novo Usuário")
        with st.form("registration_form"):
            username, password, is_gestor = st.text_input("Nome de Usuário"), st.text_input("Senha",
                                                                                            type="password"), st.checkbox(
                "Sou um gestor (requer aprovação do admin)")
            if st.form_submit_button("Registrar", type="primary"): self.auth.register_user(username, password,
                                                                                           is_gestor)

    def render_main_app(self):
        self.render_sidebar()
        col1, col2 = st.columns([0.8, 0.2])
        with col1:
            st.title("🚀 Sistema de Controle de Compras")
        with col2:
            self.render_notification_bell()

        self.render_edit_modal()
        if st.session_state.view_history_id: self.render_history_modal()
        if st.session_state.get('show_notifications', False): self.render_notifications_modal()

        if st.session_state.focus_item:
            self.render_focused_view()
        else:
            tab_dashboard, tab_demandas, tab_rcs, tab_pedidos = st.tabs(
                ["📊 Dashboard", "📝 Demandas", "🛒 Requisições", "🚚 Pedidos"])
            with tab_dashboard:
                self.render_dashboard()
            with tab_demandas:
                self.render_demandas()
            with tab_rcs:
                self.render_requisicoes()
            with tab_pedidos:
                self.render_pedidos()

    def render_sidebar(self):
        with st.sidebar:
            st.write(f"👤 **{st.session_state.username}** ({st.session_state.role})")
            with st.expander("Meu Perfil"):
                with st.form("change_password_form", clear_on_submit=True):
                    st.subheader("Alterar Senha")
                    old_p, new_p, conf_p = st.text_input("Senha Antiga", type="password"), st.text_input("Nova Senha",
                                                                                                         type="password"), st.text_input(
                        "Confirmar Nova Senha", type="password")
                    if st.form_submit_button("Alterar Senha", type="primary"):
                        if new_p != conf_p:
                            st.error("As novas senhas não coincidem.")
                        else:
                            if self.auth.change_password(st.session_state.username, old_p, new_p): time.sleep(
                                2); st.rerun()
            if st.button("Logout", use_container_width=True):
                for key in list(st.session_state.keys()): del st.session_state[key]
                st.rerun()
            st.divider()
            if st.session_state.role == 'admin': self.render_admin_panel()

    def render_admin_panel(self):
        st.header("⚙️ Administração")
        with st.expander("Gerenciar Usuários", expanded=True):
            pending_users = self.db.get_docs("users", [("status", "==", "pending")])
            if not pending_users.empty:
                st.subheader("Aprovações Pendentes")
                st.warning(f"🔔 **{len(pending_users)} aprovações pendentes!**")
                for _, user in pending_users.iterrows():
                    c1, c2, c3 = st.columns([2, 1, 1])
                    c1.write(f"{user['username']} ({user['role']})")
                    if c2.button("✅", key=f"a_{user['id']}", help="Aprovar"): self.db.update_doc("users", user['id'],
                                                                                                 {"status": "active"},
                                                                                                 st.session_state.username); st.rerun()
                    if c3.button("🗑️", key=f"r_{user['id']}", help="Rejeitar"): self.db.delete_doc("users", user[
                        'id']); st.rerun()
                st.divider()
            st.subheader("Usuários Ativos")
            active_users = self.db.get_docs("users", [("status", "==", "active")])
            for _, user in active_users.iterrows():
                is_self = user['username'] == st.session_state.username
                c1, c2, c3 = st.columns([3, 1, 1])
                c1.write(f"**{user['username']}** ({user['role']}){' (Você)' if is_self else ''}")
                if c2.button("🔑", key=f"reset_pw_{user['id']}", help="Redefinir Senha",
                             disabled=is_self): st.session_state.reset_password_for_user = {'id': user['id'],
                                                                                            'username': user[
                                                                                                'username']}; st.rerun()
                if c3.button("🗑️", key=f"del_user_{user['id']}", help="Excluir Usuário",
                             disabled=is_self): st.session_state.confirm_delete_user = {'id': user['id'],
                                                                                        'username': user[
                                                                                            'username']}; st.rerun()
                if st.session_state.get('reset_password_for_user', {}).get('id') == user['id']:
                    with st.form(key=f"reset_form_{user['id']}", clear_on_submit=True):
                        st.warning(
                            f"Redefinindo senha para **{st.session_state.reset_password_for_user['username']}**.")
                        new_pass = st.text_input("Nova Senha", type="password")
                        if st.form_submit_button("Confirmar", type="primary"):
                            if self.auth.reset_password_by_admin(user['id'], new_pass): st.toast(
                                f"Senha para {user['username']} redefinida.",
                                icon="🔑"); del st.session_state.reset_password_for_user; time.sleep(1); st.rerun()
                        if st.form_submit_button("Cancelar"): del st.session_state.reset_password_for_user; st.rerun()
                if st.session_state.get('confirm_delete_user', {}).get('id') == user['id']:
                    st.error(f"Excluir **{st.session_state.confirm_delete_user['username']}**?")
                    c1, c2, _ = st.columns([1, 1, 3])
                    if c1.button("Sim, excluir", key=f"conf_del_u_{user['id']}", type="primary"): self.db.delete_doc(
                        "users", user['id']); del st.session_state.confirm_delete_user; st.toast(
                        f"Usuário {user['username']} excluído.", icon="🗑️"); time.sleep(1); st.rerun()
                    if c2.button("Cancelar",
                                 key=f"canc_del_u_{user['id']}"): del st.session_state.confirm_delete_user; st.rerun()
        st.divider()
        st.subheader("Backup e Restauro Local")
        st.download_button(label="📥 Baixar Backup Local", data=self._generate_backup_data(),
                           file_name=f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", mime="application/json",
                           use_container_width=True, type="primary")
        uploaded_file = st.file_uploader("Restaurar a partir de arquivo (.json)", type="json")
        if uploaded_file:
            if st.button("Restaurar Backup"): st.session_state.confirm_restore = uploaded_file; st.rerun()
        if st.session_state.get('confirm_restore'):
            st.error(f"Restaurar '{st.session_state.confirm_restore.name}'? Dados atuais serão perdidos.")
            rc1, rc2, _ = st.columns([1, 1, 3])
            if rc1.button("Sim, restaurar", key="conf_restore_l", type="primary"):
                if self.db.restore_from_backup_data(json.load(st.session_state.confirm_restore)): st.success(
                    "Backup restaurado!"); del st.session_state.confirm_restore; time.sleep(2); st.rerun()
            if rc2.button("Cancelar", key="canc_restore_l"): del st.session_state.confirm_restore; st.rerun()

    def render_notification_bell(self):
        num_notifications = 0
        if st.session_state.role == 'admin':
            pending_users = self.db.get_docs("users", [("status", "==", "pending")])
            num_notifications = len(pending_users)
            st.session_state.notifications = [f"Aprovação pendente: {user['username']}" for _, user in
                                              pending_users.iterrows()] if num_notifications > 0 else []
        label = f"🔔 ({num_notifications})" if num_notifications > 0 else "🔔"
        if st.button(label, help="Ver notificações"): st.session_state.show_notifications = not st.session_state.get(
            'show_notifications', False); st.rerun()

    @st.dialog("🔔 Notificações")
    def render_notifications_modal(self):
        notifications = st.session_state.get('notifications', [])
        if not notifications:
            st.info("Nenhuma notificação nova.")
        else:
            for n in notifications: st.warning(n)
        if st.button("Fechar", key="close_notifications"): st.session_state.show_notifications = False; st.rerun()

    def _render_paginated_rows(self, df: pd.DataFrame, render_function, key_suffix: str, **kwargs):
        if df.empty:
            st.info("Nenhum dado encontrado.")
            return

        items_per_page = st.selectbox("Itens por página", [5, 10, 20], key=f"items_{key_suffix}", index=1)
        total_pages = max(1, (len(df) - 1) // items_per_page + 1)
        page_key = f"page_{key_suffix}"
        if page_key not in st.session_state:
            st.session_state[page_key] = 1
        st.session_state[page_key] = min(st.session_state[page_key], total_pages)

        c1, c2, c3 = st.columns([1, 2, 1])
        if c1.button("⬅️", key=f"prev_{key_suffix}", disabled=(st.session_state[page_key] <= 1)):
            st.session_state[page_key] -= 1
            st.rerun()
        if c3.button("➡️", key=f"next_{key_suffix}", disabled=(st.session_state[page_key] >= total_pages)):
            st.session_state[page_key] += 1
            st.rerun()

        c2.write(f"Página **{st.session_state[page_key]}** de **{total_pages}**")
        start_idx = (st.session_state[page_key] - 1) * items_per_page
        for _, row in df.iloc[start_idx: start_idx + items_per_page].iterrows():
            render_function(row, **kwargs)

    def render_focused_view(self):
        """Renderiza uma visão focada em um único item (RC ou Pedido)."""
        focus_info = st.session_state.focus_item
        collection = focus_info['collection']
        doc_id = focus_info['id']

        st.subheader(f"Visualizando {collection[:-1].capitalize()} Específico")

        if st.button("⬅️ Voltar para a visão completa"):
            st.session_state.focus_item = None
            st.rerun()

        doc_data = self.db.get_doc(collection, doc_id)
        if doc_data:
            row = pd.Series(doc_data)
            all_demandas = None
            if collection == 'requisicoes':
                all_demandas = self.db.get_docs("demandas")
            self.render_data_row(row, collection=collection, all_demandas=all_demandas)
        else:
            st.error("Item não encontrado.")

    def render_dashboard(self):
        st.header("Dashboard de Métricas")
        df_demandas, df_rc, df_pedidos = self.db.get_docs("demandas"), self.db.get_docs(
            "requisicoes"), self.db.get_docs("pedidos")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total de Demandas", f"{len(df_demandas)} 📝")
        c2.metric("Total de RCs", f"{len(df_rc)} 🛒")
        c3.metric("Total de Pedidos", f"{len(df_pedidos)} 🚚")
        total_valor_rc = df_rc['valor'].sum() if not df_rc.empty else 0
        c4.metric("Valor Total em RCs", format_brazilian_currency(total_valor_rc))
        st.divider()
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Status das Demandas")
            if not df_demandas.empty:
                st.plotly_chart(
                    px.bar(df_demandas['status_demanda'].value_counts().reset_index(), x='status_demanda', y='count',
                           title="Distribuição de Status", text_auto=True, color='status_demanda',
                           labels={'status_demanda': 'Status', 'count': 'Quantidade'}), use_container_width=True)
            else:
                st.info("Nenhuma demanda para exibir.")
        with c2:
            st.subheader("Demandas por Categoria")
            if not df_demandas.empty:
                st.plotly_chart(
                    px.pie(df_demandas['categoria'].value_counts().reset_index(), names='categoria', values='count',
                           title="Distribuição por Categoria", hole=.3,
                           labels={'categoria': 'Categoria', 'count': 'Quantidade'}), use_container_width=True)
            else:
                st.info("Nenhuma categoria para exibir.")

    def render_demandas(self):
        st.header("Demandas de Compras")
        if st.session_state.role in ['admin', 'user', 'gestor']:
            with st.expander("➕ Adicionar Nova Demanda"):
                with st.form("demanda_form", clear_on_submit=True):
                    descricao = st.text_area("Descrição da Necessidade")
                    tipo = st.selectbox("Tipo", ["Material", "Serviço"], index=None, placeholder="Selecione o tipo...")
                    categorias_fixas = ["Facilities/Eletromecânica", "Manutenção de rede", "Tratamento",
                                        "Tratamento (Laboratório)"]
                    categoria = st.selectbox("Categoria", categorias_fixas, index=None,
                                             placeholder="Selecione a categoria...")
                    uploaded_file = st.file_uploader("Anexo (Opcional, máx 750KB)")
                    if st.form_submit_button("Registrar Demanda", type="primary"):
                        if not descricao or not categoria or not tipo:
                            st.error("Preencha todos os campos obrigatórios (Descrição, Tipo e Categoria).")
                            return
                        with st.spinner("Registrando demanda..."):
                            anexo_data_dict = None
                            if uploaded_file:
                                if uploaded_file.size > 750 * 1024: st.error(
                                    "Arquivo muito grande! O anexo deve ter no máximo 750 KB."); st.stop()
                                b64_data = base64.b64encode(uploaded_file.getvalue()).decode('utf-8')
                                anexo_data_dict = {"file_name": uploaded_file.name, "content_type": uploaded_file.type,
                                                   "b64_data": b64_data}
                            try:
                                demanda = Demanda(solicitante_demanda=st.session_state.username,
                                                  descricao_necessidade=descricao, tipo=tipo, categoria=categoria,
                                                  anexo=anexo_data_dict)
                                demanda_data = demanda.model_dump()
                                demanda_data['historico'] = [
                                    f"Criado por {st.session_state.username} em {datetime.now().strftime('%d/%m/%Y %H:%M')}"]
                                if self.db.add_doc("demandas", demanda_data): st.toast("✅ Demanda registrada!",
                                                                                       icon="✅"); time.sleep(
                                    1); st.rerun()
                            except ValidationError as e:
                                st.error(f"Erro de validação: {e}")
        st.header("Demandas Registradas")
        df_demandas = self.db.get_docs("demandas")
        df_rcs = self.db.get_docs("requisicoes")
        df_pedidos = self.db.get_docs("pedidos")
        self._render_paginated_rows(df_demandas, self.render_data_row, "demandas", collection="demandas",
                                    all_rcs=df_rcs, all_pedidos=df_pedidos)

    def render_requisicoes(self):
        st.header("Requisições de Compra (RCs)")
        if st.session_state.role in ['admin', 'user']:
            with st.expander("➕ Adicionar Nova Requisição"):
                st.subheader("Passo 1: Selecione a Demanda")
                df_demandas_abertas = self.db.get_docs("demandas", [("status_demanda", "==", "Aberta")])
                demanda_options = {"Selecione uma Demanda": None,
                                   **{f"ID: ...{r['id'][-6:]} - {r['descricao_necessidade'][:40]}...": r['id'] for _, r
                                      in df_demandas_abertas.iterrows()}}
                selected_demanda_id = demanda_options.get(
                    st.selectbox("Vincular à Demanda", list(demanda_options.keys()), label_visibility="collapsed"))
                if selected_demanda_id:
                    details = df_demandas_abertas[df_demandas_abertas['id'] == selected_demanda_id].iloc[0]
                    with st.container(border=True):
                        st.markdown("##### Detalhes da Demanda Selecionada")
                        st.text_area("Descrição da Necessidade", value=details['descricao_necessidade'], height=150,
                                     disabled=True)
                        c1, c2, c3 = st.columns(3)
                        c1.markdown(f"**Tipo:**\n\n`{details.get('tipo', 'N/A')}`")
                        c2.markdown(f"**Categoria:**\n\n`{details['categoria']}`")
                        c3.markdown(f"**Solicitante:**\n\n`{details['solicitante_demanda']}`")

                    st.subheader("Passo 2: Detalhes da Requisição")
                    with st.form("requisicao_form_details", clear_on_submit=True):
                        valor_str = st.text_input("Valor da Requisição (R$)", placeholder="Ex: 1.234,56")
                        numero_rc = st.text_input("Número da RC (opcional)")
                        if st.form_submit_button("Registrar Requisição", type="primary"):
                            try:
                                valor = parse_brazilian_float(valor_str)
                                if valor <= 0:
                                    st.error("O valor da requisição deve ser maior que zero.")
                                    return
                                requisicao = Requisicao(solicitante=st.session_state.username,
                                                        demanda_id=selected_demanda_id, valor=valor,
                                                        numero_rc=numero_rc or None)
                                req_data = requisicao.model_dump()
                                req_data['historico'] = [
                                    f"Criado por {st.session_state.username} em {datetime.now().strftime('%d/%m/%Y %H:%M')}"]
                                if self.db.add_doc("requisicoes", req_data):
                                    self.db.update_doc("demandas", selected_demanda_id,
                                                       {"status_demanda": "Em Atendimento"}, st.session_state.username)
                                    st.toast("✅ Requisição registrada!", icon="✅");
                                    time.sleep(1);
                                    st.rerun()
                            except ValueError:
                                return
                            except Exception as e:
                                st.error(f"Erro ao registrar: {e}")
        st.header("Requisições Registradas")
        df_rc = self.db.get_docs("requisicoes")
        df_demandas = self.db.get_docs("demandas")
        if not df_rc.empty: st.download_button("📥 Exportar para Excel", to_excel(df_rc, "Relatório de RCs"),
                                               'relatorio_rcs.xlsx')
        self._render_paginated_rows(df_rc, self.render_data_row, "rcs", collection="requisicoes",
                                    all_demandas=df_demandas)

    def render_pedidos(self):
        st.header("Pedidos de Compra")
        all_pedidos = self.db.get_docs("pedidos")
        tabs = st.tabs(["⏳ Em Andamento", "✅ Entregues", "❌ Cancelados"])
        status_map = [['Em Processamento', 'Em Transporte'], ['Entregue'], ['Cancelado']]
        for tab, statuses in zip(tabs, status_map):
            with tab:
                df_filtered = all_pedidos[
                    all_pedidos['status'].isin(statuses)] if not all_pedidos.empty else pd.DataFrame()
                if not df_filtered.empty: st.download_button("📥 Exportar",
                                                             to_excel(df_filtered, f"Pedidos {statuses[0]}"),
                                                             f'pedidos_{statuses[0].lower()}.xlsx',
                                                             key=f'btn_{statuses[0]}')
                self._render_paginated_rows(df_filtered, self.render_data_row, f"pedidos_{statuses[0]}",
                                            collection="pedidos")

    def render_data_row(self, row: pd.Series, collection: str, all_rcs=None, all_pedidos=None, all_demandas=None):
        key, role = f"{collection}_{row['id']}", st.session_state.role
        with st.container(border=True):
            if collection == 'demandas':
                title = f"Demanda: {row.get('descricao_necessidade', '')} (Tipo: {row.get('tipo', 'N/A')} | Cat: {row.get('categoria', 'N/A')})"
                status = row.get('status_demanda', 'N/A')
            elif collection == 'requisicoes':
                title = f"RC: {row.get('numero_rc', 'S/N')} | Valor: {format_brazilian_currency(row.get('valor', 0))}"
                status = row.get('status', 'N/A')
            else:  # Pedidos
                title = f"Pedido: {row.get('numero_pedido', 'S/N')} | Valor: {format_brazilian_currency(row.get('valor', 0))}"
                status = row.get('status', 'N/A')

            st.markdown(
                f"**{title}**\n\n**Status:** `{status}` | **Criado por:** `{row.get('solicitante', row.get('solicitante_demanda', 'N/A'))}` em `{row.get('created_at').strftime('%d/%m/%Y')}`")

            if collection == 'requisicoes' and pd.notna(row.get('demanda_id')):
                demanda_id = row['demanda_id']
                if all_demandas is not None and not all_demandas.empty:
                    demanda_info = all_demandas[all_demandas['id'] == demanda_id]
                    if not demanda_info.empty:
                        descricao = demanda_info.iloc[0]['descricao_necessidade']
                        with st.expander("Ver Descrição da Demanda Original"):
                            st.info(descricao)

            if collection == 'demandas':
                anexo_info = row.get('anexo')
                if anexo_info and isinstance(anexo_info, dict) and 'b64_data' in anexo_info:
                    try:
                        file_bytes = base64.b64decode(anexo_info['b64_data'])
                        st.download_button(label=f"📥 Baixar anexo: {anexo_info['file_name']}", data=file_bytes,
                                           file_name=anexo_info['file_name'],
                                           mime=anexo_info.get('content_type', 'application/octet-stream'),
                                           key=f"download_{key}")
                    except Exception as e:
                        st.error(f"Erro no anexo: {e}")
            if collection == 'pedidos':
                if pd.notna(row.get('data_entrega')): st.markdown(
                    f"**Data de Entrega:** `{pd.to_datetime(row.get('data_entrega')).strftime('%d/%m/%Y')}`")
                if row.get('observacao'): st.markdown(f"**Observação:** *{row.get('observacao')}*")

            cols = st.columns([1, 1, 1, 2, 5])
            if (role == 'admin') or (role == 'user') or (role == 'gestor' and collection == 'demandas'):
                if cols[0].button("✏️", key=f"edit_{key}", help="Editar"): st.session_state.edit_id = {
                    'collection': collection, 'id': row['id'], 'data': row.to_dict()}; st.rerun()
            if role == 'admin' and cols[1].button("🗑️", key=f"del_{key}",
                                                  help="Excluir"): st.session_state.confirm_delete = {
                'collection': collection, 'id': row['id'], 'desc': title}; st.rerun()
            if cols[2].button("📜", key=f"hist_{key}", help="Ver Histórico"): st.session_state.view_history_id = {
                'collection': collection, 'id': row['id'], 'data': row.to_dict()}; st.rerun()

            if collection == 'demandas':
                linked_rc = all_rcs[
                    all_rcs['demanda_id'] == row['id']] if all_rcs is not None and not all_rcs.empty else pd.DataFrame()
                if not linked_rc.empty:
                    rc_id = linked_rc.iloc[0]['id']
                    linked_pedido = all_pedidos[all_pedidos[
                                                    'requisicao_id'] == rc_id] if all_pedidos is not None and not all_pedidos.empty else pd.DataFrame()
                    if not linked_pedido.empty:
                        if cols[3].button("🚚 Ver Pedido", key=f"goto_ped_{key}"):
                            st.session_state.focus_item = {'collection': 'pedidos', 'id': linked_pedido.iloc[0]['id']}
                            st.rerun()
                    else:
                        if cols[3].button("🛒 Ver RC", key=f"goto_rc_{key}"):
                            st.session_state.focus_item = {'collection': 'requisicoes', 'id': rc_id}
                            st.rerun()

            if collection == "requisicoes" and status == "Aberto" and role in ['admin', 'user']:
                if cols[3].button("📦 Gerar Pedido", key=f"gen_ped_{key}", type="primary"):
                    pedido = Pedido(requisicao_id=row['id'], solicitante=row['solicitante'], valor=row['valor'],
                                    numero_pedido=f"PED-{row.get('numero_rc', row['id'][-4:])}")
                    pedido_data = pedido.model_dump()
                    pedido_data['historico'] = [
                        f"Criado por {st.session_state.username} em {datetime.now().strftime('%d/%m/%Y %H:%M')}"]
                    if self.db.add_doc("pedidos", pedido_data): self.db.update_doc("requisicoes", row['id'],
                                                                                   {"status": "Pedido Gerado"},
                                                                                   st.session_state.username); st.toast(
                        "🚀 Pedido gerado!", icon="🚀"); time.sleep(1); st.rerun()
            if st.session_state.confirm_delete.get('id') == row['id']:
                st.warning(f"Excluir '{st.session_state.confirm_delete['desc']}'?")
                c1, c2, _ = st.columns([1, 1, 8])
                if c1.button("Sim, excluir", key=f"conf_del_{key}", type="primary"): self.db.delete_doc(collection, row[
                    'id']); st.session_state.confirm_delete = {}; st.rerun()
                if c2.button("Cancelar", key=f"canc_del_{key}"): st.session_state.confirm_delete = {}; st.rerun()

    @st.dialog("Histórico de Alterações")
    def render_history_modal(self):
        info = st.session_state.view_history_id
        st.markdown(f"**ID:** `{info['id']}`")
        for entry in reversed(info['data'].get('historico', ["Nenhum histórico."])): st.info(entry)
        if st.button("Fechar", key=f"close_hist_{info['id']}"): st.session_state.view_history_id = None; st.rerun()

    def render_edit_modal(self):
        if st.session_state.edit_id:
            edit_info = st.session_state.edit_id
            with st.form(key=f"edit_form_{edit_info['id']}"):
                st.subheader(f"Editando {edit_info['collection'][:-1].capitalize()} ID: ...{edit_info['id'][-6:]}")
                data = edit_info['data']
                new_data = {}
                valor_str = None

                if edit_info['collection'] == 'demandas':
                    new_data['descricao_necessidade'] = st.text_area("Descrição", data.get('descricao_necessidade', ''))
                    tipos = ["Material", "Serviço"]
                    current_tipo = data.get('tipo')
                    tipo_index = tipos.index(current_tipo) if current_tipo in tipos else 0
                    new_data['tipo'] = st.selectbox("Tipo", tipos, index=tipo_index)
                    categorias_fixas = ["Facilities/Eletromecânica", "Manutenção de rede", "Tratamento",
                                        "Tratamento (Laboratório)"]
                    current_categoria = data.get('categoria')
                    cat_index = categorias_fixas.index(
                        current_categoria) if current_categoria in categorias_fixas else 0
                    new_data['categoria'] = st.selectbox("Categoria", categorias_fixas, index=cat_index)
                    opts = ["Aberta", "Em Atendimento", "Fechada", "Cancelada"]
                    new_data['status_demanda'] = st.selectbox("Status", opts,
                                                              index=opts.index(data.get('status_demanda')))
                elif edit_info['collection'] == 'requisicoes':
                    new_data['numero_rc'] = st.text_input("Número da RC", data.get('numero_rc', ''))
                    current_valor_str = f"{data.get('valor', 0.0):_.2f}".replace('.', ',').replace('_', '.')
                    valor_str = st.text_input("Valor (R$)", value=current_valor_str)
                    opts = ["Aberto", "Pedido Gerado", "Cancelado"]
                    new_data['status'] = st.selectbox("Status", opts, index=opts.index(data.get('status')))
                elif edit_info['collection'] == 'pedidos':
                    new_data['numero_pedido'] = st.text_input("Número do Pedido", data.get('numero_pedido', ''))
                    current_valor_str = f"{data.get('valor', 0.0):_.2f}".replace('.', ',').replace('_', '.')
                    valor_str = st.text_input("Valor (R$)", value=current_valor_str)
                    opts = ["Em Processamento", "Em Transporte", "Entregue", "Cancelado"]
                    new_data['status'] = st.selectbox("Status", opts, index=opts.index(data.get('status')))
                    entrega_val = pd.to_datetime(data.get('data_entrega')).date() if pd.notna(
                        data.get('data_entrega')) else None
                    data_entrega_input = st.date_input("Data de Entrega", value=entrega_val)
                    new_data['data_entrega'] = datetime.combine(data_entrega_input,
                                                                datetime.min.time()) if data_entrega_input else None
                    new_data['observacao'] = st.text_area("Observação", data.get('observacao', ''))

                c1, c2 = st.columns(2)
                if c1.form_submit_button("Salvar", type="primary"):
                    try:
                        if valor_str is not None:
                            new_data['valor'] = parse_brazilian_float(valor_str)
                        with st.spinner("Salvando alterações..."):
                            if self.db.update_doc(edit_info['collection'], edit_info['id'], new_data,
                                                  st.session_state.username):
                                st.toast("💾 Atualizado!", icon="💾")
                                st.session_state.edit_id = None
                                time.sleep(1)
                                st.rerun()
                    except ValueError:
                        pass
                if c2.form_submit_button("Cancelar"):
                    st.session_state.edit_id = None
                    st.rerun()

    def _generate_backup_data(self) -> bytes:
        try:
            backup_data = {}
            for col in ["users", "demandas", "requisicoes", "pedidos"]:
                docs_df = self.db.get_docs(col)
                for col_name in docs_df.columns:
                    if docs_df[col_name].apply(lambda x: isinstance(x, bytes)).any(): docs_df[col_name] = docs_df[
                        col_name].apply(lambda x: base64.b64encode(x).decode('utf-8') if isinstance(x, bytes) else x)
                    if pd.api.types.is_datetime64_any_dtype(docs_df[col_name]): docs_df[col_name] = docs_df[
                        col_name].astype(str)
                backup_data[col] = docs_df.to_dict(orient='records')
            return json.dumps(backup_data, ensure_ascii=False, indent=4).encode('utf-8')
        except Exception as e:
            st.error(f"Erro ao gerar backup: {e}"); return b""


# -----------------------------------------------------------------------------
# 4. PONTO DE ENTRADA DA APLICAÇÃO
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        if "firebase_credentials" not in st.secrets:
            st.error("Credenciais do Firebase não encontradas! Verifique seu arquivo secrets.toml.")
            st.stop()
        db_service = FirebaseService(dict(st.secrets["firebase_credentials"]))
        auth_service = AuthService(db_service)
        app = ViewManager(auth_service, db_service)
        app.run()
    except Exception as e:
        st.error("Ocorreu um erro crítico na aplicação.")
        st.exception(e)
        logger.critical(f"Erro crítico na aplicação: {e}", exc_info=True)
