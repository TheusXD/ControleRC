import streamlit as st
import pandas as pd
import os
import time
import hashlib
from datetime import datetime, timedelta, date
import io
import openpyxl
import firebase_admin
from firebase_admin import credentials, firestore, storage
import plotly.express as px
from openpyxl.styles import Font, Alignment, Border, Side, PatternFill
from openpyxl.utils import get_column_letter
from pydantic import BaseModel, Field, constr
from typing import List, Dict, Any, Optional, Tuple
import re
import logging
import json
import tempfile
import base64
from fpdf import FPDF

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
    categoria: constr(min_length=1)
    anexo_path: Optional[str] = None
    status_demanda: str = "Aberta"
    created_at: datetime = Field(default_factory=datetime.now)
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
            # Corrige a formatação da chave privada lida dos secrets do Streamlit
            cred_dict['private_key'] = cred_dict['private_key'].replace('\\n', '\n')
            cert = credentials.Certificate(cred_dict)
            firebase_admin.initialize_app(cert)
        self.db = firestore.client()

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
            logger.info(f"Obtendo documentos da coleção: {collection} com filtros: {filters}")
            query = self.db.collection(collection)
            if filters:
                for f in filters:
                    query = query.where(filter=firestore.FieldFilter(f[0], f[1], f[2]))

            docs = query.stream()
            data = []
            for doc in docs:
                doc_data = doc.to_dict()
                doc_data['id'] = doc.id
                # Converte timestamps do Firebase para objetos datetime do Python
                for key, value in doc_data.items():
                    if isinstance(value, datetime):
                        doc_data[key] = value
                data.append(doc_data)

            logger.info(f"Foram obtidos {len(data)} documentos da coleção: {collection}")
            df = pd.DataFrame(data) if data else pd.DataFrame()
            # Ordena os dados pela data de criação para mostrar os mais recentes primeiro
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
            logger.info(f"Adicionando documento à coleção: {collection}")
            self.db.collection(collection).add(data)
            logger.info(f"Documento adicionado com sucesso à coleção: {collection}")
            return True
        except Exception as e:
            logger.error(f"Erro ao adicionar documento a '{collection}': {e}", exc_info=True)
            st.error(f"Erro ao adicionar em '{collection}': {e}")
            return False

    def update_doc(self, collection: str, doc_id: str, new_data: Dict[str, Any], username: str) -> bool:
        """Atualiza um documento e registra as alterações no histórico."""
        try:
            logger.info(f"Atualizando documento ID: {doc_id} na coleção: {collection} por {username}")
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
                # Normaliza valores para comparação (ex: None vs "")
                if (old_value or "") != (value or ""):
                    log_entry = f"'{key.replace('_', ' ').capitalize()}' alterado de '{old_value}' para '{value}' por {username} em {now_str}"
                    history_log.append(log_entry)

            new_data['historico'] = history_log
            doc_ref.update(new_data)
            logger.info(f"Documento ID: {doc_id} atualizado com sucesso.")
            return True
        except Exception as e:
            logger.error(f"Erro ao atualizar documento ID: {doc_id} em '{collection}': {e}", exc_info=True)
            st.error(f"Erro ao atualizar em '{collection}': {e}")
            return False

    def delete_doc(self, collection: str, doc_id: str) -> bool:
        """Exclui um documento."""
        try:
            logger.info(f"Excluindo documento ID: {doc_id} da coleção: {collection}")
            self.db.collection(collection).document(doc_id).delete()
            logger.info(f"Documento ID: {doc_id} excluído com sucesso da coleção: {collection}")
            return True
        except Exception as e:
            logger.error(f"Erro ao excluir documento ID: {doc_id} de '{collection}': {e}", exc_info=True)
            st.error(f"Erro ao excluir de '{collection}': {e}")
            return False

    def restore_from_backup_data(self, backup_data: Dict[str, Any]) -> bool:
        """Restaura o banco de dados a partir de um dicionário de dados."""
        try:
            # Apaga todas as coleções atuais
            collections_to_restore = ["users", "demandas", "requisicoes", "pedidos"]
            for col in collections_to_restore:
                docs_stream = self.db.collection(col).stream()
                for doc in docs_stream:
                    doc.reference.delete()

            # Restaura os dados
            for collection_name, documents in backup_data.items():
                if collection_name not in collections_to_restore:
                    continue
                for doc_data in documents:
                    # Converte strings de volta para datetime e bytes
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
        """Gera o hash de uma senha com um salt para segurança."""
        if salt is None:
            salt = os.urandom(16)
        hashed_password = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt, 100000)
        return hashed_password, salt

    def _check_password(self, stored_password, salt, provided_password: str) -> bool:
        """Verifica se a senha fornecida corresponde ao hash armazenado, lidando com tipos de dados inconsistentes."""
        if salt is None or stored_password is None:
            return False

        # CORREÇÃO: Garante que o salt seja bytes. Se for uma string, assume que é base64 e decodifica.
        if isinstance(salt, str):
            try:
                salt = base64.b64decode(salt)
            except (ValueError, TypeError):
                logger.error("O 'salt' no banco de dados está em um formato de string inválido.")
                return False

        # CORREÇÃO: Garante que a senha armazenada seja bytes.
        if isinstance(stored_password, str):
            try:
                stored_password = base64.b64decode(stored_password)
            except (ValueError, TypeError):
                logger.error("A senha armazenada no banco de dados está em um formato de string inválido.")
                return False

        # Agora que ambos são bytes, podemos comparar.
        return stored_password == hashlib.pbkdf2_hmac('sha256', provided_password.encode('utf-8'), salt, 100000)

    def _validate_password_strength(self, password: str) -> bool:
        """Verifica se a senha atende aos critérios de força (comprimento, caracteres)."""
        if len(password) < 8: return False
        if not re.search(r"[A-Z]", password): return False
        if not re.search(r"[a-z]", password): return False
        if not re.search(r"[0-9]", password): return False
        return True

    def register_user(self, username, password, is_gestor):
        """Registra um novo usuário com validação de senha e atribuição de papel."""
        logger.info(f"Tentativa de registro do usuário: {username}")
        if not self._validate_password_strength(password):
            logger.error(f"Falha na validação da força da senha para o usuário: {username}")
            st.error("A senha deve ter no mínimo 8 caracteres, com uma letra maiúscula, uma minúscula e um número.")
            return

        existing_user = self.db.get_docs("users", [("username", "==", username)])
        if not existing_user.empty:
            logger.warning(f"Falha no registro: O nome de usuário {username} já existe.")
            st.error("Este nome de usuário já existe.");
            return

        all_users = self.db.get_docs("users")
        # O primeiro usuário é admin, os outros são 'user' ou 'gestor' (pendente de aprovação)
        role = "admin" if all_users.empty else "gestor" if is_gestor else "user"
        status = "active" if role == "admin" or not is_gestor else "pending"

        hashed_pw, salt = self._hash_password(password)
        user_data = {"username": username, "password": hashed_pw, "salt": salt, "role": role, "status": status,
                     "created_at": datetime.now()}

        if self.db.add_doc("users", user_data):
            logger.info(f"Usuário {username} registrado com sucesso com o papel: {role} e estado: {status}")
            st.success(f"Usuário '{username}' registrado como '{role}'. Status: {status}")
            time.sleep(2);
            st.session_state.page = "Login";
            st.rerun()

    def login_user(self, username, password):
        """Faz o login do usuário e inicia a sessão."""
        logger.info(f"Tentativa de login do usuário: {username}")
        user_df = self.db.get_docs("users", [("username", "==", username)])
        if not user_df.empty:
            user_data = user_df.iloc[0]
            if user_data['status'] == 'pending':
                logger.warning(f"Login falhou para {username}: Conta pendente de aprovação.")
                st.warning("Sua conta está aguardando aprovação.")
            elif self._check_password(user_data['password'], user_data['salt'], password):
                st.session_state.logged_in = True
                st.session_state.username = user_data['username']
                st.session_state.role = user_data['role']
                st.session_state.last_activity = time.time()
                logger.info(f"Usuário {username} logado com sucesso com o papel: {user_data['role']}")
                st.rerun()
            else:
                logger.error(f"Login falhou para {username}: Senha incorreta.")
                st.error("Usuário ou senha incorretos.")
        else:
            logger.error(f"Login falhou: Usuário {username} não encontrado.")
            st.error("Usuário ou senha incorretos.")

    def check_session_timeout(self):
        """Verifica se a sessão expirou por inatividade e força o logout."""
        if 'last_activity' in st.session_state:
            timeout_seconds = self.SESSION_TIMEOUT_MINUTES * 60
            if time.time() - st.session_state.last_activity > timeout_seconds:
                logger.warning(
                    f"Sessão expirada por inatividade para o usuário: {st.session_state.get('username', 'desconhecido')}")
                for key in list(st.session_state.keys()):
                    del st.session_state[key]
                st.warning("Sessão expirada por inatividade. Por favor, faça login novamente.")
                time.sleep(3)
                st.rerun()
        st.session_state.last_activity = time.time()

    def change_password(self, username, old_password, new_password):
        """Permite que um usuário altere sua própria senha."""
        logger.info(f"Tentativa de alteração de senha para o usuário: {username}")
        user_df = self.db.get_docs("users", [("username", "==", username)])
        if user_df.empty:
            st.error("Usuário não encontrado.")
            return False

        user_data = user_df.iloc[0]
        if not self._check_password(user_data['password'], user_data['salt'], old_password):
            st.error("A senha antiga está incorreta.")
            return False

        if not self._validate_password_strength(new_password):
            st.error("A nova senha não é forte o suficiente.")
            return False

        hashed_pw, salt = self._hash_password(new_password)
        update_data = {"password": hashed_pw, "salt": salt}
        if self.db.update_doc("users", user_data['id'], update_data, username):
            logger.info(f"Senha alterada com sucesso para o usuário: {username}")
            st.success("Senha alterada com sucesso!")
            return True
        return False

    def reset_password_by_admin(self, user_id, new_password):
        """Permite que um administrador redefina a senha de um usuário."""
        logger.info(f"Admin redefinindo a senha para o usuário ID: {user_id}")
        if not self._validate_password_strength(new_password):
            st.error("A nova senha não é forte o suficiente.")
            return False

        hashed_pw, salt = self._hash_password(new_password)
        update_data = {"password": hashed_pw, "salt": salt}
        if self.db.update_doc("users", user_id, update_data, st.session_state.username):
            logger.info(f"Senha redefinida com sucesso para o usuário ID: {user_id}")
            return True
        return False


# -----------------------------------------------------------------------------
# 3. UI / VIEWS (LÓGICA DE APRESENTAÇÃO)
# -----------------------------------------------------------------------------

class PDF(FPDF):
    def header(self):
        self.set_font('Arial', 'B', 12)
        self.cell(0, 10, 'Relatório de Compras', 0, 1, 'C')
        self.ln(5)

    def footer(self):
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        self.cell(0, 10, f'Página {self.page_no()}', 0, 0, 'C')
        self.cell(0, 10, f'Emitido em: {datetime.now().strftime("%d/%m/%Y %H:%M")}', 0, 0, 'R')


def generate_summary_pdf(df: pd.DataFrame, title: str, filters: Dict[str, str]) -> bytes:
    """Gera um PDF de resumo com uma tabela de dados filtrados."""
    pdf = PDF()
    pdf.add_page(orientation='L')  # Paisagem para mais espaço

    # Título
    pdf.set_font('Arial', 'B', 16)
    pdf.cell(0, 10, f"Relatório de {title}", 0, 1, 'C')
    pdf.ln(5)

    # Filtros aplicados
    pdf.set_font('Arial', 'I', 9)
    for key, value in filters.items():
        pdf.cell(0, 5, f"{key}: {value}", 0, 1, 'L')
    pdf.ln(10)

    # Cabeçalho da Tabela
    pdf.set_font('Arial', 'B', 8)
    pdf.set_fill_color(220, 220, 220)

    # Definir colunas relevantes e larguras
    if title == "Demandas":
        cols = ['created_at', 'solicitante_demanda', 'categoria', 'descricao_necessidade', 'status_demanda']
        widths = [25, 30, 40, 125, 30]
    elif title == "Requisições":
        cols = ['created_at', 'solicitante', 'numero_rc', 'valor', 'status']
        widths = [30, 40, 40, 30, 30]
    else:  # Pedidos
        cols = ['created_at', 'solicitante', 'numero_pedido', 'valor', 'status', 'data_entrega', 'observacao']
        widths = [25, 30, 30, 25, 25, 25, 90]

    df_report = df[cols]

    for i, header in enumerate(df_report.columns):
        pdf.cell(widths[i], 7, str(header).replace('_', ' ').title(), 1, 0, 'C', 1)
    pdf.ln()

    # Dados da Tabela
    pdf.set_font('Arial', '', 8)
    for _, row in df_report.iterrows():
        max_height = 6
        for i, col in enumerate(df_report.columns):
            text = str(row[col]) if pd.notna(row[col]) else ""
            if isinstance(row[col], datetime) or isinstance(row[col], pd.Timestamp):
                text = pd.to_datetime(row[col]).strftime('%d/%m/%Y')
            elif col == 'valor':
                text = f"R$ {row[col]:,.2f}"

            # Codifica para evitar erros com caracteres especiais
            encoded_text = text.encode('latin-1', 'replace').decode('latin-1')
            pdf.cell(widths[i], max_height, encoded_text, 1, 0, 'L')
        pdf.ln()

    return pdf.output(dest='S')


def to_excel(df: pd.DataFrame, title: str = "Relatório") -> bytes:
    """Converte um DataFrame para um arquivo Excel formatado em memória."""
    output = io.BytesIO()
    df_copy = df.copy()

    # Remove informações de fuso horário para compatibilidade com o openpyxl
    for col in df_copy.select_dtypes(include=['datetimetz']).columns:
        df_copy[col] = df_copy[col].dt.tz_localize(None)

    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_copy.to_excel(writer, index=False, sheet_name=title)
        workbook = writer.book
        worksheet = writer.sheets[title]

        # Estilos para o cabeçalho e células
        header_fill = PatternFill(start_color="4F81BD", end_color="4F81BD", fill_type="solid")
        header_font = Font(color="FFFFFF", bold=True)
        border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'),
                        bottom=Side(style='thin'))
        alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)

        # Formatar cabeçalho
        for col_num, col_name in enumerate(df_copy.columns, 1):
            cell = worksheet.cell(row=1, column=col_num)
            cell.fill = header_fill
            cell.font = header_font
            cell.border = border
            cell.alignment = alignment
            # Ajustar largura das colunas
            column_letter = get_column_letter(col_num)
            max_len = max(df_copy[col_name].astype(str).map(len).max(), len(col_name)) + 2
            worksheet.column_dimensions[column_letter].width = min(max_len, 50)

        # Formatar células de dados
        for row in range(2, len(df_copy) + 2):
            for col in range(1, len(df_copy.columns) + 1):
                cell = worksheet.cell(row=row, column=col)
                cell.border = border
                cell.alignment = Alignment(horizontal='left', vertical='center')
                # Formatar colunas de valor como moeda
                if 'valor' in df_copy.columns[col - 1].lower():
                    cell.number_format = 'R$ #,##0.00'

        # Formatação condicional para status
        status_col_name = next((col for col in ['status', 'status_demanda'] if col in df_copy.columns), None)
        if status_col_name:
            green_fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
            red_fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
            yellow_fill = PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid")
            blue_fill = PatternFill(start_color="DDEBF7", end_color="DDEBF7", fill_type="solid")

            status_col_index = df_copy.columns.get_loc(status_col_name) + 1
            for row in range(2, len(df_copy) + 2):
                cell = worksheet.cell(row=row, column=status_col_index)
                if cell.value in ['Finalizado', 'Entregue', 'Fechada']:
                    cell.fill = green_fill
                elif cell.value in ['Cancelado', 'Rejeitado']:
                    cell.fill = red_fill
                elif cell.value in ['Em Processamento', 'Em Atendimento', 'Em Transporte', 'Pedido Gerado']:
                    cell.fill = yellow_fill
                elif cell.value in ['Aberto', 'Aberta']:
                    cell.fill = blue_fill

        worksheet.freeze_panes = 'A2'
        worksheet.auto_filter.ref = worksheet.dimensions
    return output.getvalue()


class ViewManager:
    """Classe para gerenciar a renderização da UI."""

    def __init__(self, auth_service: AuthService, db_service: FirebaseService):
        self.auth = auth_service
        self.db = db_service
        self._init_session_state()

    def _init_session_state(self):
        """Inicializa as variáveis de estado da sessão necessárias."""
        defaults = {
            'logged_in': False, 'username': "", 'role': "", 'page': "Login",
            'confirm_delete': {}, 'edit_id': None, 'confirm_delete_user': {},
            'reset_password_for_user': {}, 'create_rc_from_demanda_id': None,
            'view_history_id': None, 'confirm_restore': None
        }
        for key, value in defaults.items():
            if key not in st.session_state:
                st.session_state[key] = value

    def run(self):
        """Executa o fluxo principal da aplicação."""
        if not st.session_state.logged_in:
            self.render_login_page()
        else:
            self.auth.check_session_timeout()
            self.render_main_app()

    def render_login_page(self):
        """Renderiza a página de login ou registro."""
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.markdown(
                """
                <div style="text-align: center; margin-bottom: 2rem;">
                    <span style="font-family: sans-serif; font-size: 4rem; font-weight: 900; color: var(--text-color);">ATIBAIA</span><span style="font-family: sans-serif; font-size: 4rem; font-weight: 900; color: #00AEEF;">💧</span>
                    <div style="font-family: sans-serif; font-size: 2.5rem; color: #00AEEF; letter-spacing: 0.1rem; margin-top: -1rem;">SANEAMENTO</div>
                </div>
                """,
                unsafe_allow_html=True
            )
            if st.session_state.page == "Login":
                self._render_login_form()
                if st.button("Não tem conta? Registre-se"):
                    st.session_state.page = "Registro";
                    st.rerun()
            else:
                self._render_registration_form()
                if st.button("Já tem conta? Faça login"):
                    st.session_state.page = "Login";
                    st.rerun()

    def _render_login_form(self):
        st.title("🔐 Login do Sistema")
        with st.form("login_form"):
            username = st.text_input("Nome de Usuário")
            password = st.text_input("Senha", type="password")
            if st.form_submit_button("Entrar", type="primary"):
                self.auth.login_user(username, password)

    def _render_registration_form(self):
        st.title("📝 Registro de Novo Usuário")
        with st.form("registration_form"):
            username = st.text_input("Nome de Usuário")
            password = st.text_input("Senha", type="password")
            is_gestor = st.checkbox("Sou um gestor (requer aprovação do admin)")
            if st.form_submit_button("Registrar", type="primary"):
                self.auth.register_user(username, password, is_gestor)

    def render_main_app(self):
        """Renderiza a aplicação principal após o login."""
        self.render_sidebar()
        st.title("🚀 Sistema de Controle de Compras")

        # Modais são renderizados primeiro para lidar com o estado
        self.render_edit_modal()
        if st.session_state.view_history_id:
            self.render_history_modal()

        # Navegação principal usando st.tabs
        tab_dashboard, tab_demandas, tab_rcs, tab_pedidos = st.tabs([
            "📊 Dashboard", "📝 Demandas", "🛒 Requisições", "🚚 Pedidos"
        ])

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
                    old_password = st.text_input("Senha Antiga", type="password")
                    new_password = st.text_input("Nova Senha", type="password")
                    confirm_password = st.text_input("Confirmar Nova Senha", type="password")
                    if st.form_submit_button("Alterar Senha", type="primary"):
                        if new_password != confirm_password:
                            st.error("As novas senhas não coincidem.")
                        else:
                            with st.spinner("Alterando senha..."):
                                if self.auth.change_password(st.session_state.username, old_password, new_password):
                                    time.sleep(2)
                                    st.rerun()

            if st.button("Logout", use_container_width=True):
                for key in list(st.session_state.keys()): del st.session_state[key]
                st.rerun()
            st.divider()
            if st.session_state.role == 'admin':
                self.render_admin_panel()

    def render_admin_panel(self):
        st.header("⚙️ Administração")
        with st.expander("Gerenciar Usuários", expanded=True):
            # Seção para usuários pendentes
            pending_users = self.db.get_docs("users", [("status", "==", "pending")])
            if not pending_users.empty:
                st.subheader("Aprovações Pendentes")
                st.warning(f"🔔 **{len(pending_users)} aprovações pendentes!**")
                for _, user in pending_users.iterrows():
                    c1, c2, c3 = st.columns([2, 1, 1])
                    c1.write(f"{user['username']} ({user['role']})")
                    if c2.button("✅", key=f"a_{user['id']}", help="Aprovar"):
                        with st.spinner("Processando..."):
                            self.db.update_doc("users", user['id'], {"status": "active"}, st.session_state.username);
                            st.rerun()
                    if c3.button("🗑️", key=f"r_{user['id']}", help="Rejeitar"):
                        with st.spinner("Processando..."):
                            self.db.delete_doc("users", user['id']);
                            st.rerun()
                st.divider()

            # Seção para usuários ativos
            st.subheader("Usuários Ativos")
            active_users = self.db.get_docs("users", [("status", "==", "active")])
            if not active_users.empty:
                for _, user in active_users.iterrows():
                    is_current_user = user['username'] == st.session_state.username
                    c1, c2, c3 = st.columns([3, 1, 1])
                    c1.write(f"**{user['username']}** ({user['role']}){' (Você)' if is_current_user else ''}")

                    if c2.button("🔑", key=f"reset_pw_{user['id']}", help="Redefinir Senha", disabled=is_current_user):
                        st.session_state.reset_password_for_user = {'id': user['id'], 'username': user['username']}
                        st.rerun()

                    if c3.button("🗑️", key=f"del_user_{user['id']}", help="Excluir Usuário", disabled=is_current_user):
                        st.session_state.confirm_delete_user = {'id': user['id'], 'username': user['username']}
                        st.rerun()

                    # Formulário para redefinir senha
                    if st.session_state.get('reset_password_for_user', {}).get('id') == user['id']:
                        with st.form(key=f"reset_form_{user['id']}", clear_on_submit=True):
                            st.warning(
                                f"Redefinindo a senha para **{st.session_state.reset_password_for_user['username']}**.")
                            new_pass = st.text_input("Nova Senha", type="password")
                            if st.form_submit_button("Confirmar Redefinição", type="primary"):
                                with st.spinner("Redefinindo senha..."):
                                    if self.auth.reset_password_by_admin(user['id'], new_pass):
                                        st.toast(f"Senha para {user['username']} redefinida.", icon="🔑")
                                        del st.session_state.reset_password_for_user
                                        time.sleep(1);
                                        st.rerun()
                            if st.form_submit_button("Cancelar"):
                                del st.session_state.reset_password_for_user
                                st.rerun()

                    # Modal de confirmação para excluir usuário
                    if st.session_state.get('confirm_delete_user', {}).get('id') == user['id']:
                        st.error(
                            f"Tem certeza que quer excluir o usuário **{st.session_state.confirm_delete_user['username']}**?")
                        confirm_c1, confirm_c2, _ = st.columns([1, 1, 3])
                        if confirm_c1.button("Sim, excluir", key=f"confirm_del_user_{user['id']}", type="primary"):
                            with st.spinner("Excluindo usuário..."):
                                self.db.delete_doc("users", user['id'])
                                del st.session_state.confirm_delete_user
                                st.toast(f"Usuário {user['username']} excluído.", icon="🗑️")
                                time.sleep(1);
                                st.rerun()
                        if confirm_c2.button("Cancelar", key=f"cancel_del_user_{user['id']}"):
                            del st.session_state.confirm_delete_user
                            st.rerun()
            else:
                st.info("Nenhum usuário ativo encontrado.")

        st.divider()
        st.subheader("Backup e Restauro Local")

        # Seção de Backup Local
        backup_data_bytes = self._generate_backup_data()
        st.download_button(
            label="📥 Criar e Baixar Backup Local",
            data=backup_data_bytes,
            file_name=f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json",
            use_container_width=True,
            type="primary"
        )

        # Seção de Restauro Local
        st.subheader("Restaurar a partir de Arquivo")
        uploaded_file = st.file_uploader("Carregue um arquivo de backup (.json)", type="json")
        if uploaded_file is not None:
            if st.button("Restaurar a partir deste Arquivo"):
                st.session_state.confirm_restore = uploaded_file
                st.rerun()

        if st.session_state.get('confirm_restore'):
            st.error(
                f"Tem certeza que quer restaurar o backup '{st.session_state.confirm_restore.name}'? Todos os dados atuais serão apagados.")
            rc1, rc2, _ = st.columns([1, 1, 3])
            if rc1.button("Sim, restaurar", key=f"confirm_restore_local", type="primary"):
                with st.spinner("Restaurando backup... Isso pode demorar..."):
                    backup_data = json.load(st.session_state.confirm_restore)
                    if self.db.restore_from_backup_data(backup_data):
                        st.success("Backup restaurado com sucesso!")
                        del st.session_state.confirm_restore
                        time.sleep(2);
                        st.rerun()
            if rc2.button("Cancelar", key=f"cancel_restore_local"):
                del st.session_state.confirm_restore
                st.rerun()

    def _render_paginated_rows(self, df: pd.DataFrame, render_function, key_suffix: str):
        """Renderiza linhas de um DataFrame com paginação."""
        if df.empty:
            st.info("Nenhum dado encontrado.")
            return

        items_per_page = st.selectbox("Itens por página", [5, 10, 20], key=f"items_{key_suffix}", index=1)
        total_pages = max(1, (len(df) - 1) // items_per_page + 1)
        page_key = f"page_{key_suffix}"
        if page_key not in st.session_state: st.session_state[page_key] = 1
        st.session_state[page_key] = min(st.session_state[page_key], total_pages)

        c1, c2, c3 = st.columns([1, 2, 1])
        if c1.button("⬅️", key=f"prev_{key_suffix}", help="Página Anterior",
                     disabled=(st.session_state[page_key] <= 1)):
            st.session_state[page_key] -= 1;
            st.rerun()
        if c3.button("➡️", key=f"next_{key_suffix}", help="Próxima Página",
                     disabled=(st.session_state[page_key] >= total_pages)):
            st.session_state[page_key] += 1;
            st.rerun()
        c2.write(f"Página **{st.session_state[page_key]}** de **{total_pages}**")

        start_idx = (st.session_state[page_key] - 1) * items_per_page
        for _, row in df.iloc[start_idx: start_idx + items_per_page].iterrows():
            render_function(row)

    def render_dashboard(self):
        st.header("Dashboard de Métricas")
        df_demandas = self.db.get_docs("demandas")
        df_rc = self.db.get_docs("requisicoes")
        df_pedidos = self.db.get_docs("pedidos")

        total_valor_rc = df_rc['valor'].sum() if not df_rc.empty else 0

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total de Demandas", f"{len(df_demandas)} 📝")
        c2.metric("Total de RCs", f"{len(df_rc)} 🛒")
        c3.metric("Total de Pedidos", f"{len(df_pedidos)} 🚚")
        c4.metric("Valor Total em RCs", f"R$ {total_valor_rc:,.2f}")
        st.divider()

        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Status das Demandas")
            if not df_demandas.empty:
                status_counts = df_demandas['status_demanda'].value_counts().reset_index()
                fig = px.bar(status_counts, x='status_demanda', y='count', title="Distribuição de Status",
                             text_auto=True, color='status_demanda',
                             labels={'status_demanda': 'Status', 'count': 'Quantidade'})
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Nenhuma demanda para exibir.")
        with c2:
            st.subheader("Demandas por Categoria")
            if not df_demandas.empty:
                cat_counts = df_demandas['categoria'].value_counts().reset_index()
                fig = px.pie(cat_counts, names='categoria', values='count', title="Distribuição por Categoria", hole=.3,
                             labels={'categoria': 'Categoria', 'count': 'Quantidade'})
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Nenhuma categoria para exibir.")

    def render_demandas(self):
        st.header("Demandas de Compras")

        # Restrição de Acesso: Apenas admin, user e gestor podem criar/editar demandas
        if st.session_state.role in ['admin', 'user', 'gestor']:
            with st.expander("➕ Adicionar Nova Demanda"):
                with st.form("demanda_form", clear_on_submit=True):
                    descricao = st.text_area("Descrição da Necessidade")
                    categoria = st.text_input("Categoria")
                    uploaded_file = st.file_uploader("Anexo (Opcional)")
                    if st.form_submit_button("Registrar Demanda", type="primary"):
                        with st.spinner("Registrando demanda..."):
                            try:
                                demanda = Demanda(solicitante_demanda=st.session_state.username,
                                                  descricao_necessidade=descricao, categoria=categoria)
                                anexo_url = self.db.upload_file(uploaded_file,
                                                                uploaded_file.name) if uploaded_file else None
                                demanda_data = demanda.dict()
                                demanda_data['historico'] = [
                                    f"Criado por {st.session_state.username} em {datetime.now().strftime('%d/%m/%Y %H:%M')}"]
                                demanda_data['anexo_path'] = anexo_url
                                if self.db.add_doc("demandas", demanda_data):
                                    st.toast("✅ Demanda registrada!", icon="✅");
                                    time.sleep(1);
                                    st.rerun()
                            except Exception as e:
                                st.error(f"Erro de validação: {e}")

        st.header("Demandas Registradas")
        df_demandas = self.db.get_docs("demandas")
        self._render_paginated_rows(df_demandas, lambda row: self.render_data_row("demandas", row), "demandas")

    def render_requisicoes(self):
        st.header("Requisições de Compra (RCs)")

        # Restrição de Acesso: Apenas admin e user podem criar RCs
        if st.session_state.role in ['admin', 'user']:
            with st.expander("➕ Adicionar Nova Requisição"):
                st.subheader("Passo 1: Selecione a Demanda")
                df_demandas_abertas = self.db.get_docs("demandas", [("status_demanda", "==", "Aberta")])
                demanda_options = {"Selecione uma Demanda": None}
                if not df_demandas_abertas.empty:
                    for _, row in df_demandas_abertas.iterrows():
                        demanda_options[f"ID: ...{row['id'][-6:]} - {row['descricao_necessidade'][:40]}..."] = row['id']

                selected_demanda_display = st.selectbox(
                    "Vincular à Demanda (apenas abertas)",
                    list(demanda_options.keys()),
                    label_visibility="collapsed"
                )
                selected_demanda_id = demanda_options.get(selected_demanda_display)

                if selected_demanda_id:
                    selected_demanda_details = \
                    df_demandas_abertas[df_demandas_abertas['id'] == selected_demanda_id].iloc[0]
                    with st.container(border=True):
                        st.markdown(f"**Descrição Completa:** {selected_demanda_details['descricao_necessidade']}")
                        st.markdown(
                            f"**Categoria:** {selected_demanda_details['categoria']} | **Solicitante:** {selected_demanda_details['solicitante_demanda']}")

                    st.subheader("Passo 2: Detalhes da Requisição")
                    with st.form("requisicao_form_details", clear_on_submit=True):
                        valor = st.number_input("Valor da Requisição (R$)", min_value=0.01, format="%.2f")
                        numero_rc = st.text_input("Número da RC (opcional)")

                        if st.form_submit_button("Registrar Requisição", type="primary"):
                            with st.spinner("Registrando requisição..."):
                                try:
                                    requisicao = Requisicao(solicitante=st.session_state.username,
                                                            demanda_id=selected_demanda_id, valor=valor,
                                                            numero_rc=numero_rc or None)
                                    req_data = requisicao.dict()
                                    req_data['historico'] = [
                                        f"Criado por {st.session_state.username} em {datetime.now().strftime('%d/%m/%Y %H:%M')}"]
                                    if self.db.add_doc("requisicoes", req_data):
                                        self.db.update_doc("demandas", selected_demanda_id,
                                                           {"status_demanda": "Em Atendimento"},
                                                           st.session_state.username)
                                        st.toast("✅ Requisição registrada!", icon="✅");
                                        time.sleep(1);
                                        st.rerun()
                                except Exception as e:
                                    st.error(f"Erro ao registrar: {e}")
                else:
                    st.info("Selecione uma demanda da lista acima para continuar.")

        st.header("Requisições Registradas")
        df_rc = self.db.get_docs("requisicoes")
        if not df_rc.empty:
            st.download_button("📥 Exportar para Excel", to_excel(df_rc, "Relatório de RCs"), 'relatorio_rcs.xlsx')
        self._render_paginated_rows(df_rc, lambda row: self.render_data_row("requisicoes", row), "rcs")

    def render_pedidos(self):
        st.header("Pedidos de Compra")
        all_pedidos = self.db.get_docs("pedidos")

        tab_andamento, tab_entregues, tab_cancelados = st.tabs(["⏳ Em Andamento", "✅ Entregues", "❌ Cancelados"])

        with tab_andamento:
            df_filtered = all_pedidos[all_pedidos['status'].isin(
                ['Em Processamento', 'Em Transporte'])] if not all_pedidos.empty else pd.DataFrame()
            if not df_filtered.empty:
                st.download_button("📥 Exportar", to_excel(df_filtered, "Pedidos em Andamento"),
                                   'pedidos_andamento.xlsx', key='btn_andamento')
            self._render_paginated_rows(df_filtered, lambda row: self.render_data_row("pedidos", row),
                                        "pedidos_andamento")

        with tab_entregues:
            df_filtered = all_pedidos[all_pedidos['status'] == 'Entregue'] if not all_pedidos.empty else pd.DataFrame()
            if not df_filtered.empty:
                st.download_button("📥 Exportar", to_excel(df_filtered, "Pedidos Entregues"), 'pedidos_entregues.xlsx',
                                   key='btn_entregues')
            self._render_paginated_rows(df_filtered, lambda row: self.render_data_row("pedidos", row),
                                        "pedidos_entregues")

        with tab_cancelados:
            df_filtered = all_pedidos[all_pedidos['status'] == 'Cancelado'] if not all_pedidos.empty else pd.DataFrame()
            if not df_filtered.empty:
                st.download_button("📥 Exportar", to_excel(df_filtered, "Pedidos Cancelados"), 'pedidos_cancelados.xlsx',
                                   key='btn_cancelados')
            self._render_paginated_rows(df_filtered, lambda row: self.render_data_row("pedidos", row),
                                        "pedidos_cancelados")

    def render_data_row(self, collection: str, row: pd.Series):
        """Renderiza uma linha de dados com botões de ação."""
        key = f"{collection}_{row['id']}"
        role = st.session_state.role

        with st.container(border=True):
            # Define o título e status com base na coleção
            if collection == 'demandas':
                title = f"Demanda: {row.get('descricao_necessidade', row['id'])} (Cat: {row.get('categoria', 'N/A')})"
                status = row.get('status_demanda', 'N/A')
            elif collection == 'requisicoes':
                title = f"RC: {row.get('numero_rc', 'S/N')} | Valor: R$ {row.get('valor', 0):,.2f}"
                status = row.get('status', 'N/A')
            else:  # Pedidos
                title = f"Pedido: {row.get('numero_pedido', 'S/N')} | Valor: R$ {row.get('valor', 0):,.2f}"
                status = row.get('status', 'N/A')

            st.markdown(f"**{title}**")
            st.markdown(
                f"**Status:** `{status}` | **Criado por:** `{row.get('solicitante', row.get('solicitante_demanda', 'N/A'))}` em `{row.get('created_at').strftime('%d/%m/%Y')}`")

            # Exibe campos adicionais para Pedidos
            if collection == 'pedidos':
                if pd.notna(row.get('data_entrega')):
                    st.markdown(
                        f"**Data de Entrega:** `{pd.to_datetime(row.get('data_entrega')).strftime('%d/%m/%Y')}`")
                if row.get('observacao'):
                    st.markdown(f"**Observação:** *{row.get('observacao')}*")

            # Botões de ação com base na role
            cols = st.columns([1, 1, 1, 2, 5])

            # Botão Editar
            can_edit = (role == 'admin') or \
                       (role == 'user') or \
                       (role == 'gestor' and collection == 'demandas')
            if can_edit:
                if cols[0].button("✏️", key=f"edit_{key}", help="Editar"):
                    st.session_state.edit_id = {'collection': collection, 'id': row['id'], 'data': row.to_dict()};
                    st.rerun()

            # Botão Excluir (Apenas Admin)
            if role == 'admin':
                if cols[1].button("🗑️", key=f"del_{key}", help="Excluir"):
                    st.session_state.confirm_delete = {'collection': collection, 'id': row['id'], 'desc': title};
                    st.rerun()

            # Botão Histórico (Todos)
            if cols[2].button("📜", key=f"hist_{key}", help="Ver Histórico"):
                st.session_state.view_history_id = {'collection': collection, 'id': row['id'], 'data': row.to_dict()};
                st.rerun()

            # Botão Gerar Pedido (Apenas Admin e User)
            if collection == "requisicoes" and status == "Aberto" and role in ['admin', 'user']:
                if cols[3].button("📦 Gerar Pedido", key=f"gen_ped_{key}", type="primary",
                                  help="Gerar Pedido de Compra"):
                    with st.spinner("Gerando pedido..."):
                        try:
                            pedido = Pedido(requisicao_id=row['id'], solicitante=row['solicitante'], valor=row['valor'],
                                            numero_pedido=f"PED-{row.get('numero_rc', row['id'][-4:])}")
                            pedido_data = pedido.dict()
                            pedido_data['historico'] = [
                                f"Criado por {st.session_state.username} em {datetime.now().strftime('%d/%m/%Y %H:%M')}"]
                            if self.db.add_doc("pedidos", pedido_data):
                                self.db.update_doc("requisicoes", row['id'], {"status": "Pedido Gerado"},
                                                   st.session_state.username)
                                st.toast("🚀 Pedido gerado!", icon="🚀");
                                time.sleep(1);
                                st.rerun()
                        except Exception as e:
                            st.error(f"Erro ao gerar pedido: {e}")

            # Confirmação de exclusão
            if st.session_state.confirm_delete.get('id') == row['id']:
                st.warning(f"Tem certeza que quer excluir '{st.session_state.confirm_delete['desc']}'?")
                c1, c2, _ = st.columns([1, 1, 8])
                if c1.button("Sim, excluir", key=f"conf_del_{key}", type="primary"):
                    with st.spinner("Excluindo..."):
                        self.db.delete_doc(collection, row['id']);
                        st.session_state.confirm_delete = {};
                        st.rerun()
                if c2.button("Cancelar", key=f"canc_del_{key}"):
                    st.session_state.confirm_delete = {};
                    st.rerun()

    @st.dialog("Histórico de Alterações")
    def render_history_modal(self):
        """Renderiza o histórico de um item num dialog (modal)."""
        if not st.session_state.view_history_id:
            return
        info = st.session_state.view_history_id
        st.markdown(f"**ID do Documento:** `{info['id']}`")
        historico = info['data'].get('historico', ["Nenhum histórico encontrado."])

        for entry in reversed(historico):  # Mostra o mais recente primeiro
            st.info(entry)

        if st.button("Fechar", key=f"close_hist_{info['id']}"):
            st.session_state.view_history_id = None
            st.rerun()

    def render_edit_modal(self):
        """Renderiza o formulário de edição num modal se um item for selecionado."""
        if st.session_state.edit_id:
            edit_info = st.session_state.edit_id
            with st.form(key=f"edit_form_{edit_info['id']}"):
                st.subheader(f"Editando {edit_info['collection'][:-1].capitalize()} ID: ...{edit_info['id'][-6:]}")
                data, new_data = edit_info['data'], {}

                if edit_info['collection'] == 'demandas':
                    new_data['descricao_necessidade'] = st.text_area("Descrição", data.get('descricao_necessidade', ''))
                    new_data['categoria'] = st.text_input("Categoria", data.get('categoria', ''))
                    opts = ["Aberta", "Em Atendimento", "Fechada", "Cancelada"]
                    new_data['status_demanda'] = st.selectbox("Status", opts,
                                                              index=opts.index(data.get('status_demanda')))
                elif edit_info['collection'] == 'requisicoes':
                    new_data['numero_rc'] = st.text_input("Número da RC", data.get('numero_rc', ''))
                    new_data['valor'] = st.number_input("Valor (R$)", min_value=0.01, value=data.get('valor'),
                                                        format="%.2f")
                    opts = ["Aberto", "Pedido Gerado", "Cancelado"]
                    new_data['status'] = st.selectbox("Status", opts, index=opts.index(data.get('status')))
                elif edit_info['collection'] == 'pedidos':
                    new_data['numero_pedido'] = st.text_input("Número do Pedido", data.get('numero_pedido', ''))
                    opts = ["Em Processamento", "Em Transporte", "Entregue", "Cancelado"]
                    new_data['status'] = st.selectbox("Status", opts, index=opts.index(data.get('status')))

                    entrega_val = data.get('data_entrega')
                    if pd.notna(entrega_val):
                        entrega_val = pd.to_datetime(entrega_val).date()
                    else:
                        entrega_val = None

                    data_entrega_input = st.date_input("Data de Entrega", value=entrega_val)
                    if data_entrega_input:
                        new_data['data_entrega'] = datetime.combine(data_entrega_input, datetime.min.time())
                    else:
                        new_data['data_entrega'] = None

                    new_data['observacao'] = st.text_area("Observação", data.get('observacao', ''))

                c1, c2 = st.columns(2)
                if c1.form_submit_button("Salvar Alterações", type="primary"):
                    with st.spinner("Salvando alterações..."):
                        if self.db.update_doc(edit_info['collection'], edit_info['id'], new_data,
                                              st.session_state.username):
                            st.toast("💾 Atualizado!", icon="💾");
                            st.session_state.edit_id = None;
                            time.sleep(1);
                            st.rerun()
                if c2.form_submit_button("Cancelar"):
                    st.session_state.edit_id = None;
                    st.rerun()

    def _generate_backup_data(self) -> bytes:
        """Gera os dados de backup como bytes para download."""
        try:
            logger.info("Iniciando processo de geração de backup local.")
            collections_to_backup = ["users", "demandas", "requisicoes", "pedidos"]
            backup_data = {}
            for col in collections_to_backup:
                docs_df = self.db.get_docs(col)
                # Converte tipos de dados não serializáveis para string
                for col_name in docs_df.columns:
                    if docs_df[col_name].apply(lambda x: isinstance(x, bytes)).any():
                        docs_df[col_name] = docs_df[col_name].apply(
                            lambda x: base64.b64encode(x).decode('utf-8') if isinstance(x, bytes) else x)
                    if pd.api.types.is_datetime64_any_dtype(docs_df[col_name]):
                        docs_df[col_name] = docs_df[col_name].astype(str)

                backup_data[col] = docs_df.to_dict(orient='records')

            return json.dumps(backup_data, ensure_ascii=False, indent=4).encode('utf-8')
        except Exception as e:
            logger.error(f"Falha ao gerar dados de backup: {e}", exc_info=True)
            st.error(f"Erro ao gerar dados de backup: {e}")
            return b""


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
