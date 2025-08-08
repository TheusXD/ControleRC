import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime, date
import os
import time
import hashlib
from PIL import Image
import logging
import shutil
import io
from openpyxl.styles import Font, Alignment, Border, Side, PatternFill
from openpyxl.utils import get_column_letter

# --- CONFIGURAÇÕES DA PÁGINA E ESTADO DA SESSÃO ---
st.set_page_config(page_title="Controle de Compras", layout="wide")

# --- CONFIGURAÇÃO DE LOGGING ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filename='app_log.log',
                    filemode='a')

# --- FUNÇÕES DE BANCO DE DADOS E AUTENTICAÇÃO ---
DB_NAME = "controle_rcs.db"
UPLOAD_DIR = "uploads"


def get_db_connection():
    """Cria e retorna uma conexão com o banco de dados."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn


def setup_database():
    """Cria as tabelas no banco de dados e adiciona colunas faltantes se necessário."""
    if not os.path.exists(UPLOAD_DIR):
        os.makedirs(UPLOAD_DIR)

    conn = get_db_connection()
    cursor = conn.cursor()
    # Tabela de Demandas
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS demandas (
            id INTEGER PRIMARY KEY,
            data_demanda TEXT NOT NULL,
            solicitante_demanda TEXT NOT NULL,
            descricao_necessidade TEXT NOT NULL,
            anexo_path TEXT,
            status_demanda TEXT NOT NULL DEFAULT 'Aberta'
        )
    """)

    # Tabela de Requisições
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS requisicoes (
            id INTEGER PRIMARY KEY,
            demanda_id INTEGER,
            numero_rc TEXT UNIQUE NOT NULL,
            data_criacao TEXT NOT NULL,
            solicitante TEXT NOT NULL,
            centro_custo TEXT NOT NULL,
            tipo TEXT NOT NULL,
            fornecedor TEXT,
            descricao TEXT NOT NULL,
            valor REAL NOT NULL,
            status TEXT NOT NULL,
            observacoes TEXT,
            FOREIGN KEY(demanda_id) REFERENCES demandas(id)
        )
    """)
    # Tabela de Pedidos
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pedidos (
            id INTEGER PRIMARY KEY,
            rc_id INTEGER NOT NULL,
            data_pedido TEXT NOT NULL,
            numero_pedido TEXT,
            previsao_entrega TEXT,
            status_pedido TEXT NOT NULL,
            observacoes_pedido TEXT,
            FOREIGN KEY (rc_id) REFERENCES requisicoes (id) ON DELETE CASCADE
        )
    """)
    # Tabela de Usuários
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            password BLOB NOT NULL,
            salt BLOB NOT NULL,
            role TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending'
        )
    """)

    # --- VERIFICAÇÃO E ADIÇÃO DE COLUNAS FALTANTES (MIGRAÇÃO SEGURA) ---
    cursor.execute("PRAGMA table_info(users)")
    user_columns = [info[1] for info in cursor.fetchall()]
    if 'status' not in user_columns:
        cursor.execute("ALTER TABLE users ADD COLUMN status TEXT NOT NULL DEFAULT 'active'")
    if 'salt' not in user_columns:
        cursor.execute("ALTER TABLE users ADD COLUMN salt BLOB")

    cursor.execute("PRAGMA table_info(requisicoes)")
    columns_req = [info[1] for info in cursor.fetchall()]
    if 'demanda_id' not in columns_req:
        cursor.execute("ALTER TABLE requisicoes ADD COLUMN demanda_id INTEGER")

    cursor.execute("PRAGMA table_info(demandas)")
    columns_dem = [info[1] for info in cursor.fetchall()]
    if 'status_demanda' not in columns_dem:
        cursor.execute("ALTER TABLE demandas ADD COLUMN status_demanda TEXT NOT NULL DEFAULT 'Aberta'")

    conn.commit()
    conn.close()


def hash_password(password, salt=None):
    """Gera um hash seguro para a senha usando PBKDF2 com salt."""
    if salt is None:
        salt = os.urandom(16)
    hashed_password = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt, 100000)
    return hashed_password, salt


def check_password(stored_password, salt, provided_password):
    """Verifica se a senha fornecida corresponde ao hash armazenado."""
    if salt is None:
        return False
    return stored_password == hashlib.pbkdf2_hmac('sha256', provided_password.encode('utf-8'), salt, 100000)


def fetch_data(query, params=()):
    """Busca dados do banco e retorna como DataFrame do Pandas."""
    conn = get_db_connection()
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df


def execute_query(query, params=()):
    """Executa uma query de modificação (INSERT, UPDATE, DELETE)."""
    try:
        conn = get_db_connection()
        conn.execute(query, params)
        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError as e:
        if "UNIQUE constraint failed: requisicoes.numero_rc" in str(e):
            st.error("Erro: O Nº da RC informado já existe.")
        elif "UNIQUE constraint failed: users.username" in str(e):
            st.error("Erro: Nome de usuário já existe.")
        else:
            st.error(f"Erro de integridade no banco de dados: {e}")
        return False
    except sqlite3.Error as e:
        st.error(f"Erro no banco de dados: {e}")
        return False


def backup_database():
    """Cria um backup do arquivo de banco de dados com timestamp."""
    if os.path.exists(DB_NAME):
        backup_filename = f"{DB_NAME}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        shutil.copy2(DB_NAME, backup_filename)
        return backup_filename
    return None


def reset_database():
    """Apaga os dados das tabelas, mantendo os usuários."""
    backup_file = backup_database()
    if backup_file:
        st.toast(f"Backup criado em: {backup_file}", icon="📦")

    execute_query("DELETE FROM pedidos")
    execute_query("DELETE FROM requisicoes")
    execute_query("DELETE FROM demandas")
    for filename in os.listdir(UPLOAD_DIR):
        file_path = os.path.join(UPLOAD_DIR, filename)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception as e:
            st.error(f"Erro ao deletar arquivo {file_path}: {e}")
    st.cache_data.clear()
    logging.warning(f"O usuário '{st.session_state.username}' zerou o banco de dados.")


# --- FUNÇÕES AUXILIARES ---
def format_currency(value):
    """Formata um valor numérico para o padrão monetário brasileiro."""
    if isinstance(value, (int, float)):
        return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return value


def safe_strptime(date_string, fmt):
    try:
        return datetime.strptime(date_string, fmt)
    except (ValueError, TypeError):
        return None


def to_excel(df, title="Relatório"):
    """Converte um DataFrame para um arquivo Excel formatado em memória."""
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name=title)

        workbook = writer.book
        worksheet = writer.sheets[title]

        # Estilos
        header_fill = PatternFill(start_color="4F81BD", end_color="4F81BD", fill_type="solid")
        header_font = Font(color="FFFFFF", bold=True)
        border = Border(left=Side(style='thin'),
                        right=Side(style='thin'),
                        top=Side(style='thin'),
                        bottom=Side(style='thin'))
        alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)

        # Formatar cabeçalho
        for col in range(1, len(df.columns) + 1):
            cell = worksheet.cell(row=1, column=col)
            cell.fill = header_fill
            cell.font = header_font
            cell.border = border
            cell.alignment = alignment

            # Ajustar largura das colunas
            column_letter = get_column_letter(col)
            column_len = max(df.iloc[:, col - 1].astype(str).map(len).max(), len(df.columns[col - 1])) + 2
            worksheet.column_dimensions[column_letter].width = min(column_len, 30)

        # Formatar células de dados
        for row in range(2, len(df) + 2):
            for col in range(1, len(df.columns) + 1):
                cell = worksheet.cell(row=row, column=col)
                cell.border = border
                cell.alignment = Alignment(horizontal='left', vertical='center')

                # Formatar valores monetários
                if 'valor' in df.columns[col - 1].lower() or 'total' in df.columns[col - 1].lower():
                    cell.number_format = 'R$ #,##0.00'

        # Congelar cabeçalho
        worksheet.freeze_panes = 'A2'

        # Auto-filtro
        worksheet.auto_filter.ref = worksheet.dimensions

    processed_data = output.getvalue()
    return processed_data


# --- INICIALIZAÇÃO ---
setup_database()

# --- LÓGICA DE LOGIN E REGISTRO ---
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
    st.session_state.username = ""
    st.session_state.role = ""


def login_form():
    st.title("Login do Sistema")
    with st.form("login_form"):
        username = st.text_input("Usuário")
        password = st.text_input("Senha", type="password")
        submitted = st.form_submit_button("Entrar")
        if submitted:
            user = fetch_data("SELECT * FROM users WHERE username = ?", (username,))
            if not user.empty:
                user_data = user.iloc[0]

                # Lida com a migração de senhas para usuários antigos (sem salt)
                if user_data['salt'] is None:
                    old_hashed_password = hashlib.sha256(str.encode(password)).hexdigest()
                    if user_data['password'] == old_hashed_password:
                        st.info("Atualizando a segurança da sua conta...")
                        new_hashed_password, new_salt = hash_password(password)
                        execute_query("UPDATE users SET password = ?, salt = ? WHERE username = ?",
                                      (new_hashed_password, new_salt, username))

                        st.session_state.logged_in = True
                        st.session_state.username = user_data['username']
                        st.session_state.role = user_data['role']
                        st.toast("Conta atualizada com sucesso!")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("Usuário ou senha incorretos.")

                # Login padrão para usuários com salt
                elif user_data['status'] == 'pending':
                    st.warning("Sua conta está aguardando aprovação de um administrador.")
                elif check_password(user_data['password'], user_data['salt'], password):
                    st.session_state.logged_in = True
                    st.session_state.username = user_data['username']
                    st.session_state.role = user_data['role']
                    logging.info(f"Usuário '{username}' logado com sucesso.")
                    st.rerun()
                else:
                    st.error("Usuário ou senha incorretos.")
            else:
                st.error("Usuário ou senha incorretos.")


def registration_form():
    st.title("Registro de Novo Usuário")
    with st.form("registration_form"):
        new_username = st.text_input("Novo Usuário")
        new_password = st.text_input("Nova Senha", type="password")
        is_gestor = st.checkbox("Sou um gestor (minha conta precisará de aprovação)")

        submitted = st.form_submit_button("Registrar")
        if submitted:
            if not new_username or not new_password:
                st.warning("Usuário e senha não podem estar em branco.")
                return

            users = fetch_data("SELECT id FROM users")
            if users.empty:
                role = "admin"
                status = "active"
            else:
                role = "gestor" if is_gestor else "user"
                status = "pending" if is_gestor else "active"

            hashed_password, salt = hash_password(new_password)
            if execute_query("INSERT INTO users (username, password, salt, role, status) VALUES (?, ?, ?, ?, ?)",
                             (new_username, hashed_password, salt, role, status)):
                logging.info(f"Novo usuário '{new_username}' registrado como '{role}' (status: {status}).")
                if status == 'pending':
                    st.success(f"Usuário '{new_username}' registrado! Sua conta aguarda aprovação.")
                else:
                    st.success(f"Usuário '{new_username}' registrado como '{role}'. Faça o login.")
                time.sleep(2)
                st.session_state.page = "Login"
                st.rerun()


if not st.session_state.logged_in:
    if 'page' not in st.session_state:
        st.session_state.page = "Login"
    if st.session_state.page == "Login":
        login_form()
        if st.button("Não tem uma conta? Registre-se"):
            st.session_state.page = "Registro"
            st.rerun()
    elif st.session_state.page == "Registro":
        registration_form()
        if st.button("Já tem uma conta? Faça o login"):
            st.session_state.page = "Login"
            st.rerun()
    st.stop()

# --- APLICAÇÃO PRINCIPAL (APÓS LOGIN) ---
st.title("🚀 Sistema de Controle de Compras")

# Inicializa o estado da sessão
if 'edit_id' not in st.session_state: st.session_state.edit_id = None
if 'show_rc_form' not in st.session_state: st.session_state.show_rc_form = False
if 'pedido_edit_id' not in st.session_state: st.session_state.pedido_edit_id = None
if 'show_pedido_form' not in st.session_state: st.session_state.show_pedido_form = False
if 'rc_id_para_pedido' not in st.session_state: st.session_state.rc_id_para_pedido = None
if 'confirm_reset' not in st.session_state: st.session_state.confirm_reset = False
if 'confirm_delete' not in st.session_state: st.session_state.confirm_delete = {}
if 'show_demanda_form' not in st.session_state: st.session_state.show_demanda_form = False
if 'demanda_edit_id' not in st.session_state: st.session_state.demanda_edit_id = None
if 'demanda_id_para_rc' not in st.session_state: st.session_state.demanda_id_para_rc = None
if 'pedido_to_finalize' not in st.session_state: st.session_state.pedido_to_finalize = None

# ==============================================================================
# --- BARRA LATERAL (SIDEBAR) ---
# ==============================================================================
with st.sidebar:
    st.write(f"Usuário: **{st.session_state.username}** ({st.session_state.role})")
    if st.button("Logout", use_container_width=True):
        logging.info(f"Usuário '{st.session_state.username}' deslogado.")
        for key in list(st.session_state.keys()): del st.session_state[key]
        st.rerun()

    if st.session_state.role == 'admin':
        st.header("Administração")
        with st.expander("Gerenciar Usuários"):
            pending_users = fetch_data("SELECT id, username, role FROM users WHERE status = 'pending'")
            if not pending_users.empty:
                st.subheader("Aprovações Pendentes")
                for index, user in pending_users.iterrows():
                    col1, col2, col3 = st.columns([2, 1, 1])
                    with col1:
                        st.write(f"{user['username']} ({user['role']})")
                    with col2:
                        if st.button("Aprovar", key=f"approve_{user['id']}", use_container_width=True):
                            execute_query("UPDATE users SET status = 'active' WHERE id = ?", (user['id'],))
                            st.toast(f"Usuário {user['username']} aprovado.")
                            st.rerun()
                    with col3:
                        if st.button("Rejeitar", key=f"reject_{user['id']}", use_container_width=True):
                            execute_query("DELETE FROM users WHERE id = ?", (user['id'],))
                            st.toast(f"Usuário {user['username']} rejeitado.", icon="🗑️")
                            st.rerun()
                st.markdown("---")

            st.subheader("Usuários Ativos")
            all_users = fetch_data("SELECT id, username, role FROM users WHERE status = 'active'")
            st.dataframe(all_users, use_container_width=True, hide_index=True)
            selected_user = st.selectbox("Selecione um usuário para gerenciar", options=all_users['username'].tolist())
            if selected_user:
                st.subheader(f"Gerenciando: {selected_user}")
                with st.form(f"reset_pass_{selected_user}"):
                    st.write("Redefinir Senha")
                    new_pass = st.text_input("Nova Senha", type="password", key=f"new_pass_{selected_user}")
                    if st.form_submit_button("Salvar Nova Senha"):
                        if new_pass:
                            hashed_password, salt = hash_password(new_pass)
                            if execute_query("UPDATE users SET password = ?, salt = ? WHERE username = ?",
                                             (hashed_password, salt, selected_user)):
                                st.toast(f"Senha de {selected_user} alterada.")
                        else:
                            st.warning("A nova senha não pode estar em branco.")
                with st.form(f"change_role_{selected_user}"):
                    st.write("Mudar Papel (Role)")
                    current_role = all_users[all_users['username'] == selected_user]['role'].iloc[0]
                    new_role = st.selectbox("Novo Papel", ["user", "admin", "gestor"],
                                            index=["user", "admin", "gestor"].index(current_role))
                    if st.form_submit_button("Mudar Papel"):
                        num_admins = fetch_data(
                            "SELECT COUNT(id) as count FROM users WHERE role = 'admin' AND status = 'active'").iloc[0][
                            'count']
                        if current_role == 'admin' and num_admins <= 1 and new_role != 'admin':
                            st.error("Operação negada: O sistema precisa de pelo menos um administrador.")
                        else:
                            if execute_query("UPDATE users SET role = ? WHERE username = ?", (new_role, selected_user)):
                                st.toast(f"Papel de {selected_user} alterado para {new_role}.")
                                if selected_user == st.session_state.username:
                                    st.warning("Seu papel foi alterado. Você será deslogado.")
                                    time.sleep(2)
                                    for key in list(st.session_state.keys()): del st.session_state[key]
                                    st.rerun()
                if selected_user != st.session_state.username:
                    if st.button(f"Excluir {selected_user}", use_container_width=True, type="secondary"):
                        if execute_query("DELETE FROM users WHERE username = ?", (selected_user,)):
                            st.toast(f"Usuário {selected_user} excluído.", icon="🗑️")
                            st.rerun()
                else:
                    st.info("Você não pode excluir sua própria conta.")
        with st.expander("⚠️ Opções Perigosas"):
            if st.button("Zerar Dados", use_container_width=True):
                st.session_state.confirm_reset = True
            if st.session_state.confirm_reset:
                st.warning("Tem certeza que deseja apagar TODOS os dados?")
                c1, c2 = st.columns(2)
                with c1:
                    if st.button("Sim, apagar DADOS", use_container_width=True, type="primary"):
                        reset_database()
                        st.session_state.confirm_reset = False
                        st.toast("Dados zerados!", icon="✅")
                        time.sleep(1)
                        st.rerun()
                with c2:
                    if st.button("Cancelar", use_container_width=True):
                        st.session_state.confirm_reset = False
                        st.rerun()

# ==============================================================================
# --- RENDERIZAÇÃO DE FORMULÁRIOS OU ABAS ---
# ==============================================================================

if st.session_state.show_demanda_form:
    form_title = "Editar Demanda" if st.session_state.demanda_edit_id else "Adicionar Nova Demanda"
    demanda_data = {}
    if st.session_state.demanda_edit_id:
        demanda_data_df = fetch_data("SELECT * FROM demandas WHERE id = ?", (st.session_state.demanda_edit_id,))
        if not demanda_data_df.empty:
            demanda_data = demanda_data_df.iloc[0].to_dict()

    with st.form("demanda_form"):
        st.subheader(form_title)
        descricao_necessidade = st.text_area("Descrição da Necessidade",
                                             value=demanda_data.get("descricao_necessidade", ""))

        submitted = st.form_submit_button("Salvar Demanda")
        if submitted:
            if not descricao_necessidade:
                st.warning("A descrição da necessidade é obrigatória.")
            else:
                if st.session_state.demanda_edit_id:
                    params = (descricao_necessidade, st.session_state.demanda_edit_id)
                    query = "UPDATE demandas SET descricao_necessidade=? WHERE id=?"
                    if execute_query(query, params):
                        st.toast("Demanda atualizada!")
                        st.session_state.show_demanda_form = False
                        st.session_state.demanda_edit_id = None
                        st.rerun()
    if st.button("Cancelar", key="cancel_demanda_form"):
        st.session_state.show_demanda_form = False
        st.session_state.demanda_edit_id = None
        st.rerun()

elif st.session_state.show_rc_form:
    form_title = "Editar RC" if st.session_state.edit_id else "Adicionar Nova RC"
    rc_data = {}
    demanda_origem = None

    if st.session_state.edit_id:
        rc_data_df = fetch_data("SELECT * FROM requisicoes WHERE id = ?", (st.session_state.edit_id,))
        if not rc_data_df.empty:
            rc_data = rc_data_df.iloc[0].to_dict()
    elif st.session_state.demanda_id_para_rc:
        demanda_origem_df = fetch_data("SELECT * FROM demandas WHERE id = ?", (st.session_state.demanda_id_para_rc,))
        if not demanda_origem_df.empty:
            demanda_origem = demanda_origem_df.iloc[0]
            rc_data['descricao'] = demanda_origem['descricao_necessidade']
            rc_data['solicitante'] = demanda_origem['solicitante_demanda']

    with st.form(key="rc_form"):
        st.subheader(form_title)
        if demanda_origem is not None:
            st.info(f"RC gerada a partir da Demanda Nº {demanda_origem['id']}")

        if not st.session_state.edit_id:
            conn = get_db_connection()
            next_id_df = pd.read_sql_query("SELECT MAX(id) as max_id FROM requisicoes", conn)
            conn.close()
            next_id = (next_id_df['max_id'].iloc[0] or 0) + 1
            suggested_rc_num = str(next_id)
        else:
            suggested_rc_num = rc_data.get("numero_rc", rc_data.get("id", ""))

        c1, c2, c3 = st.columns(3)
        with c1:
            numero_rc = st.text_input("Nº RC", value=suggested_rc_num)
            solicitante = st.text_input("Solicitante", value=rc_data.get("solicitante", st.session_state.username))
        with c2:
            centro_custo = st.text_input("Centro de Custo", value=rc_data.get("centro_custo", ""))
            valor = st.number_input("Valor (R$)", value=rc_data.get("valor", 0.0), format="%.2f")
        with c3:
            fornecedor = st.text_input("Fornecedor", value=rc_data.get("fornecedor", ""))
            tipo_options = ["Material", "Serviço", "Outro"]
            tipo_index = tipo_options.index(rc_data.get("tipo", "Material")) if rc_data.get(
                "tipo") in tipo_options else 0
            tipo = st.selectbox("Tipo", tipo_options, index=tipo_index)

        status = st.selectbox("Status", ["Aberto", "Finalizado", "Cancelado"],
                              index=["Aberto", "Finalizado", "Cancelado"].index(rc_data.get("status", "Aberto")))
        descricao = st.text_area("Descrição do Material ou Serviço", value=rc_data.get("descricao", ""), height=200)
        observacoes = st.text_area("Observações (opcional)", value=rc_data.get("observacoes", ""))

        submitted = st.form_submit_button("Salvar RC")
        if submitted:
            if not all([numero_rc, solicitante, centro_custo, descricao, valor > 0]):
                st.warning("Por favor, preencha todos os campos obrigatórios.")
            else:
                if st.session_state.edit_id:
                    params_db = (
                    numero_rc, solicitante, centro_custo, tipo, fornecedor, descricao, valor, status, observacoes,
                    st.session_state.edit_id)
                    q = "UPDATE requisicoes SET numero_rc=?, solicitante=?, centro_custo=?, tipo=?, fornecedor=?, descricao=?, valor=?, status=?, observacoes=? WHERE id=?"
                    if execute_query(q, params_db):
                        st.toast(f"RC Nº {numero_rc} atualizada!")
                        st.session_state.show_rc_form = False
                        st.session_state.edit_id = None
                        st.rerun()
                else:
                    data_criacao = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    demanda_id = st.session_state.demanda_id_para_rc
                    params_db = (
                    demanda_id, numero_rc, data_criacao, solicitante, centro_custo, tipo, fornecedor, descricao, valor,
                    status, observacoes)
                    q = "INSERT INTO requisicoes (demanda_id, numero_rc, data_criacao, solicitante, centro_custo, tipo, fornecedor, descricao, valor, status, observacoes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    if execute_query(q, params_db):
                        if demanda_id:
                            execute_query("UPDATE demandas SET status_demanda = 'Em atendimento' WHERE id = ?",
                                          (demanda_id,))
                        st.toast("Nova RC adicionada!")
                        st.session_state.show_rc_form = False
                        st.session_state.demanda_id_para_rc = None
                        st.rerun()
    if st.button("Cancelar", key="cancel_rc_form"):
        st.session_state.show_rc_form = False
        st.session_state.edit_id = None
        st.session_state.demanda_id_para_rc = None
        st.rerun()

elif st.session_state.show_pedido_form:
    form_title_pedido = "Editar Pedido de Compra" if st.session_state.pedido_edit_id else "Gerar Novo Pedido de Compra"
    pedido_data = {}
    rc_origem_id = st.session_state.rc_id_para_pedido
    if st.session_state.pedido_edit_id:
        pedido_data_df = fetch_data("SELECT * FROM pedidos WHERE id = ?", (st.session_state.pedido_edit_id,))
        if not pedido_data_df.empty:
            pedido_data = pedido_data_df.iloc[0].to_dict()
            rc_origem_id = pedido_data.get("rc_id")
    with st.form(key="pedido_form"):
        st.subheader(form_title_pedido)
        if rc_origem_id:
            rc_num_df = fetch_data("SELECT numero_rc FROM requisicoes WHERE id=?", (rc_origem_id,))
            if not rc_num_df.empty:
                rc_num = rc_num_df.iloc[0]['numero_rc']
                st.info(f"Este pedido é vinculado à RC Nº {rc_num}")
        previsao_entrega_val = None
        if pedido_data.get("previsao_entrega"):
            try:
                previsao_entrega_val = datetime.strptime(pedido_data.get("previsao_entrega"), '%Y-%m-%d').date()
            except (ValueError, TypeError):
                previsao_entrega_val = None
        status_options = ["Aguardando Entrega", "Entregue Parcialmente", "Entregue", "Atrasado", "Cancelado"]
        status_index = 0
        if pedido_data.get("status_pedido") in status_options:
            status_index = status_options.index(pedido_data.get("status_pedido"))
        c1, c2, c3 = st.columns(3)
        with c1:
            numero_pedido = st.text_input("Nº Pedido Fornecedor", value=pedido_data.get("numero_pedido", ""))
        with c2:
            previsao_entrega = st.date_input("Previsão de Entrega", value=previsao_entrega_val)
        with c3:
            status_pedido = st.selectbox("Status do Pedido", status_options, index=status_index)
        observacoes_pedido = st.text_area("Observações do Pedido", value=pedido_data.get("observacoes_pedido", ""))
        submitted_pedido = st.form_submit_button("Salvar Pedido")
        if submitted_pedido:
            previsao_entrega_str = previsao_entrega.strftime('%Y-%m-%d') if previsao_entrega else None
            if st.session_state.pedido_edit_id:
                params_db_pedido = (
                numero_pedido, previsao_entrega_str, status_pedido, observacoes_pedido, st.session_state.pedido_edit_id)
                q_pedido = "UPDATE pedidos SET numero_pedido=?, previsao_entrega=?, status_pedido=?, observacoes_pedido=? WHERE id=?"
                if execute_query(q_pedido, params_db_pedido):
                    st.toast(f"Pedido Nº {st.session_state.pedido_edit_id} atualizado!")
                    st.session_state.show_pedido_form = False
                    st.session_state.pedido_edit_id = None
                    st.rerun()
            else:
                data_pedido = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                params_db_pedido = (
                rc_origem_id, data_pedido, numero_pedido, previsao_entrega_str, status_pedido, observacoes_pedido)
                q_pedido = "INSERT INTO pedidos (rc_id, data_pedido, numero_pedido, previsao_entrega, status_pedido, observacoes_pedido) VALUES (?, ?, ?, ?, ?, ?)"
                if execute_query(q_pedido, params_db_pedido):
                    st.toast("Novo pedido de compra gerado!")
                    st.session_state.show_pedido_form = False
                    st.session_state.rc_id_para_pedido = None
                    st.rerun()
    if st.button("Cancelar", key="cancel_pedido_form"):
        st.session_state.show_pedido_form = False
        st.session_state.rc_id_para_pedido = None
        st.session_state.pedido_edit_id = None
        st.rerun()

else:
    # --- VISUALIZAÇÃO DAS ABAS ---
    tab_dashboard, tab_demandas, tab_rcs, tab_pedidos_andamento, tab_pedidos_finalizados = st.tabs(
        ["📊 Dashboard", "📝 Demandas de Compras", "🛒 Requisições (RCs)", "🚚 Pedidos em Andamento",
         "✅ Pedidos Finalizados"])

    with tab_dashboard:
        st.header("Dashboard de Métricas")

        with st.expander("Filtros do Dashboard"):
            solicitantes_dash = fetch_data("SELECT DISTINCT solicitante_demanda FROM demandas")
            solicitante_list_dash = solicitantes_dash['solicitante_demanda'].tolist()
            filtro_solicitante_dash = st.multiselect("Filtrar por Solicitante da Demanda",
                                                     options=solicitante_list_dash)

        # Constrói a cláusula WHERE para as queries
        where_clause_demanda = ""
        params_demanda_dash = []
        if filtro_solicitante_dash:
            where_clause_demanda = f"AND solicitante_demanda IN ({','.join(['?'] * len(filtro_solicitante_dash))})"
            params_demanda_dash.extend(filtro_solicitante_dash)

        with st.spinner("Carregando métricas..."):
            query_total_gasto = f"""
                SELECT SUM(r.valor) as total 
                FROM requisicoes r 
                JOIN demandas d ON r.demanda_id = d.id 
                WHERE r.status = 'Finalizado' {where_clause_demanda.replace('solicitante_demanda', 'd.solicitante_demanda')}
            """
            total_rcs_finalizadas = fetch_data(query_total_gasto, tuple(params_demanda_dash)).iloc[0]['total'] or 0

            demandas_abertas = fetch_data(
                f"SELECT COUNT(id) as count FROM demandas WHERE status_demanda = 'Aberta' {where_clause_demanda}",
                tuple(params_demanda_dash)).iloc[0]['count']

            query_rcs_abertas = f"""
                SELECT COUNT(r.id) as count 
                FROM requisicoes r 
                JOIN demandas d ON r.demanda_id = d.id 
                WHERE r.status = 'Aberto' {where_clause_demanda.replace('solicitante_demanda', 'd.solicitante_demanda')}
            """
            rcs_abertas = fetch_data(query_rcs_abertas, tuple(params_demanda_dash)).iloc[0]['count']

            query_pedidos_andamento = f"""
                SELECT COUNT(p.id) as count 
                FROM pedidos p 
                JOIN requisicoes r ON p.rc_id = r.id 
                JOIN demandas d ON r.demanda_id = d.id 
                WHERE p.status_pedido NOT IN ('Entregue', 'Cancelado') {where_clause_demanda.replace('solicitante_demanda', 'd.solicitante_demanda')}
            """
            pedidos_andamento = fetch_data(query_pedidos_andamento, tuple(params_demanda_dash)).iloc[0]['count']

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric(label="Total Gasto (RCs Finalizadas)", value=format_currency(total_rcs_finalizadas))
        with col2:
            st.metric(label="Demandas Abertas", value=demandas_abertas)
        with col3:
            st.metric(label="RCs Abertas", value=rcs_abertas)
        with col4:
            st.metric(label="Pedidos em Andamento", value=pedidos_andamento)

    with tab_demandas:
        st.header("Demandas de Compras")

        with st.expander("➕ Adicionar Nova Demanda"):
            with st.form("demanda_form", clear_on_submit=True):
                descricao_necessidade = st.text_area("O que precisa comprar ou contratar? (Material ou Serviço)")
                uploaded_file = st.file_uploader("Anexar arquivo (imagem, PDF, Doc)",
                                                 type=["png", "jpg", "jpeg", "pdf", "doc", "docx"])

                submitted = st.form_submit_button("Registrar Demanda")
                if submitted:
                    if not descricao_necessidade:
                        st.warning("A descrição é obrigatória.")
                    else:
                        anexo_path = None
                        if uploaded_file is not None:
                            file_path = os.path.join(UPLOAD_DIR, f"{int(time.time())}_{uploaded_file.name}")
                            with open(file_path, "wb") as f:
                                f.write(uploaded_file.getbuffer())
                            anexo_path = file_path

                        params = (
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'), st.session_state.username, descricao_necessidade,
                        anexo_path, 'Aberta')
                        query = "INSERT INTO demandas (data_demanda, solicitante_demanda, descricao_necessidade, anexo_path, status_demanda) VALUES (?, ?, ?, ?, ?)"
                        if execute_query(query, params):
                            st.toast("Nova demanda registrada com sucesso!")
                            st.rerun()

        with st.expander("🔍 Filtros e Busca"):
            filtro_busca_demanda = st.text_input("Buscar na descrição da demanda")

            # Filtro por gestor
            solicitantes = fetch_data("SELECT DISTINCT solicitante_demanda FROM demandas")
            solicitante_list = solicitantes['solicitante_demanda'].tolist()
            filtro_solicitante = st.multiselect("Filtrar por Solicitante", options=solicitante_list)

        query_demanda = "SELECT * FROM demandas WHERE 1=1"
        params_demanda = []
        if filtro_busca_demanda:
            query_demanda += " AND descricao_necessidade LIKE ?"
            params_demanda.append(f"%{filtro_busca_demanda}%")
        if filtro_solicitante:
            query_demanda += f" AND solicitante_demanda IN ({','.join(['?'] * len(filtro_solicitante))})"
            params_demanda.extend(filtro_solicitante)
        query_demanda += " ORDER BY id DESC"

        with st.spinner("Carregando demandas..."):
            df_demandas = fetch_data(query_demanda, tuple(params_demanda))

        st.header("Lista de Demandas")
        if df_demandas.empty:
            st.info("Nenhuma demanda encontrada.")
        else:
            for index, row in df_demandas.iterrows():
                st.markdown("---")
                col1, col2 = st.columns([3, 1])
                with col1:
                    status_demanda = row.get('status_demanda', 'Status Indefinido')
                    status_emoji = "🔵" if status_demanda == "Aberta" else "🟢" if status_demanda == "Em atendimento" else "🔴"
                    st.subheader(f"Demanda Nº {row['id']} - Status: {status_emoji} {status_demanda}")
                    st.write(f"**Solicitante:** {row['solicitante_demanda']}")
                    st.write(
                        f"**Data:** {safe_strptime(row['data_demanda'], '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y') if safe_strptime(row['data_demanda'], '%Y-%m-%d %H:%M:%S') else 'Data inválida'}")
                    st.info(f"**Necessidade:** {row['descricao_necessidade']}")
                with col2:
                    if row['anexo_path'] and os.path.exists(row['anexo_path']):
                        file_path = row['anexo_path']
                        file_name = os.path.basename(file_path)
                        file_extension = os.path.splitext(file_name)[1].lower()

                        if file_extension in ['.png', '.jpg', '.jpeg']:
                            try:
                                image = Image.open(file_path)
                                st.image(image, caption="Anexo", use_container_width=True)
                            except Exception as e:
                                st.warning("Não foi possível carregar a imagem.")
                        else:
                            try:
                                with open(file_path, "rb") as file:
                                    st.download_button(
                                        label=f"Baixar Anexo ({file_name})",
                                        data=file,
                                        file_name=file_name,
                                        mime="application/octet-stream",
                                        key=f"download_{row['id']}"
                                    )
                            except Exception as e:
                                st.warning("Não foi possível carregar o anexo para download.")

                c1, c2, c3 = st.columns([1, 1, 3])
                with c1:
                    if row['solicitante_demanda'] == st.session_state.username or st.session_state.role in ['admin',
                                                                                                            'gestor']:
                        if st.button("✏️ Editar", key=f"edit_demanda_{row['id']}"):
                            st.session_state.demanda_edit_id = row['id']
                            st.session_state.show_demanda_form = True
                            st.rerun()
                with c2:
                    if row['solicitante_demanda'] == st.session_state.username or st.session_state.role == 'admin':
                        if st.button("🗑️ Excluir", key=f"del_demanda_{row['id']}", type="secondary"):
                            st.session_state.confirm_delete['demanda'] = row['id']
                            st.rerun()

                if st.session_state.confirm_delete.get('demanda') == row['id']:
                    st.warning(f"Tem certeza que deseja excluir a Demanda Nº {row['id']}?")
                    del_c1, del_c2 = st.columns(2)
                    with del_c1:
                        if st.button("Sim, excluir demanda", key=f"confirm_del_demanda_{row['id']}",
                                     use_container_width=True):
                            if execute_query("DELETE FROM demandas WHERE id = ?", (row['id'],)):
                                if row['anexo_path'] and os.path.exists(row['anexo_path']):
                                    os.remove(row['anexo_path'])
                                st.toast("Demanda excluída.", icon="🗑️")
                                st.session_state.confirm_delete = {}
                                st.rerun()
                    with del_c2:
                        if st.button("Cancelar", key=f"cancel_del_demanda_{row['id']}", use_container_width=True):
                            st.session_state.confirm_delete = {}
                            st.rerun()

                with c3:
                    status_demanda = row.get('status_demanda', 'Aberta')
                    if status_demanda == 'Aberta' and st.session_state.role != 'gestor':
                        if st.button("🛒 Criar RC", key=f"create_rc_{row['id']}", type="primary"):
                            st.session_state.demanda_id_para_rc = row['id']
                            st.session_state.show_rc_form = True
                            st.rerun()
                    elif status_demanda == 'Em atendimento':
                        st.success("✔️ Demanda em atendimento (RC criada).")
                    elif status_demanda == 'Finalizada':
                        st.error("✔️ Demanda Finalizada (Pedido Entregue).")

    with tab_rcs:
        st.header("Requisições de Compra (RCs)")
        if st.session_state.role != 'gestor':
            if st.button("➕ Adicionar Nova RC", key="add_rc"):
                st.session_state.show_rc_form = True
                st.session_state.edit_id = None
                st.session_state.demanda_id_para_rc = None
                st.rerun()

        # --- FILTROS E RELATÓRIOS DE RC ---
        with st.expander("Filtros e Relatórios de RCs"):
            rc_status_list = ["Aberto", "Finalizado", "Cancelado"]
            filtro_status_rc = st.multiselect("Filtrar por Status da RC", options=rc_status_list)

            c1, c2 = st.columns(2)
            with c1:
                filtro_data_inicio_rc = st.date_input("Data de Início da RC", value=None)
            with c2:
                filtro_data_fim_rc = st.date_input("Data de Fim da RC", value=None)

        query_rc = "SELECT * FROM requisicoes WHERE 1=1"
        params_rc = []
        if filtro_status_rc:
            query_rc += f" AND status IN ({','.join(['?'] * len(filtro_status_rc))})"
            params_rc.extend(filtro_status_rc)
        if filtro_data_inicio_rc:
            query_rc += " AND date(data_criacao) >= ?"
            params_rc.append(filtro_data_inicio_rc)
        if filtro_data_fim_rc:
            query_rc += " AND date(data_criacao) <= ?"
            params_rc.append(filtro_data_fim_rc)
        query_rc += " ORDER BY id DESC"

        with st.spinner("Carregando requisições..."):
            df_rc = fetch_data(query_rc, tuple(params_rc))

        if not df_rc.empty:
            st.download_button(
                label="📥 Exportar RCs para Excel",
                data=to_excel(df_rc, "Relatório de RCs"),
                file_name='relatorio_rcs.xlsx',
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            )

        st.header("Lista de Requisições")
        if df_rc.empty:
            st.info("Nenhuma RC encontrada com os filtros aplicados.")
        else:
            df_display_rc = df_rc.copy()
            df_display_rc['valor'] = df_display_rc['valor'].apply(format_currency)
            df_display_rc['data_criacao'] = pd.to_datetime(df_display_rc['data_criacao']).dt.strftime('%d/%m/%Y')
            st.dataframe(df_display_rc, use_container_width=True, hide_index=True)

            if st.session_state.role != 'gestor':
                st.subheader("Operações com a RC Selecionada")
                rc_ids = df_rc['id'].tolist()
                selected_id_rc = st.selectbox("Selecione uma RC", options=rc_ids, format_func=lambda x: f"RC Nº {x}",
                                              key="select_rc")
                if selected_id_rc:
                    col_edit, col_delete, col_gerar_pedido, col_space = st.columns([1.5, 1.5, 2, 5])
                    with col_edit:
                        if st.button("✏️ Editar RC", use_container_width=True):
                            st.session_state.edit_id = selected_id_rc
                            st.session_state.show_rc_form = True
                            st.rerun()
                    with col_delete:
                        if st.button("🗑️ Excluir RC", use_container_width=True, type="secondary"):
                            st.session_state.confirm_delete['rc'] = selected_id_rc
                            st.rerun()

                    if st.session_state.confirm_delete.get('rc') == selected_id_rc:
                        pedidos_associados = fetch_data("SELECT id FROM pedidos WHERE rc_id = ?", (selected_id_rc,))
                        if not pedidos_associados.empty:
                            st.error(
                                f"Não é possível excluir a RC Nº {selected_id_rc}, pois ela possui pedidos associados.")
                            if st.button("Ok, entendi", key=f"ack_del_rc_{selected_id_rc}"):
                                st.session_state.confirm_delete = {}
                                st.rerun()
                        else:
                            st.warning(f"Tem certeza que deseja excluir a RC Nº {selected_id_rc}?")
                            del_c1, del_c2 = st.columns(2)
                            with del_c1:
                                if st.button("Sim, excluir RC", key=f"confirm_del_rc_{selected_id_rc}",
                                             use_container_width=True):
                                    if execute_query("DELETE FROM requisicoes WHERE id = ?", (selected_id_rc,)):
                                        st.toast(f"RC Nº {selected_id_rc} excluída!", icon="🗑️")
                                        st.session_state.confirm_delete = {}
                                        st.rerun()
                            with del_c2:
                                if st.button("Cancelar", key=f"cancel_del_rc_{selected_id_rc}",
                                             use_container_width=True):
                                    st.session_state.confirm_delete = {}
                                    st.rerun()

                    selected_rc_details = df_rc[df_rc['id'] == selected_id_rc].iloc[0]
                    if selected_rc_details['status'] == 'Finalizado':
                        pedido_existente = fetch_data("SELECT id FROM pedidos WHERE rc_id = ?", (selected_id_rc,))
                        with col_gerar_pedido:
                            if pedido_existente.empty:
                                if st.button("🛒 Gerar Pedido", use_container_width=True):
                                    st.session_state.rc_id_para_pedido = selected_id_rc
                                    st.session_state.show_pedido_form = True
                                    st.session_state.pedido_edit_id = None
                                    st.rerun()
                            else:
                                st.info(f"Pedido já existe.")

    with tab_pedidos_andamento:
        st.header("Pedidos de Compra em Andamento")

        # --- FILTROS E RELATÓRIOS DE PEDIDOS ---
        with st.expander("Filtros e Relatórios de Pedidos"):
            pedido_status_list = ["Aguardando Entrega", "Entregue Parcialmente", "Atrasado", "Cancelado"]
            filtro_status_pedido = st.multiselect("Filtrar por Status do Pedido", options=pedido_status_list)

            c1, c2 = st.columns(2)
            with c1:
                filtro_data_inicio_pedido = st.date_input("Data de Início do Pedido", value=None)
            with c2:
                filtro_data_fim_pedido = st.date_input("Data de Fim do Pedido", value=None)

        query_pedidos = "SELECT p.id, p.rc_id, r.numero_rc, p.data_pedido, p.numero_pedido, p.previsao_entrega, p.status_pedido, r.solicitante, p.observacoes_pedido FROM requisicoes r JOIN pedidos p ON r.id = p.rc_id WHERE p.status_pedido != 'Entregue'"
        params_pedidos = []
        if filtro_status_pedido:
            query_pedidos += f" AND p.status_pedido IN ({','.join(['?'] * len(filtro_status_pedido))})"
            params_pedidos.extend(filtro_status_pedido)
        if filtro_data_inicio_pedido:
            query_pedidos += " AND date(p.data_pedido) >= ?"
            params_pedidos.append(filtro_data_inicio_pedido)
        if filtro_data_fim_pedido:
            query_pedidos += " AND date(p.data_pedido) <= ?"
            params_pedidos.append(filtro_data_fim_pedido)
        query_pedidos += " ORDER BY p.id DESC"

        with st.spinner("Carregando pedidos..."):
            df_pedidos = fetch_data(query_pedidos, tuple(params_pedidos))

        if not df_pedidos.empty:
            st.download_button(
                label="📥 Exportar Pedidos para Excel",
                data=to_excel(df_pedidos, "Pedidos em Andamento"),
                file_name='pedidos_em_andamento.xlsx',
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            )

        if df_pedidos.empty:
            st.info("Nenhum pedido de compra encontrado com os filtros aplicados.")
        else:
            st.dataframe(df_pedidos, use_container_width=True, hide_index=True)
            if st.session_state.role != 'gestor':
                st.subheader("Operações com o Pedido Selecionado")
                pedido_ids = df_pedidos['id'].tolist()
                selected_id_pedido = st.selectbox("Selecione um Pedido", options=pedido_ids,
                                                  format_func=lambda x: f"Pedido Nº {x}", key="select_pedido")
                if selected_id_pedido:
                    col_edit, col_delete, col_finalize, col_space = st.columns([1.5, 1.5, 2, 5])
                    with col_edit:
                        if st.button("✏️ Editar Pedido", use_container_width=True):
                            st.session_state.pedido_edit_id = selected_id_pedido
                            st.session_state.show_pedido_form = True
                            st.rerun()
                    with col_delete:
                        if st.button("🗑️ Excluir Pedido", use_container_width=True, type="secondary"):
                            st.session_state.confirm_delete['pedido'] = selected_id_pedido
                            st.rerun()

                    selected_pedido_details = df_pedidos[df_pedidos['id'] == selected_id_pedido].iloc[0]
                    if selected_pedido_details['status_pedido'] not in ['Entregue', 'Cancelado']:
                        with col_finalize:
                            if st.button("✅ Finalizar Pedido", use_container_width=True):
                                st.session_state.pedido_to_finalize = selected_id_pedido
                                st.rerun()

                    if st.session_state.confirm_delete.get('pedido') == selected_id_pedido:
                        st.warning(f"Tem certeza que deseja excluir o Pedido Nº {selected_id_pedido}?")
                        c1, c2 = st.columns(2)
                        with c1:
                            if st.button("Sim, excluir pedido", use_container_width=True, type="primary"):
                                if execute_query("DELETE FROM pedidos WHERE id = ?", (selected_id_pedido,)):
                                    st.toast(f"Pedido Nº {selected_id_pedido} excluído!", icon="🗑️")
                                    st.session_state.confirm_delete = {}
                                    st.rerun()
                        with c2:
                            if st.button("Cancelar exclusão", use_container_width=True):
                                st.session_state.confirm_delete = {}
                                st.rerun()

                    if 'pedido_to_finalize' in st.session_state and st.session_state.pedido_to_finalize == selected_id_pedido:
                        st.warning("Tem certeza que deseja marcar este pedido como FINALIZADO?")
                        st.info("Esta ação irá:")
                        st.info("- Marcar o pedido como 'Entregue'")
                        st.info("- Atualizar a demanda relacionada para 'Finalizado'")

                        c1, c2 = st.columns(2)
                        with c1:
                            if st.button("Sim, Finalizar Pedido", use_container_width=True, type="primary"):
                                # Atualiza o status do pedido
                                if execute_query("UPDATE pedidos SET status_pedido = 'Entregue' WHERE id = ?",
                                                 (selected_id_pedido,)):
                                    # Busca a RC relacionada ao pedido
                                    rc_info = fetch_data(
                                        "SELECT demanda_id FROM requisicoes WHERE id = (SELECT rc_id FROM pedidos WHERE id = ?)",
                                        (selected_id_pedido,))
                                    if not rc_info.empty and rc_info.iloc[0]['demanda_id']:
                                        execute_query("UPDATE demandas SET status_demanda = 'Finalizada' WHERE id = ?",
                                                      (rc_info.iloc[0]['demanda_id'],))
                                    st.toast(f"Pedido Nº {selected_id_pedido} finalizado com sucesso!")
                                    del st.session_state.pedido_to_finalize
                                    st.rerun()
                        with c2:
                            if st.button("Cancelar", use_container_width=True):
                                del st.session_state.pedido_to_finalize
                                st.rerun()

    with tab_pedidos_finalizados:
        st.header("Pedidos de Compra Finalizados")
        query_finalizados = "SELECT p.id, p.rc_id, r.numero_rc, p.data_pedido, p.numero_pedido, p.previsao_entrega, p.status_pedido, r.solicitante, p.observacoes_pedido FROM requisicoes r JOIN pedidos p ON r.id = p.rc_id WHERE p.status_pedido = 'Entregue' ORDER BY p.id DESC"
        df_finalizados = fetch_data(query_finalizados)
        if df_finalizados.empty:
            st.info("Nenhum pedido finalizado encontrado.")
        else:
            if not df_finalizados.empty:
                st.download_button(
                    label="📥 Exportar Pedidos Finalizados para Excel",
                    data=to_excel(df_finalizados, "Pedidos Finalizados"),
                    file_name='pedidos_finalizados.xlsx',
                    mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                )
            st.dataframe(df_finalizados, use_container_width=True, hide_index=True)
