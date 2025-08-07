import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime, date
import os
import time
import hashlib
from PIL import Image

# --- CONFIGURAÇÕES DA PÁGINA E ESTADO DA SESSÃO ---
st.set_page_config(page_title="Controle de Compras", layout="wide")

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
            password TEXT NOT NULL,
            role TEXT NOT NULL
        )
    """)

    # --- VERIFICAÇÃO E ADIÇÃO DE COLUNAS FALTANTES ---
    # Verifica a coluna status na tabela users
    cursor.execute("PRAGMA table_info(users)")
    columns = [info[1] for info in cursor.fetchall()]
    if 'status' not in columns:
        # Adiciona a coluna e define todos os usuários existentes como 'active'
        cursor.execute("ALTER TABLE users ADD COLUMN status TEXT NOT NULL DEFAULT 'active'")

    # Verifica a coluna demanda_id na tabela requisicoes
    cursor.execute("PRAGMA table_info(requisicoes)")
    columns_req = [info[1] for info in cursor.fetchall()]
    if 'demanda_id' not in columns_req:
        cursor.execute("ALTER TABLE requisicoes ADD COLUMN demanda_id INTEGER")

    conn.commit()
    conn.close()


def hash_password(password):
    """Retorna o hash de uma senha."""
    return hashlib.sha256(str.encode(password)).hexdigest()


def check_password(hashed_password, user_password):
    """Verifica se a senha fornecida corresponde ao hash."""
    return hashed_password == hash_password(user_password)


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
            st.error("Erro: O Nº da RC informado já existe. Por favor, utilize outro número.")
        elif "UNIQUE constraint failed: users.username" in str(e):
            st.error("Erro: Nome de usuário já existe.")
        else:
            st.error(f"Erro de integridade no banco de dados: {e}")
        return False
    except sqlite3.Error as e:
        st.error(f"Erro no banco de dados: {e}")
        return False


def reset_database():
    """Apaga os dados das tabelas, mantendo os usuários."""
    execute_query("DELETE FROM pedidos")
    execute_query("DELETE FROM requisicoes")
    execute_query("DELETE FROM demandas")
    # Limpa a pasta de uploads
    for filename in os.listdir(UPLOAD_DIR):
        file_path = os.path.join(UPLOAD_DIR, filename)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception as e:
            st.error(f"Erro ao deletar arquivo {file_path}: {e}")
    st.cache_data.clear()


# --- FUNÇÕES AUXILIARES ---
def format_currency(value):
    """Formata um valor numérico para o padrão monetário brasileiro."""
    if isinstance(value, (int, float)):
        return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return value


@st.cache_data
def check_column_exists(table_name, column_name):
    """Verifica se uma coluna existe em uma tabela."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [info[1] for info in cursor.fetchall()]
    conn.close()
    return column_name in columns


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
                if user_data['status'] == 'pending':
                    st.warning("Sua conta está aguardando aprovação de um administrador.")
                elif check_password(user_data['password'], password):
                    st.session_state.logged_in = True
                    st.session_state.username = user_data['username']
                    st.session_state.role = user_data['role']
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
                if is_gestor:
                    role = "gestor"
                    status = "pending"
                else:
                    role = "user"
                    status = "active"

            hashed_password = hash_password(new_password)
            if execute_query("INSERT INTO users (username, password, role, status) VALUES (?, ?, ?, ?)",
                             (new_username, hashed_password, role, status)):
                if status == 'pending':
                    st.success(
                        f"Usuário '{new_username}' registrado com sucesso! Sua conta aguarda aprovação de um administrador.")
                else:
                    st.success(f"Usuário '{new_username}' registrado com sucesso como '{role}'. Faça o login.")
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
if 'confirm_pedido_delete' not in st.session_state: st.session_state.confirm_pedido_delete = None
if 'show_demanda_form' not in st.session_state: st.session_state.show_demanda_form = False
if 'demanda_edit_id' not in st.session_state: st.session_state.demanda_edit_id = None
if 'demanda_id_para_rc' not in st.session_state: st.session_state.demanda_id_para_rc = None

# ==============================================================================
# --- BARRA LATERAL (SIDEBAR) ---
# ==============================================================================
with st.sidebar:
    st.write(f"Usuário: **{st.session_state.username}** ({st.session_state.role})")
    if st.button("Logout", use_container_width=True):
        for key in list(st.session_state.keys()): del st.session_state[key]
        st.rerun()

    if st.session_state.role == 'admin':
        st.header("Administração")
        with st.expander("Gerenciar Usuários"):
            # Aprovações pendentes
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
                            st.success(f"Usuário {user['username']} aprovado.")
                            st.rerun()
                    with col3:
                        if st.button("Rejeitar", key=f"reject_{user['id']}", use_container_width=True):
                            execute_query("DELETE FROM users WHERE id = ?", (user['id'],))
                            st.warning(f"Usuário {user['username']} rejeitado e excluído.")
                            st.rerun()
                st.markdown("---")

            # Gerenciar usuários ativos
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
                            hashed_pass = hash_password(new_pass)
                            if execute_query("UPDATE users SET password = ? WHERE username = ?",
                                             (hashed_pass, selected_user)):
                                st.success(f"Senha de {selected_user} alterada com sucesso.")
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
                            st.error("Não é possível remover o último administrador.")
                        else:
                            if execute_query("UPDATE users SET role = ? WHERE username = ?", (new_role, selected_user)):
                                st.success(f"O papel de {selected_user} foi alterado para {new_role}.")
                                if selected_user == st.session_state.username:
                                    st.warning("Seu papel foi alterado. Você será deslogado.")
                                    time.sleep(2)
                                    for key in list(st.session_state.keys()): del st.session_state[key]
                                    st.rerun()
                if selected_user != st.session_state.username:
                    if st.button(f"Excluir {selected_user}", use_container_width=True, type="secondary"):
                        if execute_query("DELETE FROM users WHERE username = ?", (selected_user,)):
                            st.success(f"Usuário {selected_user} excluído com sucesso.")
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
                        st.success("Dados zerados!")
                        time.sleep(1)
                        st.rerun()
                with c2:
                    if st.button("Cancelar", use_container_width=True):
                        st.session_state.confirm_reset = False
                        st.rerun()

# ==============================================================================
# --- RENDERIZAÇÃO DE FORMULÁRIOS OU ABAS ---
# ==============================================================================

if st.session_state.show_rc_form:
    # --- FORMULÁRIO DE ADIÇÃO/EDIÇÃO DE RC ---
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
                        st.success(f"RC Nº {numero_rc} atualizada!")
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
                        st.success("Nova RC adicionada!")
                        st.session_state.show_rc_form = False
                        st.session_state.demanda_id_para_rc = None
                        st.rerun()
    if st.button("Cancelar", key="cancel_rc_form"):
        st.session_state.show_rc_form = False
        st.session_state.edit_id = None
        st.session_state.demanda_id_para_rc = None
        st.rerun()

elif st.session_state.show_pedido_form:
    # --- FORMULÁRIO DE GERAÇÃO/EDIÇÃO DE PEDIDO ---
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
                    st.success(f"Pedido Nº {st.session_state.pedido_edit_id} atualizado!")
                    st.session_state.show_pedido_form = False
                    st.session_state.pedido_edit_id = None
                    st.rerun()
            else:
                data_pedido = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                params_db_pedido = (
                rc_origem_id, data_pedido, numero_pedido, previsao_entrega_str, status_pedido, observacoes_pedido)
                q_pedido = "INSERT INTO pedidos (rc_id, data_pedido, numero_pedido, previsao_entrega, status_pedido, observacoes_pedido) VALUES (?, ?, ?, ?, ?, ?)"
                if execute_query(q_pedido, params_db_pedido):
                    st.success("Novo pedido de compra gerado!")
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
    tab_demandas, tab_rcs, tab_pedidos = st.tabs(["Demandas de Compras", "Requisições (RCs)", "Pedidos de Compra"])
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
                            st.success("Nova demanda registrada com sucesso!")
                            st.rerun()

        st.header("Lista de Demandas")
        df_demandas = fetch_data("SELECT * FROM demandas ORDER BY id DESC")
        if df_demandas.empty:
            st.info("Nenhuma demanda registrada.")
        else:
            for index, row in df_demandas.iterrows():
                st.markdown("---")
                col1, col2 = st.columns([3, 1])
                with col1:
                    status_demanda = row.get('status_demanda', 'Status Indefinido')
                    st.subheader(f"Demanda Nº {row['id']} - Status: {status_demanda}")
                    st.write(f"**Solicitante:** {row['solicitante_demanda']}")
                    st.write(
                        f"**Data:** {datetime.strptime(row['data_demanda'], '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y')}")
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

                action_col1, action_col2 = st.columns([1, 4])
                with action_col1:
                    if row['solicitante_demanda'] == st.session_state.username or st.session_state.role == 'admin':
                        if st.button("🗑️ Excluir", key=f"del_demanda_{row['id']}", type="secondary"):
                            execute_query("DELETE FROM demandas WHERE id = ?", (row['id'],))
                            if row['anexo_path'] and os.path.exists(row['anexo_path']):
                                os.remove(row['anexo_path'])
                            st.success("Demanda excluída.")
                            st.rerun()
                with action_col2:
                    status_demanda = row.get('status_demanda', 'Aberta')
                    if status_demanda == 'Aberta' and st.session_state.role != 'gestor':
                        if st.button("🛒 Criar RC a partir desta Demanda", key=f"create_rc_{row['id']}", type="primary"):
                            st.session_state.demanda_id_para_rc = row['id']
                            st.session_state.show_rc_form = True
                            st.rerun()
                    elif status_demanda != 'Aberta':
                        st.success("✔️ Demanda em atendimento (RC criada).")

    with tab_rcs:
        st.header("Requisições de Compra (RCs)")
        if st.session_state.role != 'gestor':
            if st.button("➕ Adicionar Nova RC", key="add_rc"):
                st.session_state.show_rc_form = True
                st.session_state.edit_id = None
                st.session_state.demanda_id_para_rc = None
                st.rerun()

        st.header("Lista de Requisições")
        df_rc = fetch_data("SELECT * FROM requisicoes ORDER BY id DESC")
        if df_rc.empty:
            st.info("Nenhuma RC encontrada.")
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
                            if execute_query("DELETE FROM requisicoes WHERE id = ?", (selected_id_rc,)):
                                st.success(f"RC Nº {selected_id_rc} excluída!")
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

    with tab_pedidos:
        st.header("Pedidos de Compra Gerados")
        query_pedidos = "SELECT p.id, p.rc_id, r.numero_rc, p.data_pedido, p.numero_pedido, p.previsao_entrega, p.status_pedido, r.solicitante, p.observacoes_pedido FROM requisicoes r JOIN pedidos p ON r.id = p.rc_id ORDER BY p.id DESC"
        df_pedidos = fetch_data(query_pedidos)
        if df_pedidos.empty:
            st.info("Nenhum pedido de compra encontrado.")
        else:
            st.dataframe(df_pedidos, use_container_width=True, hide_index=True)
            if st.session_state.role != 'gestor':
                st.subheader("Operações com o Pedido Selecionado")
                pedido_ids = df_pedidos['id'].tolist()
                selected_id_pedido = st.selectbox("Selecione um Pedido", options=pedido_ids,
                                                  format_func=lambda x: f"Pedido Nº {x}", key="select_pedido")
                if selected_id_pedido:
                    col_edit, col_delete, col_space = st.columns([1.5, 1.5, 7])
                    with col_edit:
                        if st.button("✏️ Editar Pedido", use_container_width=True):
                            st.session_state.pedido_edit_id = selected_id_pedido
                            st.session_state.show_pedido_form = True
                            st.rerun()
                    with col_delete:
                        if st.button("🗑️ Excluir Pedido", use_container_width=True, type="secondary"):
                            st.session_state.confirm_pedido_delete = selected_id_pedido
                            st.rerun()
                    if st.session_state.confirm_pedido_delete == selected_id_pedido:
                        st.warning(f"Tem certeza que deseja excluir o Pedido Nº {selected_id_pedido}?")
                        c1, c2 = st.columns(2)
                        with c1:
                            if st.button("Sim, excluir pedido", use_container_width=True, type="primary"):
                                if execute_query("DELETE FROM pedidos WHERE id = ?", (selected_id_pedido,)):
                                    st.success(f"Pedido Nº {selected_id_pedido} excluído!")
                                    st.session_state.confirm_pedido_delete = None
                                    time.sleep(1)
                                    st.rerun()
                        with c2:
                            if st.button("Cancelar exclusão", use_container_width=True):
                                st.session_state.confirm_pedido_delete = None
                                st.rerun()
