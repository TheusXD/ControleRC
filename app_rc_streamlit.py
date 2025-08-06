import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime, date
from fpdf import FPDF
import os
import time
import hashlib

# --- CONFIGURAÇÕES DA PÁGINA E ESTADO DA SESSÃO ---
st.set_page_config(page_title="Controle de RCs e Pedidos", layout="wide")

# --- FUNÇÕES DE BANCO DE DADOS E AUTENTICAÇÃO ---
DB_NAME = "controle_rcs.db"


def get_db_connection():
    """Cria e retorna uma conexão com o banco de dados."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn


def setup_database():
    """Cria as tabelas no banco de dados se elas não existirem."""
    conn = get_db_connection()
    cursor = conn.cursor()
    # Tabela de Requisições
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS requisicoes (
            id INTEGER PRIMARY KEY,
            numero_rc TEXT UNIQUE NOT NULL,
            data_criacao TEXT NOT NULL,
            solicitante TEXT NOT NULL,
            centro_custo TEXT NOT NULL,
            tipo TEXT NOT NULL,
            fornecedor TEXT,
            descricao TEXT NOT NULL,
            valor REAL NOT NULL,
            status TEXT NOT NULL,
            observacoes TEXT
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
    """Apaga os dados das tabelas de RCs e Pedidos, mantendo os usuários."""
    execute_query("DELETE FROM pedidos")
    execute_query("DELETE FROM requisicoes")
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
has_numero_rc_column = check_column_exists('requisicoes', 'numero_rc')

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
            if not user.empty and check_password(user.iloc[0]['password'], password):
                st.session_state.logged_in = True
                st.session_state.username = user.iloc[0]['username']
                st.session_state.role = user.iloc[0]['role']
                st.rerun()
            else:
                st.error("Usuário ou senha incorretos.")


def registration_form():
    st.title("Registro de Novo Usuário")
    with st.form("registration_form"):
        new_username = st.text_input("Novo Usuário")
        new_password = st.text_input("Nova Senha", type="password")
        submitted = st.form_submit_button("Registrar")
        if submitted:
            if not new_username or not new_password:
                st.warning("Usuário e senha não podem estar em branco.")
                return

            # Verifica se é o primeiro usuário a se registrar
            users = fetch_data("SELECT id FROM users")
            role = "admin" if users.empty else "user"

            hashed_password = hash_password(new_password)
            if execute_query("INSERT INTO users (username, password, role) VALUES (?, ?, ?)",
                             (new_username, hashed_password, role)):
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
st.title("🚀 Sistema de Controle de RCs e Pedidos de Compra")

# Inicializa o estado da sessão para controlar a visibilidade dos formulários e IDs de edição
if 'edit_id' not in st.session_state:
    st.session_state.edit_id = None
if 'show_rc_form' not in st.session_state:
    st.session_state.show_rc_form = False
if 'pedido_edit_id' not in st.session_state:
    st.session_state.pedido_edit_id = None
if 'show_pedido_form' not in st.session_state:
    st.session_state.show_pedido_form = False
if 'rc_id_para_pedido' not in st.session_state:
    st.session_state.rc_id_para_pedido = None
if 'confirm_reset' not in st.session_state:
    st.session_state.confirm_reset = False
if 'confirm_pedido_delete' not in st.session_state:
    st.session_state.confirm_pedido_delete = None

tab_rcs, tab_pedidos = st.tabs(["Requisições (RCs)", "Pedidos de Compra"])

# ==============================================================================
# --- ABA DE REQUISIÇÕES (RCs) ---
# ==============================================================================
with tab_rcs:
    # --- BARRA LATERAL (SIDEBAR) ---
    with st.sidebar:
        st.write(f"Usuário: **{st.session_state.username}** ({st.session_state.role})")
        if st.button("Logout", use_container_width=True):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()

        st.header("Filtros de RCs")
        filtro_solicitante = st.text_input("Buscar por Solicitante")
        filtro_status_rc = st.selectbox("Filtrar RC por Status", ["", "Aberto", "Finalizado", "Cancelado"])
        col1, col2 = st.columns(2)
        with col1:
            filtro_data_inicio_rc = st.date_input("RC - Data de Início", value=None, key="rc_start_date")
        with col2:
            filtro_data_fim_rc = st.date_input("RC - Data de Fim", value=None, key="rc_end_date")

        st.header("Ações de RC")
        if st.button("➕ Adicionar Nova RC", use_container_width=True, type="primary"):
            st.session_state.edit_id = None
            st.session_state.show_rc_form = True
            st.rerun()

        if st.session_state.role == 'admin':
            st.header("Administração")
            with st.expander("Gerenciar Usuários"):
                all_users = fetch_data("SELECT id, username, role FROM users")
                st.dataframe(all_users, use_container_width=True, hide_index=True)

                selected_user = st.selectbox("Selecione um usuário para gerenciar",
                                             options=all_users['username'].tolist())

                if selected_user:
                    st.subheader(f"Gerenciando: {selected_user}")

                    # Redefinir senha
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

                    # Mudar papel
                    with st.form(f"change_role_{selected_user}"):
                        st.write("Mudar Papel (Role)")
                        current_role = all_users[all_users['username'] == selected_user]['role'].iloc[0]
                        new_role = st.selectbox("Novo Papel", ["user", "admin"],
                                                index=["user", "admin"].index(current_role))
                        if st.form_submit_button("Mudar Papel"):
                            num_admins = \
                            fetch_data("SELECT COUNT(id) as count FROM users WHERE role = 'admin'").iloc[0]['count']
                            if current_role == 'admin' and num_admins <= 1 and new_role == 'user':
                                st.error("Não é possível remover o último administrador.")
                            else:
                                if execute_query("UPDATE users SET role = ? WHERE username = ?",
                                                 (new_role, selected_user)):
                                    st.success(f"O papel de {selected_user} foi alterado para {new_role}.")
                                    # Se o admin mudou o próprio papel, deslogar para segurança
                                    if selected_user == st.session_state.username:
                                        st.warning("Seu papel foi alterado. Você será deslogado.")
                                        time.sleep(2)
                                        for key in list(st.session_state.keys()):
                                            del st.session_state[key]
                                        st.rerun()

                    # Excluir usuário
                    if selected_user != st.session_state.username:
                        if st.button(f"Excluir {selected_user}", use_container_width=True, type="secondary"):
                            if execute_query("DELETE FROM users WHERE username = ?", (selected_user,)):
                                st.success(f"Usuário {selected_user} excluído com sucesso.")
                                st.rerun()
                    else:
                        st.info("Você não pode excluir sua própria conta.")

            with st.expander("⚠️ Opções Perigosas"):
                if st.button("Zerar Dados de RCs e Pedidos", use_container_width=True):
                    st.session_state.confirm_reset = True

                if st.session_state.confirm_reset:
                    st.warning("Tem certeza que deseja apagar TODOS os dados de RCs e Pedidos?")
                    c1, c2 = st.columns(2)
                    with c1:
                        if st.button("Sim, apagar DADOS", use_container_width=True, type="primary"):
                            reset_database()
                            st.session_state.confirm_reset = False
                            st.success("Dados de RCs e Pedidos zerados!")
                            time.sleep(1)
                            st.rerun()
                    with c2:
                        if st.button("Cancelar", use_container_width=True):
                            st.session_state.confirm_reset = False
                            st.rerun()

    # --- FORMULÁRIO DE ADIÇÃO/EDIÇÃO DE RC ---
    if st.session_state.show_rc_form:
        form_title = "Editar RC" if st.session_state.edit_id else "Adicionar Nova RC"
        rc_data = {}
        if st.session_state.edit_id:
            rc_data = fetch_data("SELECT * FROM requisicoes WHERE id = ?", (st.session_state.edit_id,)).iloc[
                0].to_dict()

        with st.form(key="rc_form"):
            st.subheader(form_title)

            if not has_numero_rc_column:
                st.warning(
                    "A coluna 'numero_rc' não foi encontrada. Para habilitar a edição do Nº da RC, zere o banco de dados (opção de admin) e reinicie o app.")

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
                numero_rc = st.text_input("Nº RC (Editável)", value=suggested_rc_num, disabled=not has_numero_rc_column)
                solicitante = st.text_input("Solicitante", value=rc_data.get("solicitante", st.session_state.username))
            with c2:
                centro_custo = st.text_input("Centro de Custo", value=rc_data.get("centro_custo", ""))
                valor = st.number_input("Valor (R$)", value=rc_data.get("valor", 0.0), format="%.2f")
            with c3:
                fornecedor = st.text_input("Fornecedor", value=rc_data.get("fornecedor", ""))
                tipo = st.selectbox("Tipo", ["Material", "Serviço", "Outro"],
                                    index=["Material", "Serviço", "Outro"].index(rc_data.get("tipo", "Material")))

            status = st.selectbox("Status", ["Aberto", "Finalizado", "Cancelado"],
                                  index=["Aberto", "Finalizado", "Cancelado"].index(rc_data.get("status", "Aberto")))
            descricao = st.text_area("Descrição do Material ou Serviço", value=rc_data.get("descricao", ""))
            observacoes = st.text_area("Observações (opcional)", value=rc_data.get("observacoes", ""))

            submitted = st.form_submit_button("Salvar RC")
            if submitted:
                if not all([numero_rc, solicitante, centro_custo, descricao, valor > 0]):
                    st.warning(
                        "Por favor, preencha todos os campos obrigatórios: Nº RC, Solicitante, Centro de Custo, Descrição e Valor.")
                else:
                    if st.session_state.edit_id:
                        params_db = (
                        numero_rc, solicitante, centro_custo, tipo, fornecedor, descricao, valor, status, observacoes,
                        st.session_state.edit_id)
                        q = "UPDATE requisicoes SET numero_rc=?, solicitante=?, centro_custo=?, tipo=?, fornecedor=?, descricao=?, valor=?, status=?, observacoes=? WHERE id=?"
                        if execute_query(q, params_db):
                            st.success(f"RC Nº {numero_rc} atualizada com sucesso!")
                            st.session_state.show_rc_form = False
                            st.session_state.edit_id = None
                            st.rerun()
                    else:
                        data_criacao = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        params_db = (
                        numero_rc, data_criacao, solicitante, centro_custo, tipo, fornecedor, descricao, valor, status,
                        observacoes)
                        q = "INSERT INTO requisicoes (numero_rc, data_criacao, solicitante, centro_custo, tipo, fornecedor, descricao, valor, status, observacoes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                        if execute_query(q, params_db):
                            st.success("Nova RC adicionada com sucesso!")
                            st.session_state.show_rc_form = False
                            st.session_state.edit_id = None
                            st.rerun()
        if st.button("Cancelar"):
            st.session_state.show_rc_form = False
            st.session_state.edit_id = None
            st.rerun()

    # --- TABELA DE DADOS DE RC ---
    if not st.session_state.show_rc_form and not st.session_state.show_pedido_form:
        st.header("Requisições Cadastradas")
        query_rc = "SELECT * FROM requisicoes WHERE 1=1"
        params_rc = []
        if filtro_solicitante:
            query_rc += " AND solicitante LIKE ?"
            params_rc.append(f"%{filtro_solicitante}%")
        if filtro_status_rc:
            query_rc += " AND status = ?"
            params_rc.append(filtro_status_rc)
        if filtro_data_inicio_rc:
            query_rc += " AND date(data_criacao) >= ?"
            params_rc.append(filtro_data_inicio_rc.strftime('%Y-%m-%d'))
        if filtro_data_fim_rc:
            query_rc += " AND date(data_criacao) <= ?"
            params_rc.append(filtro_data_fim_rc.strftime('%Y-%m-%d'))
        query_rc += " ORDER BY id DESC"

        df_rc = fetch_data(query_rc, tuple(params_rc))

        if df_rc.empty:
            st.info("Nenhuma requisição encontrada com os filtros aplicados.")
        else:
            df_display_rc = df_rc.copy()
            df_display_rc['valor'] = df_display_rc['valor'].apply(format_currency)
            df_display_rc['data_criacao'] = pd.to_datetime(df_display_rc['data_criacao']).dt.strftime('%d/%m/%Y')

            df_display_rc.rename(
                columns={'id': 'ID Interno', 'numero_rc': 'Nº RC', 'data_criacao': 'Data', 'solicitante': 'Solicitante',
                         'centro_custo': 'Centro de Custo', 'tipo': 'Tipo', 'fornecedor': 'Fornecedor',
                         'descricao': 'Descrição', 'valor': 'Valor', 'status': 'Status', 'observacoes': 'Observações'},
                inplace=True)

            display_columns = ['Nº RC', 'Data', 'Solicitante', 'Centro de Custo', 'Tipo', 'Fornecedor', 'Descrição',
                               'Valor', 'Status', 'Observações']

            final_columns_to_display = [col for col in display_columns if col in df_display_rc.columns]
            if 'Nº RC' not in final_columns_to_display:
                final_columns_to_display.insert(0, 'ID Interno')

            st.dataframe(df_display_rc[final_columns_to_display], use_container_width=True, hide_index=True)

            st.subheader("Operações com a RC Selecionada")
            rc_ids = df_rc['id'].tolist()

            if 'numero_rc' in df_rc.columns:
                rc_id_to_num_map = pd.Series(df_rc.numero_rc.values, index=df_rc.id).to_dict()
            else:
                rc_id_to_num_map = pd.Series(df_rc.id.values, index=df_rc.id).to_dict()

            selected_id_rc = st.selectbox("Selecione uma RC", options=rc_ids,
                                          format_func=lambda x: f"RC {rc_id_to_num_map.get(x, x)}", key="select_rc")

            if selected_id_rc:
                col_edit, col_delete, col_gerar_pedido, col_space = st.columns([1.5, 1.5, 2, 5])
                with col_edit:
                    if st.button("✏️ Editar RC", use_container_width=True, disabled=not has_numero_rc_column):
                        st.session_state.edit_id = selected_id_rc
                        st.session_state.show_rc_form = True
                        st.rerun()
                with col_delete:
                    if st.button("🗑️ Excluir RC", use_container_width=True, type="secondary"):
                        if execute_query("DELETE FROM requisicoes WHERE id = ?", (selected_id_rc,)):
                            st.success(f"RC {rc_id_to_num_map.get(selected_id_rc)} excluída com sucesso!")
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

# ==============================================================================
# --- ABA DE PEDIDOS DE COMPRA ---
# ==============================================================================
with tab_pedidos:
    st.header("Pedidos de Compra Gerados")

    if has_numero_rc_column:
        query_pedidos = "SELECT p.id, p.rc_id, r.numero_rc, p.data_pedido, p.numero_pedido, p.previsao_entrega, p.status_pedido, r.solicitante, p.observacoes_pedido FROM pedidos p JOIN requisicoes r ON p.rc_id = r.id ORDER BY p.id DESC"
    else:
        query_pedidos = "SELECT p.id, p.rc_id, p.data_pedido, p.numero_pedido, p.previsao_entrega, p.status_pedido, r.solicitante, p.observacoes_pedido FROM pedidos p JOIN requisicoes r ON p.rc_id = r.id ORDER BY p.id DESC"

    df_pedidos = fetch_data(query_pedidos)

    if df_pedidos.empty:
        st.info("Nenhum pedido de compra encontrado.")
    else:
        df_display_pedidos = df_pedidos.copy()
        df_display_pedidos['data_pedido'] = pd.to_datetime(df_display_pedidos['data_pedido']).dt.strftime('%d/%m/%Y')
        df_display_pedidos['previsao_entrega'] = pd.to_datetime(df_display_pedidos['previsao_entrega'],
                                                                errors='coerce').dt.strftime('%d/%m/%Y')

        rename_dict = {
            'id': 'Nº Pedido',
            'data_pedido': 'Data do Pedido',
            'numero_pedido': 'Nº Pedido Fornecedor',
            'previsao_entrega': 'Previsão Entrega',
            'status_pedido': 'Status do Pedido',
            'solicitante': 'Solicitante RC',
            'observacoes_pedido': 'Obs. Pedido'
        }
        if has_numero_rc_column:
            rename_dict['numero_rc'] = 'Nº RC Origem'
        else:
            rename_dict['rc_id'] = 'Nº RC Origem'

        df_display_pedidos.rename(columns=rename_dict, inplace=True)

        display_columns_pedidos = ['Nº Pedido', 'Nº RC Origem', 'Data do Pedido', 'Nº Pedido Fornecedor',
                                   'Previsão Entrega', 'Status do Pedido', 'Solicitante RC', 'Obs. Pedido']
        st.dataframe(df_display_pedidos[display_columns_pedidos], use_container_width=True, hide_index=True)

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
                            st.success(f"Pedido Nº {selected_id_pedido} excluído com sucesso!")
                            st.session_state.confirm_pedido_delete = None
                            time.sleep(1)
                            st.rerun()
                with c2:
                    if st.button("Cancelar exclusão", use_container_width=True):
                        st.session_state.confirm_pedido_delete = None
                        st.rerun()

# ==============================================================================
# --- FORMULÁRIO DE GERAÇÃO/EDIÇÃO DE PEDIDO ---
# ==============================================================================
if st.session_state.show_pedido_form:
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
            if has_numero_rc_column:
                rc_num_df = fetch_data("SELECT numero_rc FROM requisicoes WHERE id=?", (rc_origem_id,))
                if not rc_num_df.empty:
                    rc_num = rc_num_df.iloc[0]['numero_rc']
                    st.info(f"Este pedido é vinculado à RC Nº {rc_num}")
            else:
                st.info(f"Este pedido é vinculado à RC de ID Interno {rc_origem_id}")

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
                # UPDATE
                params_db_pedido = (
                numero_pedido, previsao_entrega_str, status_pedido, observacoes_pedido, st.session_state.pedido_edit_id)
                q_pedido = "UPDATE pedidos SET numero_pedido=?, previsao_entrega=?, status_pedido=?, observacoes_pedido=? WHERE id=?"
                if execute_query(q_pedido, params_db_pedido):
                    st.success(f"Pedido Nº {st.session_state.pedido_edit_id} atualizado com sucesso!")
                    st.session_state.show_pedido_form = False
                    st.session_state.pedido_edit_id = None
                    st.rerun()
            else:
                # INSERT
                data_pedido = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                params_db_pedido = (
                rc_origem_id, data_pedido, numero_pedido, previsao_entrega_str, status_pedido, observacoes_pedido)
                q_pedido = "INSERT INTO pedidos (rc_id, data_pedido, numero_pedido, previsao_entrega, status_pedido, observacoes_pedido) VALUES (?, ?, ?, ?, ?, ?)"
                if execute_query(q_pedido, params_db_pedido):
                    st.success("Novo pedido de compra gerado com sucesso!")
                    st.session_state.show_pedido_form = False
                    st.session_state.rc_id_para_pedido = None
                    st.rerun()

    if st.button("Cancelar"):
        st.session_state.show_pedido_form = False
        st.session_state.rc_id_para_pedido = None
        st.session_state.pedido_edit_id = None
        st.rerun()
