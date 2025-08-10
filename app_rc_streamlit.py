import streamlit as st
import pandas as pd
import os
import time
import hashlib
from datetime import datetime, date
from PIL import Image
import logging
import io
from openpyxl.styles import Font, Alignment, Border, Side, PatternFill
from openpyxl.utils import get_column_letter
from sqlalchemy import create_engine, text
from supabase import create_client

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(page_title="Controle de Compras", layout="wide")

# --- CONFIGURAÇÃO DE LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='app_log.log',
    filemode='a'
)

# --- FUNÇÕES DE BANCO DE DADOS (SUPABASE) ---
def get_db_connection():
    """Cria uma conexão segura com o Supabase via SQLAlchemy."""
    try:
        url = st.secrets["SUPABASE_URL"]
        key = st.secrets["SUPABASE_KEY"]
        host = url.split("//")[1]
        engine = create_engine(f"postgresql://postgres:{key}@{host}/postgres")
        return engine
    except Exception as e:
        st.error(f"Erro ao conectar ao banco de dados: {e}")
        st.stop()

def fetch_data(query, params=None):
    """Executa uma query de leitura e retorna um DataFrame."""
    try:
        engine = get_db_connection()
        with engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df
    except Exception as e:
        st.error(f"Erro ao buscar dados: {e}")
        return pd.DataFrame()

def execute_query(query, params=None):
    """Executa uma query de escrita (INSERT, UPDATE, DELETE)."""
    try:
        engine = get_db_connection()
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(text(query), params or {})
        return True
    except Exception as e:
        st.error(f"Erro no banco de dados: {e}")
        return False

# --- SUPABASE STORAGE CLIENT ---
def get_supabase_client():
    """Retorna um cliente do Supabase para Storage."""
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

def upload_file_to_supabase(file_obj, file_name):
    """
    Faz upload de um arquivo para o bucket 'uploads' no Supabase.
    Retorna a URL pública do arquivo ou None em caso de erro.
    """
    try:
        client = get_supabase_client()
        file_bytes = file_obj.read()
        bucket_name = "uploads"
        path = f"{int(time.time())}_{file_name}"
        client.storage.from_(bucket_name).upload(path, file_bytes)
        public_url = client.storage.from_(bucket_name).get_public_url(path)
        return public_url
    except Exception as e:
        st.error(f"Erro ao fazer upload para Supabase Storage: {e}")
        return None

# --- FUNÇÕES AUXILIARES ---
def format_currency(value):
    if isinstance(value, (int, float)):
        return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return value

def safe_strptime(date_string, fmt):
    try:
        return datetime.strptime(date_string, fmt)
    except (ValueError, TypeError):
        return None

def to_excel(df, title="Relatório"):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name=title)
        workbook = writer.book
        worksheet = writer.sheets[title]
        header_fill = PatternFill(start_color="4F81BD", end_color="4F81BD", fill_type="solid")
        header_font = Font(color="FFFFFF", bold=True)
        border = Border(left=Side(style='thin'), right=Side(style='thin'),
                        top=Side(style='thin'), bottom=Side(style='thin'))
        alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)

        for col in range(1, len(df.columns) + 1):
            cell = worksheet.cell(row=1, column=col)
            cell.fill = header_fill
            cell.font = header_font
            cell.border = border
            cell.alignment = alignment
            column_letter = get_column_letter(col)
            column_len = max(df.iloc[:, col-1].astype(str).map(len).max(), len(df.columns[col-1])) + 2
            worksheet.column_dimensions[column_letter].width = min(column_len, 30)

        for row in range(2, len(df) + 2):
            for col in range(1, len(df.columns) + 1):
                cell = worksheet.cell(row=row, column=col)
                cell.border = border
                cell.alignment = Alignment(horizontal='left', vertical='center')
                if 'valor' in df.columns[col-1].lower() or 'total' in df.columns[col-1].lower():
                    cell.number_format = 'R$ #,##0.00'

        worksheet.freeze_panes = 'A2'
        worksheet.auto_filter.ref = worksheet.dimensions
    return output.getvalue()

# --- FUNÇÕES DE AUTENTICAÇÃO ---
def hash_password(password, salt=None):
    if salt is None:
        salt = os.urandom(16)
    hashed_password = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt, 100000)
    return hashed_password, salt

def check_password(stored_password, salt, provided_password):
    if salt is None:
        return False
    return stored_password == hashlib.pbkdf2_hmac('sha256', provided_password.encode('utf-8'), salt, 100000)

# --- LÓGICA DE LOGIN ---
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
    st.session_state.username = ""
    st.session_state.role = ""

def login_form():
    st.title("🔐 Login do Sistema")
    with st.form("login_form"):
        username = st.text_input("Usuário")
        password = st.text_input("Senha", type="password")
        submitted = st.form_submit_button("Entrar")
        if submitted:
            user = fetch_data("SELECT * FROM users WHERE username = :username", {"username": username})
            if not user.empty:
                user_data = user.iloc[0]
                if user_data['salt'] is None:
                    old_hash = hashlib.sha256(password.encode()).hexdigest()
                    if user_data['password'] == old_hash:
                        st.info("Atualizando segurança da conta...")
                        new_hash, new_salt = hash_password(password)
                        execute_query(
                            "UPDATE users SET password = :pw, salt = :s WHERE username = :u",
                            {"pw": new_hash, "s": new_salt, "u": username}
                        )
                        st.session_state.logged_in = True
                        st.session_state.username = username
                        st.session_state.role = user_data['role']
                        st.rerun()
                    else:
                        st.error("Usuário ou senha incorretos.")
                elif user_data['status'] == 'pending':
                    st.warning("Sua conta está aguardando aprovação de um administrador.")
                elif check_password(user_data['password'], user_data['salt'], password):
                    st.session_state.logged_in = True
                    st.session_state.username = user_data['username']
                    st.session_state.role = user_data['role']
                    st.rerun()
                else:
                    st.error("Usuário ou senha incorretos.")
            else:
                st.error("Usuário ou senha incorretos.")

def registration_form():
    st.title("📝 Registro de Novo Usuário")
    with st.form("registration_form"):
        new_username = st.text_input("Novo Usuário")
        new_password = st.text_input("Nova Senha", type="password")
        is_gestor = st.checkbox("Sou um gestor (requer aprovação)")
        submitted = st.form_submit_button("Registrar")
        if submitted:
            if not new_username or not new_password:
                st.warning("Preencha todos os campos.")
                return
            users = fetch_data("SELECT id FROM users")
            role = "admin" if users.empty else "gestor" if is_gestor else "user"
            status = "active" if not is_gestor else "pending"
            hashed_pw, salt = hash_password(new_password)
            if execute_query(
                "INSERT INTO users (username, password, salt, role, status) VALUES (:u, :p, :s, :r, :st)",
                {"u": new_username, "p": hashed_pw, "s": salt, "r": role, "st": status}
            ):
                st.success(f"Usuário '{new_username}' registrado como '{role}' (status: {status})")
                time.sleep(2)
                st.session_state.page = "Login"
                st.rerun()

if not st.session_state.logged_in:
    if 'page' not in st.session_state:
        st.session_state.page = "Login"
    if st.session_state.page == "Login":
        login_form()
        if st.button("Não tem conta? Registre-se"):
            st.session_state.page = "Registro"
            st.rerun()
    else:
        registration_form()
        if st.button("Já tem conta? Faça login"):
            st.session_state.page = "Login"
            st.rerun()
    st.stop()

# --- APLICAÇÃO PRINCIPAL ---
st.title("🚀 Sistema de Controle de Compras")

# Inicializa variáveis de estado
for key in ['edit_id', 'show_rc_form', 'pedido_edit_id', 'show_pedido_form',
            'rc_id_para_pedido', 'confirm_reset', 'confirm_delete', 'show_demanda_form',
            'demanda_edit_id', 'demanda_id_para_rc', 'pedido_to_finalize']:
    if key not in st.session_state:
        st.session_state[key] = None

# --- SIDEBAR ---
with st.sidebar:
    st.write(f"👤 **{st.session_state.username}** ({st.session_state.role})")
    if st.button("Logout"):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()

    if st.session_state.role == 'admin':
        st.header("⚙️ Administração")
        with st.expander("Gerenciar Usuários"):
            pending = fetch_data("SELECT id, username, role FROM users WHERE status = 'pending'")
            if not pending.empty:
                st.subheader("Aprovações Pendentes")
                for _, user in pending.iterrows():
                    c1, c2, c3 = st.columns([2,1,1])
                    with c1: st.write(f"{user['username']} ({user['role']})")
                    with c2:
                        if st.button("✅ Aprovar", key=f"a_{user['id']}"):
                            execute_query("UPDATE users SET status = 'active' WHERE id = :id", {"id": user['id']})
                            st.rerun()
                    with c3:
                        if st.button("🗑️ Rejeitar", key=f"r_{user['id']}"):
                            execute_query("DELETE FROM users WHERE id = :id", {"id": user['id']})
                            st.rerun()
                st.markdown("---")
            users = fetch_data("SELECT id, username, role FROM users WHERE status = 'active'")
            st.dataframe(users, hide_index=True, use_container_width=True)
            selected_user = st.selectbox("Gerenciar usuário", options=users['username'].tolist() if not users.empty else [])
            if selected_user and not users.empty:
                with st.form("reset_pass"):
                    new_pass = st.text_input("Nova senha", type="password")
                    if st.form_submit_button("Alterar senha"):
                        if new_pass:
                            hp, sl = hash_password(new_pass)
                            execute_query("UPDATE users SET password = :p, salt = :s WHERE username = :u", {"p": hp, "s": sl, "u": selected_user})
                            st.toast("Senha alterada.")
                with st.form("change_role"):
                    current_role = users[users['username'] == selected_user]['role'].iloc[0]
                    new_role = st.selectbox("Novo papel", ["user", "gestor", "admin"], index=["user", "gestor", "admin"].index(current_role))
                    if st.form_submit_button("Mudar papel"):
                        num_admins = fetch_data("SELECT COUNT(*) as c FROM users WHERE role = 'admin' AND status = 'active'").iloc[0]['c']
                        if current_role == 'admin' and num_admins <= 1 and new_role != 'admin':
                            st.error("Precisa de pelo menos 1 admin.")
                        else:
                            execute_query("UPDATE users SET role = :r WHERE username = :u", {"r": new_role, "u": selected_user})
                            st.toast("Papel alterado.")
                            if selected_user == st.session_state.username:
                                time.sleep(1)
                                for key in st.session_state.keys(): del st.session_state[key]
                                st.rerun()
                if selected_user != st.session_state.username:
                    if st.button("Excluir", type="secondary"):
                        execute_query("DELETE FROM users WHERE username = :u", {"u": selected_user})
                        st.rerun()
                else:
                    st.info("Não pode excluir a si mesmo.")

        with st.expander("⚠️ Zerar Dados"):
            if st.button("Zerar Dados"):
                st.session_state.confirm_reset = True
            if st.session_state.confirm_reset:
                st.warning("Tem certeza?")
                c1, c2 = st.columns(2)
                with c1:
                    if st.button("Sim", type="primary"):
                        execute_query("DELETE FROM pedidos")
                        execute_query("DELETE FROM requisicoes")
                        execute_query("DELETE FROM demandas")
                        st.toast("Dados zerados!")
                        st.session_state.confirm_reset = False
                        st.rerun()
                with c2:
                    if st.button("Cancelar"):
                        st.session_state.confirm_reset = False
                        st.rerun()

# --- ABAS PRINCIPAIS ---
tab_dashboard, tab_demandas, tab_rcs, tab_pedidos_andamento, tab_pedidos_finalizados = st.tabs([
    "📊 Dashboard", "📝 Demandas", "🛒 RCs", "🚚 Pedidos", "✅ Finalizados"
])

# --- DASHBOARD ---
with tab_dashboard:
    st.header("Dashboard de Métricas")
    with st.expander("Filtros do Dashboard"):
        solicitantes_dash = fetch_data("SELECT DISTINCT solicitante_demanda FROM demandas")
        solicitante_list_dash = solicitantes_dash['solicitante_demanda'].tolist()
        filtro_solicitante_dash = st.multiselect("Filtrar por Solicitante da Demanda", options=solicitante_list_dash)

    where_clause_demanda = ""
    params_demanda_dash = {}
    if filtro_solicitante_dash:
        where_clause_demanda = f"AND d.solicitante_demanda = ANY(ARRAY[:solicitante]::text[])"
        params_demanda_dash = {"solicitante": filtro_solicitante_dash}

    query_total_gasto = f"""
        SELECT COALESCE(SUM(r.valor), 0) as total 
        FROM requisicoes r 
        JOIN demandas d ON r.demanda_id = d.id 
        WHERE r.status = 'Finalizado' {where_clause_demanda if filtro_solicitante_dash else ''}
    """
    total_rcs_finalizadas = fetch_data(query_total_gasto, params_demanda_dash).iloc[0]['total']

    demandas_abertas = fetch_data(f"SELECT COUNT(id) as count FROM demandas WHERE status_demanda = 'Aberta' {where_clause_demanda.replace('d.solicitante_demanda', 'solicitante_demanda') if filtro_solicitante_dash else ''}", params_demanda_dash).iloc[0]['count']

    query_rcs_abertas = f"""
        SELECT COUNT(r.id) as count 
        FROM requisicoes r 
        JOIN demandas d ON r.demanda_id = d.id 
        WHERE r.status = 'Aberto' {where_clause_demanda if filtro_solicitante_dash else ''}
    """
    rcs_abertas = fetch_data(query_rcs_abertas, params_demanda_dash).iloc[0]['count']

    query_pedidos_andamento = f"""
        SELECT COUNT(p.id) as count 
        FROM pedidos p 
        JOIN requisicoes r ON p.rc_id = r.id 
        JOIN demandas d ON r.demanda_id = d.id 
        WHERE p.status_pedido NOT IN ('Entregue', 'Cancelado') {where_clause_demanda if filtro_solicitante_dash else ''}
    """
    pedidos_andamento = fetch_data(query_pedidos_andamento, params_demanda_dash).iloc[0]['count']

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(label="Total Gasto (RCs Finalizadas)", value=format_currency(total_rcs_finalizadas))
    with col2:
        st.metric(label="Demandas Abertas", value=demandas_abertas)
    with col3:
        st.metric(label="RCs Abertas", value=rcs_abertas)
    with col4:
        st.metric(label="Pedidos em Andamento", value=pedidos_andamento)

# --- DEMANDAS ---
with tab_demandas:
    st.header("Demandas de Compras")
    with st.expander("➕ Adicionar Nova Demanda"):
        with st.form("demanda_form", clear_on_submit=True):
            descricao_necessidade = st.text_area("O que precisa comprar ou contratar? (Material ou Serviço)")
            uploaded_file = st.file_uploader("Anexar arquivo (imagem, PDF, Doc)", type=["png", "jpg", "jpeg", "pdf", "doc", "docx"])
            submitted = st.form_submit_button("Registrar Demanda")
            if submitted:
                if not descricao_necessidade:
                    st.warning("A descrição é obrigatória.")
                else:
                    anexo_url = None
                    if uploaded_file is not None:
                        anexo_url = upload_file_to_supabase(uploaded_file, uploaded_file.name)
                        if not anexo_url:
                            st.error("Falha ao fazer upload do arquivo. Demanda não foi salva.")
                            continue

                    params = (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), st.session_state.username, descricao_necessidade, anexo_url, 'Aberta')
                    if execute_query(
                        "INSERT INTO demandas (data_demanda, solicitante_demanda, descricao_necessidade, anexo_path, status_demanda) VALUES (:data, :solicitante, :descricao, :anexo, :status)",
                        {"data": params[0], "solicitante": params[1], "descricao": params[2], "anexo": anexo_url, "status": params[4]}
                    ):
                        st.toast("Nova demanda registrada com sucesso!")
                        st.rerun()

    with st.expander("🔍 Filtros e Busca"):
        filtro_busca_demanda = st.text_input("Buscar na descrição da demanda")
        solicitantes = fetch_data("SELECT DISTINCT solicitante_demanda FROM demandas")
        solicitante_list = solicitantes['solicitante_demanda'].tolist()
        filtro_solicitante = st.multiselect("Filtrar por Solicitante", options=solicitante_list)

    query_demanda = "SELECT * FROM demandas WHERE 1=1"
    params_demanda = {}
    if filtro_busca_demanda:
        query_demanda += " AND descricao_necessidade ILIKE :busca"
        params_demanda["busca"] = f"%{filtro_busca_demanda}%"
    if filtro_solicitante:
        query_demanda += " AND solicitante_demanda = ANY(ARRAY[:solicitante]::text[])"
        params_demanda["solicitante"] = filtro_solicitante
    query_demanda += " ORDER BY id DESC"

    df_demandas = fetch_data(query_demanda, params_demanda)
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
                st.write(f"**Data:** {safe_strptime(row['data_demanda'], '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y') if safe_strptime(row['data_demanda'], '%Y-%m-%d %H:%M:%S') else 'Data inválida'}")
                st.info(f"**Necessidade:** {row['descricao_necessidade']}")
            with col2:
                anexo_url = row['anexo_path']
                if anexo_url:
                    file_name = anexo_url.split("/")[-1]
                    file_extension = os.path.splitext(file_name)[1].lower()
                    if file_extension in ['.png', '.jpg', '.jpeg', '.gif']:
                        try:
                            st.image(anexo_url, caption="Anexo", use_container_width=True)
                        except Exception:
                            st.warning("Imagem não carregada.")
                    else:
                        st.markdown(f"[📎 Baixar Anexo: {file_name}]({anexo_url})", unsafe_allow_html=True)

            c1, c2, c3 = st.columns([1,1,3])
            with c1:
                if row['solicitante_demanda'] == st.session_state.username or st.session_state.role in ['admin', 'gestor']:
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
                    if st.button("Sim, excluir demanda", key=f"confirm_del_demanda_{row['id']}", use_container_width=True):
                        if execute_query("DELETE FROM demandas WHERE id = :id", {"id": row['id']}):
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

# --- RCs ---
with tab_rcs:
    st.header("Requisições de Compra (RCs)")
    if st.session_state.role != 'gestor':
        if st.button("➕ Adicionar Nova RC", key="add_rc"):
            st.session_state.show_rc_form = True
            st.session_state.edit_id = None
            st.session_state.demanda_id_para_rc = None
            st.rerun()

    with st.expander("Filtros e Relatórios de RCs"):
        filtro_status_rc = st.multiselect("Filtrar por Status da RC", options=["Aberto", "Finalizado", "Cancelado"])
        c1, c2 = st.columns(2)
        with c1:
            filtro_data_inicio_rc = st.date_input("Data de Início da RC", value=None)
        with c2:
            filtro_data_fim_rc = st.date_input("Data de Fim da RC", value=None)

    query_rc = "SELECT * FROM requisicoes WHERE 1=1"
    params_rc = {}
    if filtro_status_rc:
        query_rc += " AND status = ANY(ARRAY[:status]::text[])"
        params_rc["status"] = filtro_status_rc
    if filtro_data_inicio_rc:
        query_rc += " AND data_criacao >= :data_inicio"
        params_rc["data_inicio"] = str(filtro_data_inicio_rc)
    if filtro_data_fim_rc:
        query_rc += " AND data_criacao <= :data_fim"
        params_rc["data_fim"] = str(filtro_data_fim_rc)
    query_rc += " ORDER BY id DESC"

    df_rc = fetch_data(query_rc, params_rc)
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
            selected_id_rc = st.selectbox("Selecione uma RC", options=rc_ids, format_func=lambda x: f"RC Nº {x}", key="select_rc")
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
                    pedidos_associados = fetch_data("SELECT id FROM pedidos WHERE rc_id = :rc_id", {"rc_id": selected_id_rc})
                    if not pedidos_associados.empty:
                        st.error(f"Não é possível excluir a RC Nº {selected_id_rc}, pois ela possui pedidos associados.")
                        if st.button("Ok, entendi", key=f"ack_del_rc_{selected_id_rc}"):
                            st.session_state.confirm_delete = {}
                            st.rerun()
                    else:
                        st.warning(f"Tem certeza que deseja excluir a RC Nº {selected_id_rc}?")
                        del_c1, del_c2 = st.columns(2)
                        with del_c1:
                            if st.button("Sim, excluir RC", key=f"confirm_del_rc_{selected_id_rc}", use_container_width=True):
                                if execute_query("DELETE FROM requisicoes WHERE id = :id", {"id": selected_id_rc}):
                                    st.toast(f"RC Nº {selected_id_rc} excluída!", icon="🗑️")
                                    st.session_state.confirm_delete = {}
                                    st.rerun()
                        with del_c2:
                            if st.button("Cancelar", key=f"cancel_del_rc_{selected_id_rc}", use_container_width=True):
                                st.session_state.confirm_delete = {}
                                st.rerun()

                selected_rc_details = df_rc[df_rc['id'] == selected_id_rc].iloc[0]
                if selected_rc_details['status'] == 'Finalizado':
                    pedido_existente = fetch_data("SELECT id FROM pedidos WHERE rc_id = :rc_id", {"rc_id": selected_id_rc})
                    with col_gerar_pedido:
                        if pedido_existente.empty:
                            if st.button("🛒 Gerar Pedido", use_container_width=True):
                                st.session_state.rc_id_para_pedido = selected_id_rc
                                st.session_state.show_pedido_form = True
                                st.session_state.pedido_edit_id = None
                                st.rerun()
                        else:
                            st.info(f"Pedido já existe.")

# --- PEDIDOS EM ANDAMENTO ---
with tab_pedidos_andamento:
    st.header("Pedidos de Compra em Andamento")
    with st.expander("Filtros e Relatórios de Pedidos"):
        filtro_status_pedido = st.multiselect("Filtrar por Status do Pedido", options=["Aguardando Entrega", "Entregue Parcialmente", "Atrasado", "Cancelado"])
        c1, c2 = st.columns(2)
        with c1:
            filtro_data_inicio_pedido = st.date_input("Data de Início do Pedido", value=None)
        with c2:
            filtro_data_fim_pedido = st.date_input("Data de Fim do Pedido", value=None)

    query_pedidos = "SELECT p.id, p.rc_id, r.numero_rc, p.data_pedido, p.numero_pedido, p.previsao_entrega, p.status_pedido, r.solicitante, p.observacoes_pedido FROM requisicoes r JOIN pedidos p ON r.id = p.rc_id WHERE p.status_pedido != 'Entregue'"
    params_pedidos = {}
    if filtro_status_pedido:
        query_pedidos += " AND p.status_pedido = ANY(ARRAY[:status]::text[])"
        params_pedidos["status"] = filtro_status_pedido
    if filtro_data_inicio_pedido:
        query_pedidos += " AND p.data_pedido >= :data_inicio"
        params_pedidos["data_inicio"] = str(filtro_data_inicio_pedido)
    if filtro_data_fim_pedido:
        query_pedidos += " AND p.data_pedido <= :data_fim"
        params_pedidos["data_fim"] = str(filtro_data_fim_pedido)
    query_pedidos += " ORDER BY p.id DESC"

    df_pedidos = fetch_data(query_pedidos, params_pedidos)
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
            selected_id_pedido = st.selectbox("Selecione um Pedido", options=pedido_ids, format_func=lambda x: f"Pedido Nº {x}", key="select_pedido")
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
                            if execute_query("DELETE FROM pedidos WHERE id = :id", {"id": selected_id_pedido}):
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
                            if execute_query("UPDATE pedidos SET status_pedido = 'Entregue' WHERE id = :id", {"id": selected_id_pedido}):
                                rc_info = fetch_data("SELECT demanda_id FROM requisicoes WHERE id = (SELECT rc_id FROM pedidos WHERE id = :id)", {"id": selected_id_pedido})
                                if not rc_info.empty and rc_info.iloc[0]['demanda_id']:
                                    execute_query("UPDATE demandas SET status_demanda = 'Finalizada' WHERE id = :id", {"id": rc_info.iloc[0]['demanda_id']})
                                st.toast(f"Pedido Nº {selected_id_pedido} finalizado com sucesso!")
                                del st.session_state.pedido_to_finalize
                                st.rerun()
                    with c2:
                        if st.button("Cancelar", use_container_width=True):
                            del st.session_state.pedido_to_finalize
                            st.rerun()

# --- PEDIDOS FINALIZADOS ---
with tab_pedidos_finalizados:
    st.header("Pedidos de Compra Finalizados")
    query_finalizados = "SELECT p.id, p.rc_id, r.numero_rc, p.data_pedido, p.numero_pedido, p.previsao_entrega, p.status_pedido, r.solicitante, p.observacoes_pedido FROM requisicoes r JOIN pedidos p ON r.id = p.rc_id WHERE p.status_pedido = 'Entregue' ORDER BY p.id DESC"
    df_finalizados = fetch_data(query_finalizados)
    if df_finalizados.empty:
        st.info("Nenhum pedido finalizado encontrado.")
    else:
        st.download_button(
            label="📥 Exportar Pedidos Finalizados para Excel",
            data=to_excel(df_finalizados, "Pedidos Finalizados"),
            file_name='pedidos_finalizados.xlsx',
            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        )
        st.dataframe(df_finalizados, use_container_width=True, hide_index=True)