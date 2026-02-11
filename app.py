import os
import shutil
import sqlite3
from datetime import date, datetime

import pandas as pd
import streamlit as st
import io

# ----------------------------
# CONFIG
# ----------------------------
st.set_page_config(page_title="Hotel - Manutenção Diária", page_icon="🛠️", layout="wide")

DB_PATH = "manutencao_hotel.db"
STATUSES = ["OK", "Problema", "N/A"]


# ----------------------------
# HELPERS
# ----------------------------
def room_code(floor: int, apt: int) -> str:
    return f"{floor:02d}{apt:02d}"

def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def fetch_pendencies_open(date_from: date, date_to: date, floor: int | None):
    conn = get_conn()
    query = """
        SELECT
            r.id AS report_id,
            r.report_date,
            r.room_code,
            r.floor,
            r.apt,
            r.technician,
            r.created_at,
            ri.id AS report_item_id,
            ri.item,
            ri.status,
            COALESCE(ri.note, '') AS note
        FROM reports r
        JOIN report_items ri ON ri.report_id = r.id
        WHERE r.report_date BETWEEN ? AND ?
          AND ri.status = 'Problema'
          AND ri.resolved_at IS NULL
    """
    params = [date_from.isoformat(), date_to.isoformat()]

    if floor is not None:
        query += " AND r.floor = ?"
        params.append(floor)

    query += " ORDER BY r.report_date DESC, r.room_code ASC, r.id DESC;"

    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

def resolve_pendency(report_item_id: int, resolved_by: str, resolution_note: str):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        UPDATE report_items
        SET
          resolved_at = ?,
          resolved_by = ?,
          resolution_note = ?
        WHERE id = ?
          AND status = 'Problema'
          AND resolved_at IS NULL;
    """, (
        datetime.now().isoformat(timespec="seconds"),
        resolved_by.strip(),
        (resolution_note.strip() or None),
        report_item_id
    ))

    conn.commit()
    conn.close()
    
def fetch_resolved(date_from: date, date_to: date, floor: int | None):
        conn = get_conn()
        query = """
            SELECT
                r.id AS report_id,
                r.report_date,
                r.room_code,
                r.floor,
                r.apt,
                r.technician,
                r.created_at,
                ri.item,
                ri.status,
                COALESCE(ri.note, '') AS note,
                ri.resolved_at,
                COALESCE(ri.resolved_by, '') AS resolved_by,
                COALESCE(ri.resolution_note, '') AS resolution_note
            FROM reports r
            JOIN report_items ri ON ri.report_id = r.id
            WHERE r.report_date BETWEEN ? AND ?
            AND ri.status = 'Problema'
            AND ri.resolved_at IS NOT NULL
        """
        params = [date_from.isoformat(), date_to.isoformat()]

        if floor is not None:
            query += " AND r.floor = ?"
            params.append(floor)

        query += " ORDER BY ri.resolved_at DESC, r.room_code ASC;"

        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        return df

GM_STATUSES = ["Aberto", "Em andamento", "Resolvido"]
def insert_general_maintenance(maint_date: date, place: str, description: str, status: str, technician: str, note: str):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO general_maintenance
        (maint_date, place, description, status, technician, note, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        maint_date.isoformat(),
        place.strip(),
        description.strip(),
        status,
        technician.strip(),
        (note.strip() or None),
        datetime.now().isoformat(timespec="seconds"),
    ))

    conn.commit()
    conn.close()


def fetch_general_maintenance(date_from: date, date_to: date, status: str | None, search: str | None):
    conn = get_conn()

    q = """
        SELECT
            id,
            maint_date,
            place,
            description,
            status,
            technician,
            COALESCE(note, '') AS note,
            created_at,
            COALESCE(resolved_at, '') AS resolved_at,
            COALESCE(resolved_by, '') AS resolved_by,
            COALESCE(resolution_note, '') AS resolution_note
        FROM general_maintenance
        WHERE maint_date BETWEEN ? AND ?
    """
    params = [date_from.isoformat(), date_to.isoformat()]

    if status:
        q += " AND status = ?"
        params.append(status)

    if search:
        q += " AND (LOWER(place) LIKE ? OR LOWER(description) LIKE ? OR LOWER(technician) LIKE ?)"
        s = f"%{search.lower().strip()}%"
        params += [s, s, s]

    q += " ORDER BY maint_date DESC, id DESC;"

    df = pd.read_sql_query(q, conn, params=params)
    conn.close()
    return df


def resolve_general_maintenance(gm_id: int, resolved_by: str, resolution_note: str):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        UPDATE general_maintenance
        SET
          status = 'Resolvido',
          resolved_at = ?,
          resolved_by = ?,
          resolution_note = ?
        WHERE id = ?;
    """, (
        datetime.now().isoformat(timespec="seconds"),
        resolved_by.strip(),
        (resolution_note.strip() or None),
        gm_id
    ))

    conn.commit()
    conn.close()

def export_unified_xlsx(df_apts: pd.DataFrame, df_resolved: pd.DataFrame, df_general: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        # Garantir que sempre exista a aba, mesmo vazia
        (df_apts if not df_apts.empty else pd.DataFrame()).to_excel(writer, index=False, sheet_name="Aptos")
        (df_resolved if not df_resolved.empty else pd.DataFrame()).to_excel(writer, index=False, sheet_name="Resolvidas Aptos")
        (df_general if not df_general.empty else pd.DataFrame()).to_excel(writer, index=False, sheet_name="Manutenção Geral")
    return output.getvalue()

# ----------------------------
# MIGRATIONS (produção)
# ----------------------------
def backup_db():
    if not os.path.exists(DB_PATH):
        return
    os.makedirs("backups", exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    shutil.copy2(DB_PATH, f"backups/manutencao_hotel_{ts}.db")


def ensure_schema_meta(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS schema_meta (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            version INTEGER NOT NULL
        );
    """)
    cur.execute("INSERT OR IGNORE INTO schema_meta (id, version) VALUES (1, 1);")


def get_schema_version(cur) -> int:
    cur.execute("SELECT version FROM schema_meta WHERE id = 1;")
    return int(cur.fetchone()[0])


def set_schema_version(cur, v: int):
    cur.execute("UPDATE schema_meta SET version = ? WHERE id = 1;", (v,))


def table_has_column(cur, table: str, column: str) -> bool:
    cur.execute(f"PRAGMA table_info({table});")
    return any(row[1] == column for row in cur.fetchall())


def migrate_if_needed(cur):
    """
    v1: reports com 'room' (1..216) e report_items com 'item' texto
    v2: adiciona floor, apt, room_code e converte room -> floor/apt/room_code
    v3: cria maintenance_items e adiciona item_id no report_items (itens cadastráveis)
    v4: resolução das pendencias
    """
    ensure_schema_meta(cur)
    v = get_schema_version(cur)

    # --- v1 -> v2
    if v < 2:
        if not table_has_column(cur, "reports", "floor"):
            cur.execute("ALTER TABLE reports ADD COLUMN floor INTEGER;")
        if not table_has_column(cur, "reports", "apt"):
            cur.execute("ALTER TABLE reports ADD COLUMN apt INTEGER;")
        if not table_has_column(cur, "reports", "room_code"):
            cur.execute("ALTER TABLE reports ADD COLUMN room_code TEXT;")

        if table_has_column(cur, "reports", "room"):
            cur.execute("""
                UPDATE reports
                SET
                  floor = ((room - 1) / 18) + 1,
                  apt   = ((room - 1) % 18) + 1
                WHERE floor IS NULL OR apt IS NULL;
            """)
            cur.execute("""
                UPDATE reports
                SET room_code = printf('%02d%02d', floor, apt)
                WHERE room_code IS NULL OR room_code = '';
            """)

        cur.execute("CREATE INDEX IF NOT EXISTS idx_reports_date ON reports(report_date);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_reports_roomcode ON reports(room_code);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_reports_floor_apt ON reports(floor, apt);")

        set_schema_version(cur, 2)
        v = 2

    # --- v2 -> v3 (itens cadastráveis)
    if v < 3:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS maintenance_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE COLLATE NOCASE,
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            );
        """)

        if not table_has_column(cur, "report_items", "item_id"):
            cur.execute("ALTER TABLE report_items ADD COLUMN item_id INTEGER;")

        cur.execute("CREATE INDEX IF NOT EXISTS idx_items_active ON maintenance_items(active);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_report_items_item_id ON report_items(item_id);")

        set_schema_version(cur, 3)

    # v4 - campos para resolver pendências
    if v < 4:
        if not table_has_column(cur, "report_items", "resolved_at"):
            cur.execute("ALTER TABLE report_items ADD COLUMN resolved_at TEXT;")
        if not table_has_column(cur, "report_items", "resolved_by"):
            cur.execute("ALTER TABLE report_items ADD COLUMN resolved_by TEXT;")
        if not table_has_column(cur, "report_items", "resolution_note"):
            cur.execute("ALTER TABLE report_items ADD COLUMN resolution_note TEXT;")

        cur.execute("CREATE INDEX IF NOT EXISTS idx_report_items_resolved_at ON report_items(resolved_at);")

        set_schema_version(cur, 4)

def seed_default_items_if_empty():
    defaults = [
        "Fechadura Porta (Pilhas)",
        "Cofre",
        "Frigobar",
        "Toalheiro",
        "Suporte Papel",
        "Ducha",
        "Luzes",
        "Televisao",
        "Telefone",
        "Abajur",
        "Tomadas",
        "Controles",
        "Cortina",
    ]

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM maintenance_items;")
    count = cur.fetchone()[0]

    if count == 0:
        now = datetime.now().isoformat(timespec="seconds")
        cur.executemany(
            "INSERT INTO maintenance_items (name, active, created_at) VALUES (?, 1, ?)",
            [(d, now) for d in defaults]
        )
    conn.commit()
    conn.close()


def init_db():
    backup_db()

    conn = get_conn()
    cur = conn.cursor()

    # Recomendo (melhora concorrência)
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")

    # reports: mantém colunas antigas opcionais pra migração e compatibilidade
    cur.execute("""
        CREATE TABLE IF NOT EXISTS reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_date TEXT NOT NULL,
            technician TEXT NOT NULL,
            created_at TEXT NOT NULL,   

            floor INTEGER,
            apt INTEGER,
            room_code TEXT,

            room INTEGER
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS report_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_id INTEGER NOT NULL,
            item_id INTEGER,
            item TEXT NOT NULL,
            status TEXT NOT NULL,
            note TEXT,
            FOREIGN KEY(report_id) REFERENCES reports(id)
        );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS general_maintenance (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        maint_date TEXT NOT NULL,
        place TEXT NOT NULL,
        description TEXT NOT NULL,
        status TEXT NOT NULL,
        technician TEXT NOT NULL,
        note TEXT,
        created_at TEXT NOT NULL,
        resolved_at TEXT,
        resolved_by TEXT,
        resolution_note TEXT
    );
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_gm_date ON general_maintenance(maint_date);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_gm_status ON general_maintenance(status);")

    migrate_if_needed(cur)

    conn.commit()
    conn.close()

    # seed inicial (se não tiver nenhum item cadastrado ainda)
    seed_default_items_if_empty()


# ----------------------------
# CRUD ITENS
# ----------------------------
def list_items(active_only: bool = True) -> pd.DataFrame:
    conn = get_conn()
    q = "SELECT id, name, active, created_at FROM maintenance_items"
    if active_only:
        q += " WHERE active = 1"
    q += " ORDER BY name ASC;"
    df = pd.read_sql_query(q, conn)
    conn.close()
    return df


def normalize_item_name(name: str) -> str:
    # remove espaços extras e padroniza
    return " ".join(name.strip().split())


def item_exists(name: str) -> bool:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT 1 FROM maintenance_items
        WHERE name = ?
        COLLATE NOCASE
        LIMIT 1;
    """, (name,))
    exists = cur.fetchone() is not None
    conn.close()
    return exists


def add_item(name: str):
    name = normalize_item_name(name)

    if item_exists(name):
        raise ValueError("Item já cadastrado.")

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO maintenance_items (name, active, created_at)
        VALUES (?, 1, ?)
    """, (name, datetime.now().isoformat(timespec="seconds")))
    conn.commit()
    conn.close()


def set_item_active(item_id: int, active: bool):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE maintenance_items SET active = ? WHERE id = ?", (1 if active else 0, item_id))
    conn.commit()
    conn.close()


# ----------------------------
# CRUD RELATÓRIOS
# ----------------------------
def insert_report(report_date: date, floor: int, apt: int, technician: str, items_payload: list[dict]):
    conn = get_conn()
    cur = conn.cursor()

    code = room_code(floor, apt)

    cur.execute("""
        INSERT INTO reports (report_date, floor, apt, room_code, technician, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        report_date.isoformat(),
        floor,
        apt,
        code,
        technician.strip(),
        datetime.now().isoformat(timespec="seconds"),
    ))

    report_id = cur.lastrowid

    cur.executemany("""
        INSERT INTO report_items (report_id, item_id, item, status, note)
        VALUES (?, ?, ?, ?, ?)
    """, [
        (report_id, row["item_id"], row["item"], row["status"], row.get("note", "").strip() or None)
        for row in items_payload
    ])

    conn.commit()
    conn.close()


def fetch_reports(
    date_from: date,
    date_to: date,
    floor: int | None,
    apt: int | None,
    room_code_filter: str | None,
    technician: str | None,
    status: str | None
):
    conn = get_conn()

    query = """
        SELECT
            r.id AS report_id,
            r.report_date,
            r.floor,
            r.apt,
            r.room_code,
            r.technician,
            r.created_at,
            COALESCE(mi.name, ri.item) AS item,
            ri.status,
            COALESCE(ri.note, '') AS note
        FROM reports r
        JOIN report_items ri ON ri.report_id = r.id
        LEFT JOIN maintenance_items mi ON mi.id = ri.item_id
        WHERE r.report_date BETWEEN ? AND ?
    """
    params = [date_from.isoformat(), date_to.isoformat()]

    if room_code_filter:
        query += " AND r.room_code = ?"
        params.append(room_code_filter.strip())

    if floor is not None:
        query += " AND r.floor = ?"
        params.append(floor)

    if apt is not None:
        query += " AND r.apt = ?"
        params.append(apt)

    if technician:
        query += " AND LOWER(r.technician) LIKE ?"
        params.append(f"%{technician.lower().strip()}%")

    if status:
        query += " AND ri.status = ?"
        params.append(status)

    query += " ORDER BY r.report_date DESC, r.floor ASC, r.apt ASC, r.id DESC;"

    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df


# ----------------------------
# UI
# ----------------------------
init_db()

st.title("🛠️ Relatório Diário de Manutenção - Hotel")

menu = st.sidebar.radio("Navegação", ["Registrar manutenção", "Manutenção Geral", "Relatórios", "Pendências", "Itens"])
st.sidebar.markdown("---")
st.sidebar.caption("Dados salvos localmente em SQLite (manutencao_hotel.db).")

if menu == "Registrar manutenção":
    st.subheader("Registrar manutenção do dia")

    if "reset_token" not in st.session_state:
        st.session_state["reset_token"] = 0

    if "saving" not in st.session_state:
        st.session_state["saving"] = False

    colA, colB, colC, colD = st.columns(4)
    with colA:
        report_date = st.date_input("Data", value=date.today())
    with colB:
        floor = st.selectbox("Andar", list(range(1, 13)), index=0)
    with colC:
        apt = st.selectbox("Apartamento (no andar)", list(range(1, 19)), index=0)
    with colD:
        code = room_code(floor, apt)
        st.text_input("Quarto", value=code, disabled=True)

    technician = st.text_input("Responsável / Técnico", placeholder="Ex: Gabriel / Manutenção")

    items_df = list_items(active_only=True)
    if items_df.empty:
        st.warning("Nenhum item ativo cadastrado. Vá em 'Itens' e cadastre/ative os itens.")
        st.stop()

    st.markdown("### Itens vistoriados no quarto")
    st.caption("Selecione apenas o que você realmente vistoriou/mexeu. O que não for selecionado não será salvo.")

    options = [(int(r["id"]), r["name"]) for _, r in items_df.iterrows()]
    name_by_id = {item_id: name for item_id, name in options}

    selected_names = st.multiselect(
        "Selecione os itens vistoriados",
        options=[name for _, name in options],
        placeholder="Ex: Frigobar, Luzes, Tomadas...",
        key=f"vistoria_selected_names_{st.session_state['reset_token']}"
    )

    selected_ids = [item_id for item_id, name in options if name in selected_names]

    st.markdown("### Detalhes dos itens selecionados")
    items_payload = []

    for item_id in selected_ids:
        item_name = name_by_id[item_id]

        # chaves por quarto+data+item para não "vazar" estado entre quartos
        base_key = f"{report_date.isoformat()}_{code}_{item_id}"

        with st.container(border=True):
            c1, c2 = st.columns([1, 2])
            with c1:
                status = st.selectbox(
                    item_name,
                    ["OK", "Problema"],
                    index=0,
                    key=f"status_{base_key}",
                )
            with c2:
                note = st.text_input(
                    "Observação (opcional)",
                    key=f"note_{base_key}",
                    placeholder="Ex: trocado / ajustado / peça quebrada"
                )

        items_payload.append({
            "item_id": item_id,
            "item": item_name,
            "status": status,
            "note": note
        })

    st.markdown("---")
    colS1, colS2 = st.columns([1, 3])
    with colS1:
        save = st.button("💾 Salvar relatório", type="primary", disabled=st.session_state["saving"])
    with colS2:
        st.caption("Cada item selecionado vira uma linha no relatório.")

    if save:
        if st.session_state["saving"]:
            st.warning("Salvando... aguarde.")
            st.stop()

        st.session_state["saving"] = True

        if not technician.strip():
            st.session_state["saving"] = False
            st.error("Informe o nome do responsável/técnico.")
            st.stop()

        if not items_payload:
            st.session_state["saving"] = False
            st.error("Selecione pelo menos 1 item vistoriado antes de salvar.")
            st.stop()

        insert_report(report_date, int(floor), int(apt), technician, items_payload)

        st.session_state["saving"] = False
        st.success(f"Relatório salvo! ✅ (Quarto {code} - {report_date.strftime('%d/%m/%Y')})")

        # reset do multiselect (sem erro)
        st.session_state["reset_token"] += 1
        st.rerun()

elif menu == "Relatórios":
    st.subheader("Relatórios e exportação")
    st.markdown("## 📦 Exportação Unificada (tudo em 1 arquivo)")

    colU1, colU2, colU3 = st.columns(3)
    with colU1:
        uni_from = st.date_input("De (unificado)", value=date.today(), key="uni_from")
    with colU2:
        uni_to = st.date_input("Até (unificado)", value=date.today(), key="uni_to")
    with colU3:
        uni_floor_chk = st.checkbox("Filtrar por andar (apts)", value=False, key="uni_floor_chk")

    uni_floor = None
    if uni_floor_chk:
        uni_floor = st.selectbox("Andar", list(range(1, 13)), index=0, key="uni_floor")

    if uni_from > uni_to:
        st.error("A data 'De' não pode ser maior que a data 'Até'.")
    else:
        # 1) Aptos (todos os status)
        df_apts_uni = fetch_reports(uni_from, uni_to, uni_floor, None, None, technician=None, status=None)

        # 2) Resolvidas aptos
        df_res_uni = fetch_resolved(uni_from, uni_to, uni_floor)

        # 3) Manutenção geral
        df_gm_uni = fetch_general_maintenance(uni_from, uni_to, status=None, search=None)

        # Colunas (padronizar para export)
        cols_apts = ["report_date","room_code","floor","apt","technician","item","status","note","created_at","report_id"]
        cols_res = ["report_date","room_code","floor","apt","item","note","resolved_at","resolved_by","resolution_note","technician","report_id"]
        cols_gm = ["maint_date","place","description","status","technician","note","resolved_at","resolved_by","resolution_note","created_at","id"]

        df_apts_uni = df_apts_uni[cols_apts] if not df_apts_uni.empty else pd.DataFrame(columns=cols_apts)
        df_res_uni = df_res_uni[cols_res] if not df_res_uni.empty else pd.DataFrame(columns=cols_res)
        df_gm_uni = df_gm_uni[cols_gm] if not df_gm_uni.empty else pd.DataFrame(columns=cols_gm)

        xlsx_bytes = export_unified_xlsx(df_apts_uni, df_res_uni, df_gm_uni)

        st.download_button(
            "⬇️ Baixar Excel Unificado",
            data=xlsx_bytes,
            file_name=f"relatorio_unificado_{uni_from.isoformat()}_a_{uni_to.isoformat()}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="uni_xlsx"
        )

    st.markdown("---")

    tab1, tab2, tab3 = st.tabs(["📄 Relatórios", "✅ Pendencias Resolvidas", "🧰 Manutenção Geral"])

    # -----------------------------
    # TAB 1 - RELATÓRIOS (igual ao seu, só que dentro do tab1)
    # -----------------------------
    with tab1:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            date_from = st.date_input("De", value=date.today(), key="rep_de")
        with col2:
            date_to = st.date_input("Até", value=date.today(), key="rep_ate")
        with col3:
            filter_mode = st.selectbox(
                "Filtrar por",
                ["(nenhum)", "Andar/Apto", "Código do quarto (ex: 0101)"],
                index=0,
                key="rep_filter_mode"
            )
        with col4:
            status = st.selectbox("Status do item", ["(todos)"] + STATUSES, index=0, key="rep_status")
            status_val = None if status == "(todos)" else status

        floor_val = None
        apt_val = None
        code_val = None

        if filter_mode == "Andar/Apto":
            cA, cB = st.columns(2)
            with cA:
                floor_val = st.selectbox("Andar (filtro)", list(range(1, 13)), index=0, key="rep_floor")
            with cB:
                apt_val = st.selectbox("Apto (filtro)", list(range(1, 19)), index=0, key="rep_apt")
            code_val = room_code(int(floor_val), int(apt_val))

        elif filter_mode == "Código do quarto (ex: 0101)":
            code_val = st.text_input(
                "Quarto (4 dígitos)",
                placeholder="0101, 0218, 1203...",
                key="rep_roomcode"
            ).strip() or None

        technician = st.text_input(
            "Filtrar por responsável (contém)",
            placeholder="Ex: gabriel / joão / terceirizada",
            key="rep_tech"
        )

        if date_from > date_to:
            st.error("A data 'De' não pode ser maior que a data 'Até'.")
        else:
            df = fetch_reports(date_from, date_to, floor_val, apt_val, code_val, technician, status_val)

            st.markdown("### Resultado")
            st.caption(f"{len(df)} linha(s) encontrada(s).")

            if df.empty:
                st.info("Nada encontrado com esses filtros.")
            else:
                show_cols = ["report_date", "room_code", "floor", "apt", "technician", "item", "status", "note", "created_at", "report_id"]
                st.dataframe(df[show_cols], use_container_width=True, hide_index=True)

                csv = df[show_cols].to_csv(index=False).encode("utf-8-sig")
                st.download_button(
                    "⬇️ Baixar CSV",
                    data=csv,
                    file_name=f"relatorio_manutencao_{date_from.isoformat()}_a_{date_to.isoformat()}.csv",
                    mime="text/csv",
                    key="rep_csv"
                )

    # -----------------------------
    # TAB 2 - RESOLVIDAS
    # -----------------------------
    with tab2:
        st.markdown("### Pendências resolvidas (quando, quem e o que foi feito)")

        colA, colB, colC = st.columns(3)
        with colA:
            date_from_r = st.date_input("De (data do relatório)", value=date.today(), key="res_de")
        with colB:
            date_to_r = st.date_input("Até (data do relatório)", value=date.today(), key="res_ate")
        with colC:
            floor_filter_r = st.checkbox("Filtrar por andar", value=False, key="res_floor_chk")

        floor_val_r = None
        if floor_filter_r:
            floor_val_r = st.selectbox("Andar", list(range(1, 13)), index=0, key="res_floor")

        if date_from_r > date_to_r:
            st.error("A data 'De' não pode ser maior que a data 'Até'.")
        else:
            df_res = fetch_resolved(date_from_r, date_to_r, floor_val_r)

            if df_res.empty:
                st.info("Nenhuma pendência resolvida nesse período.")
            else:
                st.success(f"{len(df_res)} pendência(s) resolvida(s) encontrada(s).")

                show_cols_res = [
                    "report_date", "room_code", "floor", "apt",
                    "item", "note",
                    "resolved_at", "resolved_by", "resolution_note",
                    "technician", "report_id"
                ]
                st.dataframe(df_res[show_cols_res], use_container_width=True, hide_index=True)

                csv_res = df_res[show_cols_res].to_csv(index=False).encode("utf-8-sig")
                st.download_button(
                    "⬇️ Baixar CSV (Resolvidas)",
                    data=csv_res,
                    file_name=f"pendencias_resolvidas_{date_from_r.isoformat()}_a_{date_to_r.isoformat()}.csv",
                    mime="text/csv",
                    key="res_csv"
                )
        # -----------------------------
    # TAB 3 - MANUTENÇÃO GERAL
    # -----------------------------
    with tab3:
        st.markdown("### Manutenção Geral (fora dos apartamentos)")

        col1, col2, col3 = st.columns(3)
        with col1:
            gm_from = st.date_input("De", value=date.today(), key="rep_gm_from")
        with col2:
            gm_to = st.date_input("Até", value=date.today(), key="rep_gm_to")
        with col3:
            gm_status = st.selectbox("Status", ["(todos)"] + GM_STATUSES, index=0, key="rep_gm_status")
            gm_status_val = None if gm_status == "(todos)" else gm_status

        gm_search = st.text_input(
            "Buscar (local/descrição/técnico)",
            placeholder="Ex: elevador / recepção / gabriel",
            key="rep_gm_search"
        )

        if gm_from > gm_to:
            st.error("A data 'De' não pode ser maior que a data 'Até'.")
        else:
            df_gm = fetch_general_maintenance(gm_from, gm_to, gm_status_val, gm_search)

            if df_gm.empty:
                st.info("Nada encontrado.")
            else:
                st.success(f"{len(df_gm)} registro(s) encontrado(s).")

                show_cols_gm = [
                    "maint_date", "place", "description", "status",
                    "technician", "note",
                    "resolved_at", "resolved_by", "resolution_note",
                    "created_at", "id"
                ]
                st.dataframe(df_gm[show_cols_gm], use_container_width=True, hide_index=True)

                csv_gm = df_gm[show_cols_gm].to_csv(index=False).encode("utf-8-sig")
                st.download_button(
                    "⬇️ Baixar CSV (Manutenção Geral)",
                    data=csv_gm,
                    file_name=f"relatorio_manutencao_geral_{gm_from.isoformat()}_a_{gm_to.isoformat()}.csv",
                    mime="text/csv",
                    key="rep_gm_csv"
                )

                st.markdown("---")
                
elif menu == "Manutenção Geral":
    st.subheader("🧰 Manutenção Geral (fora dos apartamentos)")

    tabG1, tabG2 = st.tabs(["➕ Registrar", "📄 Consultar / Resolver"])

    with tabG1:
        c1, c2, c3 = st.columns(3)
        with c1:
            maint_date = st.date_input("Data", value=date.today(), key="gm_date")
        with c2:
            place = st.text_input("Local", placeholder="Ex: Recepção, Corredor, Elevador, Piscina...", key="gm_place")
        with c3:
            status = st.selectbox("Status", GM_STATUSES, index=0, key="gm_status")

        technician = st.text_input("Responsável / Técnico", placeholder="Ex: Gabriel / Manutenção", key="gm_tech")
        description = st.text_area("Descrição do serviço", placeholder="Ex: Troca de lâmpadas do corredor...", key="gm_desc")
        note = st.text_area("Observação (opcional)", key="gm_note")

        if st.button("💾 Salvar Manutenção Geral", type="primary", key="gm_save"):
            if not place.strip() or not technician.strip() or not description.strip():
                st.error("Preencha Local, Responsável e Descrição.")
            else:
                insert_general_maintenance(maint_date, place, description, status, technician, note)
                st.success("Registro salvo! ✅")
                st.rerun()

    with tabG2:
        col1, col2, col3 = st.columns(3)
        with col1:
            df_from = st.date_input("De", value=date.today(), key="gm_from")
        with col2:
            df_to = st.date_input("Até", value=date.today(), key="gm_to")
        with col3:
            st_filter = st.selectbox("Status", ["(todos)"] + GM_STATUSES, index=0, key="gm_filter_status")
            st_val = None if st_filter == "(todos)" else st_filter

        search = st.text_input("Buscar (local/descrição/técnico)", placeholder="Ex: elevador / recepção / gabriel", key="gm_search")

        if df_from > df_to:
            st.error("A data 'De' não pode ser maior que a data 'Até'.")
            st.stop()

        df = fetch_general_maintenance(df_from, df_to, st_val, search)

        if df.empty:
            st.info("Nada encontrado.")
        else:
            show_cols = ["maint_date", "place", "description", "status", "technician", "note", "resolved_at", "resolved_by", "resolution_note", "created_at", "id"]
            st.dataframe(df[show_cols], use_container_width=True, hide_index=True)

            csv = df[show_cols].to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                "⬇️ Baixar CSV (Manutenção Geral)",
                data=csv,
                file_name=f"manutencao_geral_{df_from.isoformat()}_a_{df_to.isoformat()}.csv",
                mime="text/csv",
                key="gm_csv"
            )

            st.markdown("---")
            st.markdown("### ✅ Resolver manutenção geral")

            resolved_by = st.text_input("Quem resolveu?", placeholder="Ex: Gabriel / Manutenção", key="gm_res_by")

            # Mostra só as que não estão resolvidas para resolver
            pend = df[df["status"] != "Resolvido"].copy()

            if pend.empty:
                st.success("Nenhuma manutenção geral pendente ✅")
            else:
                for _, row in pend.iterrows():
                    gm_id = int(row["id"])
                    title = f"{row['maint_date']} • {row['place']} • {row['status']}"
                    with st.expander(title):
                        st.write(f"**Descrição:** {row['description']}")
                        st.write(f"**Registrado por:** {row['technician']}")
                        if row["note"]:
                            st.write(f"**Obs:** {row['note']}")

                        res_note = st.text_area("O que foi feito?", key=f"gm_res_note_{gm_id}")

                        if st.button("✅ Marcar como resolvido", type="primary", key=f"gm_btn_res_{gm_id}"):
                            if not resolved_by.strip():
                                st.error("Informe quem resolveu.")
                                st.stop()
                            resolve_general_maintenance(gm_id, resolved_by, res_note)
                            st.success("Marcado como resolvido! ✅")
                            st.rerun()

elif menu == "Pendências":
    st.subheader("Pendências (itens com PROBLEMA)")

    col1, col2, col3 = st.columns(3)
    with col1:
        date_from = st.date_input("De", value=date.today())
    with col2:
        date_to = st.date_input("Até", value=date.today())
    with col3:
        floor_filter = st.checkbox("Filtrar por andar", value=False)

    floor_val = None
    if floor_filter:
        floor_val = st.selectbox("Andar (pendências)", list(range(1, 13)), index=0)

    if date_from > date_to:
        st.error("A data 'De' não pode ser maior que a data 'Até'.")
        st.stop()

    # ✅ agora busca só pendências ABERTAS (não resolvidas)
    df = fetch_pendencies_open(date_from, date_to, floor_val)

    if df.empty:
        st.success("Nenhuma pendência nesse período ✅")
        st.stop()

    st.warning(f"{len(df)} pendência(s) aberta(s) encontrada(s).")

    show_cols = ["report_date", "room_code", "floor", "apt", "technician", "item", "note", "created_at", "report_id"]
    st.dataframe(df[show_cols], use_container_width=True, hide_index=True)

    st.markdown("---")
    st.markdown("## ✅ Resolver pendência")

    resolved_by = st.text_input("Quem resolveu?", placeholder="Ex: Gabriel / Manutenção / Terceirizada")

    st.caption("Abra uma pendência abaixo, descreva o que foi feito e marque como resolvida.")

    for _, row in df.iterrows():
        report_item_id = int(row["report_item_id"])
        room_code_val = row["room_code"]
        item_name = row["item"]
        rep_date = row["report_date"]
        note = row["note"]

        with st.expander(f"🏷️ {rep_date} • Quarto {room_code_val} • {item_name}", expanded=False):
            st.write(f"**Registrado por:** {row['technician']}")
            st.write(f"**Observação do problema:** {note or '-'}")

            resolution_note = st.text_area(
                "O que foi feito para resolver?",
                key=f"resolution_{report_item_id}",
                placeholder="Ex: trocado cabo / resetado aparelho / substituída peça..."
            )

            colA, colB = st.columns([1, 3])
            with colA:
                if st.button("✅ Marcar como resolvida", type="primary", key=f"btn_resolve_{report_item_id}"):
                    if not resolved_by.strip():
                        st.error("Informe quem resolveu.")
                        st.stop()

                    resolve_pendency(report_item_id, resolved_by, resolution_note)
                    st.success("Pendência marcada como resolvida! ✅")
                    st.rerun()

            with colB:
                st.caption("Ao resolver, a pendência sai da lista (mas fica registrada no histórico).")

    st.markdown("---")
    st.markdown("### Resumo por quarto (pendências abertas)")
    resumo = (
        df.groupby(["report_date", "room_code"])
          .size()
          .reset_index(name="qtd_pendencias")
          .sort_values(["report_date", "room_code"], ascending=[False, True])
    )
    st.dataframe(resumo, use_container_width=True, hide_index=True)

elif menu == "Itens":
    st.subheader("Cadastro de Itens de Manutenção")

    with st.expander("➕ Adicionar novo item", expanded=True):
        new_name = st.text_input("Nome do item", placeholder="Ex: Ar-condicionado / Interfone / Fechadura Banheiro")
        if st.button("Adicionar", type="primary"):
            if not new_name.strip():
                st.error("Digite um nome.")
            else:
                try:
                    add_item(new_name)
                    st.success("Item cadastrado!")
                    st.rerun()
                except ValueError as e:
                    st.warning(str(e))
                except sqlite3.IntegrityError:
                    st.warning("Esse item já existe.")

    st.markdown("### Itens cadastrados")
    items_all = list_items(active_only=False)

    if items_all.empty:
        st.info("Nenhum item cadastrado ainda.")
    else:
        # Ajusta ativo para bool na visualização
        view_df = items_all.copy()
        view_df["active"] = view_df["active"].apply(lambda x: "Sim" if int(x) == 1 else "Não")
        st.dataframe(view_df, use_container_width=True, hide_index=True)

        st.markdown("### Ativar / Desativar itens")
        st.caption("Desativar não apaga histórico; só remove do checklist novo.")

        for _, r in items_all.iterrows():
            item_id = int(r["id"])
            name = r["name"]
            active = bool(r["active"])

            col1, col2 = st.columns([4, 1])
            with col1:
                st.write(name)
            with col2:
                new_active = st.toggle("Ativo", value=active, key=f"active_{item_id}")
                if new_active != active:
                    set_item_active(item_id, new_active)
                    st.rerun()