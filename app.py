import os
import shutil
import sqlite3
from datetime import date, datetime

import pandas as pd
import streamlit as st


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

st.title("🛠️ Relatório Diário de Manutenção - Hotel (12 andares x 18 aptos)")

menu = st.sidebar.radio("Navegação", ["Registrar manutenção", "Relatórios", "Pendências", "Itens"])
st.sidebar.markdown("---")
st.sidebar.caption("Dados salvos localmente em SQLite (manutencao_hotel.db).")

if menu == "Registrar manutenção":
    st.subheader("Registrar manutenção do dia")

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
    else:
        st.markdown("### Checklist dos itens")
        st.caption("Use N/A quando não se aplica. Marque Problema para gerar pendências.")

        items_payload = []
        for _, row in items_df.iterrows():
            item_id = int(row["id"])
            item_name = row["name"]

            with st.container(border=True):
                c1, c2 = st.columns([1, 2])
                with c1:
                    status = st.selectbox(item_name, STATUSES, index=0, key=f"status_{item_id}")
                with c2:
                    note = st.text_input(
                        "Observação (opcional)",
                        key=f"note_{item_id}",
                        placeholder="Ex: pilhas fracas / troca solicitada / peça quebrada"
                    )

            items_payload.append({"item_id": item_id, "item": item_name, "status": status, "note": note})

        st.markdown("---")
        colS1, colS2 = st.columns([1, 3])
        with colS1:
            save = st.button("💾 Salvar relatório", type="primary")
        with colS2:
            st.caption("Cada item vira uma linha no relatório (facilita filtro e pendências).")

        if save:
            if not technician.strip():
                st.error("Informe o nome do responsável/técnico.")
            else:
                insert_report(report_date, int(floor), int(apt), technician, items_payload)
                st.success(f"Relatório salvo! ✅ (Quarto {code} - {report_date.strftime('%d/%m/%Y')})")

elif menu == "Relatórios":
    st.subheader("Relatórios e exportação")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        date_from = st.date_input("De", value=date.today())
    with col2:
        date_to = st.date_input("Até", value=date.today())
    with col3:
        filter_mode = st.selectbox("Filtrar por", ["(nenhum)", "Andar/Apto", "Código do quarto (ex: 0101)"], index=0)
    with col4:
        status = st.selectbox("Status do item", ["(todos)"] + STATUSES, index=0)
        status_val = None if status == "(todos)" else status

    floor_val = None
    apt_val = None
    code_val = None

    if filter_mode == "Andar/Apto":
        cA, cB = st.columns(2)
        with cA:
            floor_val = st.selectbox("Andar (filtro)", list(range(1, 13)), index=0)
        with cB:
            apt_val = st.selectbox("Apto (filtro)", list(range(1, 19)), index=0)
        code_val = room_code(int(floor_val), int(apt_val))
    elif filter_mode == "Código do quarto (ex: 0101)":
        code_val = st.text_input("Quarto (4 dígitos)", placeholder="0101, 0218, 1203...").strip() or None

    technician = st.text_input("Filtrar por responsável (contém)", placeholder="Ex: gabriel / joão / terceirizada")

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
                mime="text/csv"
            )

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
    else:
        df = fetch_reports(date_from, date_to, floor_val, None, None, technician=None, status="Problema")

        if df.empty:
            st.success("Nenhuma pendência nesse período ✅")
        else:
            st.warning(f"{len(df)} pendência(s) encontrada(s).")

            show_cols = ["report_date", "room_code", "floor", "apt", "technician", "item", "status", "note", "created_at", "report_id"]
            st.dataframe(df[show_cols], use_container_width=True, hide_index=True)

            st.markdown("### Resumo por quarto")
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