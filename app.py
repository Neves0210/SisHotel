import sqlite3
from datetime import date, datetime
import pandas as pd
import streamlit as st

# ----------------------------
# CONFIG
# ----------------------------
st.set_page_config(page_title="Hotel - Manutenção Diária", page_icon="🛠️", layout="wide")

ITEMS = [
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

STATUSES = ["OK", "Problema", "N/A"]

DB_PATH = "manutencao_hotel.db"


# ----------------------------
# DB HELPERS
# ----------------------------
def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_date TEXT NOT NULL,
            room INTEGER NOT NULL,
            technician TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS report_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_id INTEGER NOT NULL,
            item TEXT NOT NULL,
            status TEXT NOT NULL,
            note TEXT,
            FOREIGN KEY(report_id) REFERENCES reports(id)
        );
    """)

    # índice simples pra acelerar filtros
    cur.execute("CREATE INDEX IF NOT EXISTS idx_reports_date ON reports(report_date);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_reports_room ON reports(room);")

    conn.commit()
    conn.close()


def insert_report(report_date: date, room: int, technician: str, items_payload: list[dict]):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO reports (report_date, room, technician, created_at)
        VALUES (?, ?, ?, ?)
    """, (report_date.isoformat(), room, technician.strip(), datetime.now().isoformat(timespec="seconds")))

    report_id = cur.lastrowid

    cur.executemany("""
        INSERT INTO report_items (report_id, item, status, note)
        VALUES (?, ?, ?, ?)
    """, [
        (report_id, row["item"], row["status"], row.get("note", "").strip() or None)
        for row in items_payload
    ])

    conn.commit()
    conn.close()


def fetch_reports(date_from: date, date_to: date, room: int | None, technician: str | None, status: str | None):
    conn = get_conn()

    query = """
        SELECT
            r.id AS report_id,
            r.report_date,
            r.room,
            r.technician,
            r.created_at,
            ri.item,
            ri.status,
            COALESCE(ri.note, '') AS note
        FROM reports r
        JOIN report_items ri ON ri.report_id = r.id
        WHERE r.report_date BETWEEN ? AND ?
    """
    params = [date_from.isoformat(), date_to.isoformat()]

    if room is not None:
        query += " AND r.room = ?"
        params.append(room)

    if technician:
        query += " AND LOWER(r.technician) LIKE ?"
        params.append(f"%{technician.lower().strip()}%")

    if status:
        query += " AND ri.status = ?"
        params.append(status)

    query += " ORDER BY r.report_date DESC, r.room ASC, r.id DESC;"

    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df


# ----------------------------
# UI
# ----------------------------
init_db()

st.title("🛠️ Relatório Diário de Manutenção - Hotel (216 aptos)")

menu = st.sidebar.radio("Navegação", ["Registrar manutenção", "Relatórios", "Pendências"])

st.sidebar.markdown("---")
st.sidebar.caption("Dados salvos localmente em SQLite (manutencao_hotel.db).")

if menu == "Registrar manutenção":
    st.subheader("Registrar manutenção do dia")

    colA, colB, colC = st.columns(3)
    with colA:
        report_date = st.date_input("Data", value=date.today())
    with colB:
        room = st.number_input("Apartamento", min_value=1, max_value=216, value=1, step=1)
    with colC:
        technician = st.text_input("Responsável / Técnico", placeholder="Ex: Gabriel / Manutenção")

    st.markdown("### Checklist dos itens")
    st.caption("Dica: deixe como N/A quando não se aplica (ex.: item removido / fora de uso).")

    items_payload = []
    for item in ITEMS:
        with st.container(border=True):
            c1, c2 = st.columns([1, 2])
            with c1:
                status = st.selectbox(item, STATUSES, index=0, key=f"status_{item}")
            with c2:
                note = st.text_input("Observação (opcional)", key=f"note_{item}", placeholder="Ex: pilhas fracas / troca solicitada / peça quebrada")
        items_payload.append({"item": item, "status": status, "note": note})

    st.markdown("---")
    colS1, colS2 = st.columns([1, 3])
    with colS1:
        save = st.button("💾 Salvar relatório", type="primary")
    with colS2:
        st.caption("Ao salvar, cada item vira uma linha no relatório (facilita filtros e pendências).")

    if save:
        if not technician.strip():
            st.error("Informe o nome do responsável/técnico.")
        else:
            insert_report(report_date, int(room), technician, items_payload)
            st.success(f"Relatório salvo! ✅ (Apto {int(room)} - {report_date.strftime('%d/%m/%Y')})")

elif menu == "Relatórios":
    st.subheader("Relatórios e exportação")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        date_from = st.date_input("De", value=date.today())
    with col2:
        date_to = st.date_input("Até", value=date.today())
    with col3:
        room_filter = st.checkbox("Filtrar por apartamento", value=False)
        room = None
        if room_filter:
            room = st.number_input("Apartamento (1–216)", min_value=1, max_value=216, value=1, step=1)
    with col4:
        status = st.selectbox("Status do item", ["(todos)"] + STATUSES, index=0)
        status_val = None if status == "(todos)" else status

    technician = st.text_input("Filtrar por responsável (contém)", placeholder="Ex: gabriel / joão / terceirizada")

    if date_from > date_to:
        st.error("A data 'De' não pode ser maior que a data 'Até'.")
    else:
        df = fetch_reports(date_from, date_to, int(room) if room is not None else None, technician, status_val)

        st.markdown("### Resultado")
        st.caption(f"{len(df)} linha(s) encontrada(s).")

        if df.empty:
            st.info("Nada encontrado com esses filtros.")
        else:
            # visão mais amigável
            show_cols = ["report_date", "room", "technician", "item", "status", "note", "created_at", "report_id"]
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
        room_filter = st.checkbox("Somente um apartamento", value=False)
        room = None
        if room_filter:
            room = st.number_input("Apartamento (1–216)", min_value=1, max_value=216, value=1, step=1)

    if date_from > date_to:
        st.error("A data 'De' não pode ser maior que a data 'Até'.")
    else:
        df = fetch_reports(date_from, date_to, int(room) if room is not None else None, technician=None, status="Problema")

        if df.empty:
            st.success("Nenhuma pendência nesse período ✅")
        else:
            st.warning(f"{len(df)} pendência(s) encontrada(s).")

            show_cols = ["report_date", "room", "technician", "item", "status", "note", "created_at", "report_id"]
            st.dataframe(df[show_cols], use_container_width=True, hide_index=True)

            # agrupado por apto/data (resumo)
            st.markdown("### Resumo por apartamento")
            resumo = (
                df.groupby(["report_date", "room"])
                  .size()
                  .reset_index(name="qtd_pendencias")
                  .sort_values(["report_date", "room"], ascending=[False, True])
            )
            st.dataframe(resumo, use_container_width=True, hide_index=True)