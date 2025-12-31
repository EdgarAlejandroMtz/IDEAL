import os
import re
import numpy as np
import pandas as pd
import pyodbc
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from datetime import datetime

# Para leer XLSB
from pyxlsb import open_workbook

# Para logo (Pillow)
from PIL import Image, ImageTk


# =========================
# Constantes SQL DATETIME
# =========================
SQL_MIN = datetime(1753, 1, 1)
SQL_MAX = datetime(9999, 12, 31, 23, 59, 59)

# =========================
# Control de cancelación
# =========================
cancelar = False

def cancelar_carga():
    global cancelar
    cancelar = True


# =========================
# Sanitizadores (SQL safe)
# =========================
def limpiar_fecha_sql_datetime(x):
    """
    - Si viene string tipo '2024-06-24' -> parse normal
    - Si viene número (serial Excel) -> convertir con origen Excel (1899-12-30)
    - Asegura rango válido para SQL datetime
    """
    try:
        if x is None or (isinstance(x, float) and np.isnan(x)) or pd.isna(x):
            return None

        # 1) Excel serial date (XLSB suele traer floats/ints)
        if isinstance(x, (int, float, np.integer, np.floating)):
            # rango típico de serial Excel para fechas reales (evita 0, 1, etc.)
            # 30000 ~ 1982, 60000 ~ 2064
            if 20000 <= float(x) <= 80000:
                dt = pd.to_datetime(float(x), unit="D", origin="1899-12-30", errors="coerce")
            else:
                dt = pd.to_datetime(x, errors="coerce")
        else:
            dt = pd.to_datetime(x, errors="coerce")

        if pd.isna(dt):
            return None

        dt = dt.to_pydatetime()
        if dt < SQL_MIN or dt > SQL_MAX:
            return None
        return dt
    except:
        return None

def to_num_or_none(x):
    try:
        if x is None or pd.isna(x):
            return None
    except:
        pass
    try:
        v = pd.to_numeric(x, errors="coerce")
        if pd.isna(v) or v in (np.inf, -np.inf):
            return None
        return float(v)
    except:
        return None

def to_str_or_none(x):
    try:
        if x is None or pd.isna(x):
            return None
    except:
        pass
    s = str(x).strip()
    if s == "" or s.lower() in ("nan", "none"):
        return None
    return s


# =========================
# Columna Archivo desde nombre (con AJUSTE al final)
# Ej: "SEM 4 ENE 2025 ... AJUSTE PERMANENCIA 2"
# -> "ENERO 2025 SEM 04 - AJUSTE_PERMANENCIA_2"
# =========================
def formatear_archivo_desde_nombre(ruta, tipo_archivo):
    base = os.path.basename(ruta)
    base = os.path.splitext(base)[0]

    base_norm = re.sub(r"[^A-Za-z0-9ÁÉÍÓÚÜÑáéíóúüñ\s\-_/]", " ", base, flags=re.UNICODE)
    base_norm = re.sub(r"\s+", " ", base_norm).strip().upper()

    es_ajuste = "AJUSTE" in base_norm

    match = re.search(r"\bSEM\s*(\d{1,2})\s+([A-Z]{3})\s+(\d{4})\b", base_norm)

    meses = {
        "ENE": "ENERO", "FEB": "FEBRERO", "MAR": "MARZO", "ABR": "ABRIL",
        "MAY": "MAYO", "JUN": "JUNIO", "JUL": "JULIO", "AGO": "AGOSTO",
        "SEP": "SEPTIEMBRE", "OCT": "OCTUBRE", "NOV": "NOVIEMBRE", "DIC": "DICIEMBRE"
    }

    tipo_formateado = tipo_archivo.strip().upper().replace(" ", "_")
    if tipo_archivo.upper() == "PERMANENCIA 2":
        tipo_formateado = "PERMANENCIA_2"

    if es_ajuste:
        tipo_formateado = f"AJUSTE_{tipo_formateado}"

    if not match:
        return f"{base_norm} - {tipo_formateado}"

    semana, mes_abrev, anio = match.groups()
    mes_completo = meses.get(mes_abrev, mes_abrev)
    return f"{mes_completo} {anio} SEM {int(semana):02d} - {tipo_formateado}"


# =========================
# TXT -> DataFrame (formato final 17 columnas)
# - INICIALES/PERMANENCIA: layout clásico (>=23)
# - RECARGAS/AJUSTE RECARGAS: layout largo (>=28/29) -> mapeo especial
# =========================
def txt_a_dataframe(ruta_txt, tipo_archivo):
    archivo_formateado = formatear_archivo_desde_nombre(ruta_txt, tipo_archivo)

    rows = []
    with open(ruta_txt, encoding="latin1", errors="replace") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue

            parts = [p.strip() for p in line.split("/")]

            # Fix: token duplicado donde se repite la Linea (caso visto)
            if len(parts) > 10 and parts[0].isdigit() and parts[9].isdigit() and parts[0] == parts[9]:
                parts.pop(9)

            # ---- MAPEOS ----
            t = tipo_archivo.strip().upper()

            # ✅ RECARGAS (incluye AJUSTE RECARGAS) -> layout largo
            if t == "RECARGAS" and len(parts) >= 28:
                # Basado en tu orden real:
                # 0 linea
                # 1 fecha_portacion
                # 3 estatus
                # 4 motivo
                # 5 tipo_comision
                # 6 monto
                # 7 fuerza_venta
                # 10 periodo_participacion
                # 14 region_registro
                # 15 numPromotor
                # 16 promotor
                # 17 supervisor (nombre)
                # 18 grupo
                # 19 nombreCoo
                # 20 numEmpCoo
                # Nota: en estos recargas TXT normalmente NO viene num_supervisor numérico; lo dejamos NULL.
                linea = to_num_or_none(parts[0])
                if linea is None:
                    continue

                rows.append({
                    "Linea": linea,
                    "Fecha_Portacion": limpiar_fecha_sql_datetime(parts[1]),
                    "Estatus_Comision": to_str_or_none(parts[3]),
                    "Motivo_Rechazo": to_str_or_none(parts[4]),
                    "Tipo_Comision": to_str_or_none(parts[5]),
                    "Monto": to_num_or_none(parts[6]),
                    "Fuerza_Venta": to_str_or_none(parts[7]),
                    "Periodo_Participacion": to_num_or_none(parts[10]),
                    "Region_Registro": to_num_or_none(parts[14]),
                    "Num_Promotor": to_num_or_none(parts[15]),
                    "Promotor": to_str_or_none(parts[16]),
                    "Num_Supervisor": None,
                    "Nombre_Supervisor": to_str_or_none(parts[17]),
                    "Grupo": to_str_or_none(parts[18]),
                    "Num_Coord": to_num_or_none(parts[20]),
                    "Nombre_Coord": to_str_or_none(parts[19]),
                    "Archivo": archivo_formateado
                })
                continue

            # ✅ Layout clásico (INICIALES / PERMANENCIA / PERMANENCIA 2)
            if len(parts) < 23:
                continue

            linea = to_num_or_none(parts[0])
            if linea is None:
                continue

            rows.append({
                "Linea": linea,
                "Fecha_Portacion": limpiar_fecha_sql_datetime(parts[1]),
                "Estatus_Comision": to_str_or_none(parts[3]),
                "Motivo_Rechazo": to_str_or_none(parts[4]),
                "Tipo_Comision": to_str_or_none(parts[5]),
                "Monto": to_num_or_none(parts[6]),
                "Fuerza_Venta": to_str_or_none(parts[7]),
                "Periodo_Participacion": to_num_or_none(parts[10]),
                "Region_Registro": to_num_or_none(parts[13]),
                "Num_Promotor": to_num_or_none(parts[14]),
                "Promotor": to_str_or_none(parts[15]),
                "Num_Supervisor": to_num_or_none(parts[22]),
                "Nombre_Supervisor": to_str_or_none(parts[16]),
                "Grupo": to_str_or_none(parts[17]),
                "Num_Coord": to_num_or_none(parts[19]),
                "Nombre_Coord": to_str_or_none(parts[18]),
                "Archivo": archivo_formateado
            })

    return pd.DataFrame(rows)


# =========================
# XLSB -> DataFrame (17 columnas)
# - Si trae encabezados: usa encabezados
# - Si NO trae encabezados: asume tu ORDEN de 28 columnas
# - IMPORTANTE: Archivo SIEMPRE se fuerza al nombre formateado (nunca viene NULL)
# =========================
def xlsb_a_dataframe(ruta_xlsb, tipo_archivo):
    archivo_formateado = formatear_archivo_desde_nombre(ruta_xlsb, tipo_archivo)

    assumed_headers = [
        "linea", "fecha_portacion", "fecha_primer_ingreso", "estatus_comision",
        "motivo_rechazo", "tipo_comision", "monto", "fuerza_venta", "carrier",
        "archivo", "periodo_participacion", "porcentajedecomision", "numtelportado",
        "fecha_exitoso", "region_registro", "numpromotor", "promotor", "supervisor",
        "grupo", "nombrecoo", "numempcoo", "clasifcoo", "grupocc", "gpoclascc",
        "cooclascc", "grclascc", "nombrecr", "fechaportacion"
    ]

    def norm_cell(v):
        if v is None:
            return ""
        s = str(v).strip().lower()
        s = re.sub(r"\s+", " ", s)
        return s

    with open_workbook(ruta_xlsb) as wb:
        with wb.get_sheet(1) as sheet:
            it = sheet.rows()
            try:
                first_row = next(it)
            except StopIteration:
                return pd.DataFrame()

            first_vals = [norm_cell(c.v) for c in first_row]
            first_set = set([x for x in first_vals if x])

            # Detectar header real
            parece_header = ("linea" in first_set and "monto" in first_set)

            rows_all = []
            if parece_header:
                headers_norm = first_vals
                for row in it:
                    rows_all.append([c.v for c in row])
            else:
                headers_norm = assumed_headers
                rows_all.append([c.v for c in first_row])  # primera fila es dato
                for row in it:
                    rows_all.append([c.v for c in row])

    if not rows_all:
        return pd.DataFrame()

    max_cols = max(len(r) for r in rows_all)
    fixed_rows = [r + [None] * (max_cols - len(r)) for r in rows_all]

    if headers_norm == assumed_headers:
        if max_cols < len(assumed_headers):
            return pd.DataFrame()
        fixed_rows = [r[:len(assumed_headers)] for r in fixed_rows]
        df_raw = pd.DataFrame(fixed_rows, columns=assumed_headers)
    else:
        df_raw = pd.DataFrame(fixed_rows, columns=headers_norm)

    def pick(colname):
        return df_raw[colname] if colname in df_raw.columns else pd.Series([None] * len(df_raw))

    # ✅ DF normal de 17 columnas
    df = pd.DataFrame({
        "Linea": pick("linea").map(to_num_or_none),
        "Fecha_Portacion": pick("fecha_portacion").map(limpiar_fecha_sql_datetime),
        "Estatus_Comision": pick("estatus_comision").map(to_str_or_none),
        "Motivo_Rechazo": pick("motivo_rechazo").map(to_str_or_none),
        "Tipo_Comision": pick("tipo_comision").map(to_str_or_none),
        "Monto": pick("monto").map(to_num_or_none),
        "Fuerza_Venta": pick("fuerza_venta").map(to_str_or_none),
        "Periodo_Participacion": pick("periodo_participacion").map(to_num_or_none),
        "Region_Registro": pick("region_registro").map(to_num_or_none),
        "Num_Promotor": pick("numpromotor").map(to_num_or_none),
        "Promotor": pick("promotor").map(to_str_or_none),
        "Num_Supervisor": pick("numsupervisor").map(to_num_or_none) if "numsupervisor" in df_raw.columns else None,
        "Nombre_Supervisor": pick("supervisor").map(to_str_or_none),
        "Grupo": pick("grupo").map(to_str_or_none),
        "Num_Coord": pick("numempcoo").map(to_num_or_none) if "numempcoo" in df_raw.columns else pick("num_coord").map(to_num_or_none) if "num_coord" in df_raw.columns else None,
        "Nombre_Coord": pick("nombrecoo").map(to_str_or_none) if "nombrecoo" in df_raw.columns else pick("nombre_coord").map(to_str_or_none) if "nombre_coord" in df_raw.columns else pick("nombreCoo").map(to_str_or_none) if "nombreCoo" in df_raw.columns else None,
        # ✅ Archivo SIEMPRE forzado (evita NULL aunque el XLSB traiga columna vacía)
        "Archivo": [archivo_formateado] * len(df_raw)
    })

    df = df[df["Linea"].notna()].copy()
    return df


# =========================
# Excel rápido con XlsxWriter
# =========================
def guardar_excel_rapido(df, ruta_excel):
    with pd.ExcelWriter(ruta_excel, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Datos")

        wb = writer.book
        ws = writer.sheets["Datos"]

        header_fmt = wb.add_format({
            "bold": True,
            "font_name": "Arial",
            "font_size": 12,
            "align": "center",
            "valign": "vcenter",
            "text_wrap": True,
            "border": 1
        })

        body_fmt = wb.add_format({
            "font_name": "Arial",
            "font_size": 12,
            "valign": "vcenter"
        })

        for col, name in enumerate(df.columns):
            ws.write(0, col, name, header_fmt)

        ws.set_column(0, len(df.columns) - 1, 22, body_fmt)
        ws.freeze_panes(1, 0)
        ws.autofilter(0, 0, len(df), len(df.columns) - 1)


# =========================
# Insertar SQL rápido
# =========================
def insertar_en_sql(df, tipo_archivo, password):
    tabla_destino = {
        "INICIALES": "dbo.Datos_Comisiones_Iniciales",
        "PERMANENCIA": "dbo.Datos_Comisiones_Permanencia",
        "PERMANENCIA 2": "dbo.Datos_Comisiones_Permanencia",
        "RECARGAS": "dbo.Datos_Comisiones_Recargas"
    }[tipo_archivo]

    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=192.168.10.68;"
        "DATABASE=DatosLocales;"
        f"UID=sa;PWD={password};"
        "TrustServerCertificate=Yes;"
    )

    try:
        conn = pyodbc.connect(conn_str, autocommit=False)
    except pyodbc.Error as e:
        if "Login failed for user" in str(e):
            messagebox.showerror("CONTRASEÑA INCORRECTA", "❌ La contraseña ingresada es incorrecta para el usuario 'sa'.")
        else:
            messagebox.showerror("Error de conexión", str(e))
        return False

    cursor = conn.cursor()
    cursor.fast_executemany = True

    columnas_sql = [
        "Linea","Fecha_Portacion","Estatus_Comision","Motivo_Rechazo","Tipo_Comision",
        "Monto","Fuerza_Venta","Periodo_Participacion","Region_Registro","Num_Promotor",
        "Promotor","Num_Supervisor","Nombre_Supervisor","Grupo","Num_Coord","Nombre_Coord","Archivo"
    ]

    df = df[columnas_sql].copy()
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.where(pd.notnull(df), None)

    data = df.to_numpy(dtype=object)
    data[(data != data)] = None
    values = list(map(tuple, data))

    total = len(values)
    progress_bar["maximum"] = max(total, 1)
    progress_bar["value"] = 0

    placeholders = ",".join(["?"] * len(columnas_sql))
    cols = ",".join(columnas_sql)
    sql = f"INSERT INTO {tabla_destino} ({cols}) VALUES ({placeholders})"

    CHUNK = 5000

    try:
        for start in range(0, total, CHUNK):
            if cancelar:
                label_progreso.config(text="🚫 Carga cancelada por el usuario.", fg="#c0392b")
                conn.rollback()
                conn.close()
                return False

            end = min(start + CHUNK, total)
            cursor.executemany(sql, values[start:end])
            conn.commit()

            progress_bar["value"] = end
            label_progreso.config(text=f"Insertando registro {end} de {total}...")
            ventana.update_idletasks()

    except Exception as e:
        conn.rollback()
        conn.close()
        raise e

    conn.close()
    return True


# =========================
# Procesamiento por extensión
# =========================
def construir_df_desde_archivo(ruta, tipo_archivo):
    ext = os.path.splitext(ruta)[1].lower()
    if ext == ".txt":
        return txt_a_dataframe(ruta, tipo_archivo)
    if ext == ".xlsb":
        return xlsb_a_dataframe(ruta, tipo_archivo)
    raise ValueError("Solo se aceptan archivos .txt o .xlsb")


# =========================
# GUI acciones
# =========================
def seleccionar_archivo():
    ruta = filedialog.askopenfilename(filetypes=[
        ("Archivos TXT o XLSB", "*.txt *.xlsb"),
        ("TXT", "*.txt"),
        ("XLSB", "*.xlsb"),
    ])
    entry_ruta.delete(0, tk.END)
    entry_ruta.insert(0, ruta)

def procesar_archivo():
    global cancelar
    cancelar = False

    ruta = entry_ruta.get()
    tipo = combo_tipo.get()
    pwd = entry_pwd.get()

    if not ruta or not tipo or not pwd:
        messagebox.showerror("Error", "Por favor completa todos los campos.")
        return

    try:
        label_progreso.config(text="Procesando archivo...", fg="#1f4e79")
        ventana.update_idletasks()

        df = construir_df_desde_archivo(ruta, tipo)
        if df.empty:
            messagebox.showerror("Error", "El archivo no generó registros válidos.")
            return

        ruta_excel = os.path.splitext(ruta)[0] + "_FORMATEADO.xlsx"
        guardar_excel_rapido(df, ruta_excel)

        label_progreso.config(text="Cargando a SQL Server...", fg="#1f4e79")
        ventana.update_idletasks()

        ok = insertar_en_sql(df, tipo, pwd)
        if not ok:
            return

        label_progreso.config(text="✅ ¡Carga completada!", fg="#1e7e34")
        messagebox.showinfo("Éxito", f"Excel generado:\n{ruta_excel}\n\nRegistros cargados: {len(df)}")

    except Exception as e:
        messagebox.showerror("Error", str(e))


# ============================================================
#   GUI CORPORATIVA (SOLO DISEÑO) - SIN CAMBIAR FUNCIONAMIENTO
# ============================================================
ventana = tk.Tk()
ventana.title("Cargar Comisiones - Grupo Comercial Ideal")
ventana.geometry("960x560")
ventana.resizable(False, False)

BG = "#eef2f6"
WHITE = "#ffffff"
NAVY = "#0b2e4a"
NAVY2 = "#123a5a"
GRAY = "#6b7785"

ventana.configure(bg=BG)

# Header
header = tk.Frame(ventana, bg=NAVY, height=75)
header.pack(side="top", fill="x")
header.pack_propagate(False)

# Logo en header (en lugar del texto)
header_logo_tk = None
try:
    logo_header_path = os.path.join(os.path.dirname(__file__), "logo_header.png")
    if os.path.exists(logo_header_path):
        img = Image.open(logo_header_path).convert("RGBA")
        img = img.resize((280, 60))
        header_logo_tk = ImageTk.PhotoImage(img)
except:
    header_logo_tk = None

if header_logo_tk:
    tk.Label(header, image=header_logo_tk, bg=NAVY).pack(side="left", padx=16)
else:
    tk.Label(header, text="GRUPO COMERCIAL IDEAL", bg=NAVY, fg="white",
             font=("Arial", 14, "bold")).pack(side="left", padx=16)

# Main
main = tk.Frame(ventana, bg=BG)
main.pack(fill="both", expand=True, padx=18, pady=18)

tk.Label(main, text="CARGAR COMISIONES", bg=BG, fg=NAVY,
         font=("Arial", 28, "bold")).pack(pady=(6, 18))

# Card con borde estilo sombra
shadow = tk.Frame(main, bg="#d6dde6")
shadow.pack(pady=0)

card = tk.Frame(shadow, bg=WHITE, padx=18, pady=18)
card.pack(padx=2, pady=2)

# Grid settings
card.grid_columnconfigure(0, weight=0)
card.grid_columnconfigure(1, weight=1)
card.grid_columnconfigure(2, weight=0)

lbl_font = ("Arial", 12, "bold")

# Styles barra progreso
style = ttk.Style()
style.theme_use("clam")
style.configure("green.Horizontal.TProgressbar",
                background="#27ae60",
                troughcolor="#d8d8d8",
                thickness=18)

# Row 0: Archivo
tk.Label(card, text="Archivo TXT o XLSB:", font=lbl_font, bg=WHITE).grid(row=0, column=0, sticky="e", padx=12, pady=10)
entry_ruta = tk.Entry(card, font=("Arial", 11))
entry_ruta.grid(row=0, column=1, sticky="ew", padx=10, pady=10)

btn_buscar = tk.Button(card, text="Buscar", command=seleccionar_archivo,
                       bg=NAVY2, fg="white", font=("Arial", 10, "bold"),
                       width=12, relief="flat", cursor="hand2")
btn_buscar.grid(row=0, column=2, padx=10, pady=10, sticky="e")

# Row 1: Tipo
tk.Label(card, text="Tipo de archivo:", font=lbl_font, bg=WHITE).grid(row=1, column=0, sticky="e", padx=12, pady=10)
combo_tipo = ttk.Combobox(card, values=["INICIALES", "PERMANENCIA", "PERMANENCIA 2", "RECARGAS"],
                          state="readonly")
combo_tipo.grid(row=1, column=1, sticky="ew", padx=10, pady=10)

# Row 2: Usuario + SUBIR
tk.Label(card, text="USUARIO:", font=lbl_font, bg=WHITE).grid(row=2, column=0, sticky="e", padx=12, pady=10)
tk.Label(card, text="sa", bg=WHITE, fg=GRAY, font=("Arial", 12, "bold")).grid(row=2, column=1, sticky="w", padx=10, pady=10)

btn_subir = tk.Button(card, text="SUBIR", command=procesar_archivo,
                      bg="#2ecc71", fg="white", font=("Arial", 11, "bold"),
                      width=14, relief="flat", cursor="hand2")
btn_subir.grid(row=2, column=2, padx=10, pady=6, sticky="e")

# Row 3: Contraseña + CANCELAR
tk.Label(card, text="CONTRASEÑA:", font=lbl_font, bg=WHITE).grid(row=3, column=0, sticky="e", padx=12, pady=10)
entry_pwd = tk.Entry(card, show="*", width=25, font=("Arial", 11))
entry_pwd.grid(row=3, column=1, sticky="w", padx=10, pady=10)

btn_cancelar = tk.Button(card, text="CANCELAR", command=cancelar_carga,
                         bg="#e74c3c", fg="white", font=("Arial", 11, "bold"),
                         width=14, relief="flat", cursor="hand2")
btn_cancelar.grid(row=3, column=2, padx=10, pady=6, sticky="e")

# Progress
progress_bar = ttk.Progressbar(card, orient="horizontal", length=840, mode="determinate",
                               style="green.Horizontal.TProgressbar")
progress_bar.grid(row=4, column=0, columnspan=3, pady=(18, 8))

label_progreso = tk.Label(card, text="", bg=WHITE, fg="#1f4e79", font=("Arial", 11, "bold"))
label_progreso.grid(row=5, column=0, columnspan=3, pady=(2, 2))

ventana.mainloop()
