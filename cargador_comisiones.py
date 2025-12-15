import pandas as pd
import pyodbc
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import os
import re

# =========================
# Validación de fechas válidas para SQL Server
# =========================
def limpiar_fecha(fecha):
    if pd.isnull(fecha):
        return None
    try:
        fecha = pd.to_datetime(fecha, errors="coerce")
        if pd.isnull(fecha): return None
        if fecha.year < 1753 or fecha.year > 9999:
            return None
        return fecha.date()
    except:
        return None

cancelar = False

def cancelar_carga():
    global cancelar
    cancelar = True

# =========================
# Transformar nombre archivo
# =========================
def formatear_nombre_archivo(nombre, tipo_archivo):
    nombre = os.path.basename(nombre).replace(".xlsx", "")
    match = re.search(r'SEM[^\w]*(\d{1,2})[^\w]+([A-Z]{3})[^\w]+(\d{4}).*-\s*([A-Z ]+)', nombre.upper())
    if not match:
        return nombre
    semana, mes_abrev, anio, tipo = match.groups()
    meses = {
        "ENE": "ENERO", "FEB": "FEBRERO", "MAR": "MARZO", "ABR": "ABRIL",
        "MAY": "MAYO", "JUN": "JUNIO", "JUL": "JULIO", "AGO": "AGOSTO",
        "SEP": "SEPTIEMBRE", "OCT": "OCTUBRE", "NOV": "NOVIEMBRE", "DIC": "DICIEMBRE"
    }
    mes_completo = meses.get(mes_abrev, mes_abrev)

    tipo_formateado = tipo.strip().replace(' ', '_')
    if tipo_archivo.upper() == "PERMANENCIA 2":
        tipo_formateado = "PERMANENCIA_2"

    return f"{mes_completo} {anio} SEM {int(semana):02d} - {tipo_formateado}"

# =========================
# Procesar Excel
# =========================
def transformar_archivo(ruta_archivo, tipo_archivo):
    df_raw = pd.read_excel(ruta_archivo, header=1)

    df = pd.DataFrame()
    df["linea"] = df_raw.iloc[1:, 2].astype(str).str.strip()
    df["fecha_portacion"] = pd.to_datetime(df_raw.iloc[1:, 4], errors="coerce").apply(limpiar_fecha)
    df["fecha_primer_ingreso"] = pd.to_datetime(df_raw.iloc[1:, 5], errors="coerce").apply(limpiar_fecha)
    df["estatus_comision"] = df_raw.iloc[1:, 7].astype(str).str.strip()
    df["motivo_rechazo"] = df_raw.iloc[1:, 8].astype(str).str.strip()
    df["tipo_comision"] = df_raw.iloc[1:, 9].astype(str).str.strip()
    df["monto"] = pd.to_numeric(df_raw.iloc[1:, 10], errors="coerce").apply(lambda x: float(x) if pd.notnull(x) else None)
    df["fuerza_venta"] = df_raw.iloc[1:, 1].astype(str).str.strip()
    df["carrier"] = df_raw.iloc[1:, 3].astype(str).str.strip()
    df["archivo"] = formatear_nombre_archivo(ruta_archivo, tipo_archivo)
    df["periodo_participacion"] = pd.to_numeric(df_raw.iloc[1:, 6], errors="coerce").fillna(1).astype(int)

    df = df[df["linea"].notna() & df["linea"].str.strip().ne("nan")]
    df = df.replace({pd.NA: None, "nan": None, "NaN": None, "": None})
    return df

# =========================
# Cargar a SQL Server
# =========================
def insertar_en_sql(df, tipo_archivo, password):
    tabla_destino = {
        "INICIALES": "dbo.tComisionesIniciales",
        "PERMANENCIA": "dbo.tComisionesPermanencia",
        "PERMANENCIA 2": "dbo.tComisionesPermanencia",
        "RECARGAS": "dbo.tComisionesRecargas"
    }[tipo_archivo]

    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER=192.168.10.68;"
        f"DATABASE=DatosLocales;"
        f"UID=sa;"
        f"PWD={password}"
    )

    try:
        conn = pyodbc.connect(conn_str)
    except pyodbc.Error as e:
        if "Login failed for user" in str(e):
            messagebox.showerror("CONTRASEÑA INCORRECTA", "❌ La contraseña ingresada es incorrecta para el usuario 'sa'.")
        else:
            messagebox.showerror("Error de conexión", str(e))
        return False

    cursor = conn.cursor()
    cursor.fast_executemany = True

    total = len(df)
    progress_bar["maximum"] = total
    values = [tuple(row) for _, row in df.iterrows()]

    for i in range(total):
        if cancelar:
            label_progreso.config(text="🚫 Carga cancelada por el usuario.")
            conn.close()
            return False
        if i % 1000 == 0 or i == total - 1:
            progress_bar["value"] = i + 1
            label_progreso.config(text=f"Insertando registro {i + 1} de {total}...")
            ventana.update_idletasks()

    cursor.executemany(f"""
        INSERT INTO {tabla_destino} (
            linea, fecha_portacion, fecha_primer_ingreso, estatus_comision,
            motivo_rechazo, tipo_comision, monto, fuerza_venta,
            carrier, archivo, periodo_participacion
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, values)

    conn.commit()
    conn.close()
    return True

# =========================
# Funciones GUI
# =========================
def seleccionar_archivo():
    ruta = filedialog.askopenfilename(filetypes=[("Archivos Excel", "*.xlsx")])
    entry_ruta.delete(0, tk.END)
    entry_ruta.insert(0, ruta)

def procesar_archivo():
    ruta = entry_ruta.get()
    tipo = combo_tipo.get()
    pwd = entry_pwd.get()

    if not ruta or not tipo or not pwd:
        messagebox.showerror("Error", "Por favor completa todos los campos.")
        return

    try:
        df_transformado = transformar_archivo(ruta, tipo)
        exito = insertar_en_sql(df_transformado, tipo, pwd)
        if not exito:
            return

        label_progreso.config(text="✅ ¡Carga completada!", fg="blue")
        nombre_formateado = formatear_nombre_archivo(ruta, tipo)
        tabla_destino = {
            "INICIALES": "dbo.tComisionesIniciales",
            "PERMANENCIA": "dbo.tComisionesPermanencia",
            "PERMANENCIA 2": "dbo.tComisionesPermanencia",
            "RECARGAS": "dbo.tComisionesRecargas"
        }[tipo]
        messagebox.showinfo("Éxito", f"Archivo: {nombre_formateado}\nTabla destino: {tabla_destino}\nRegistros: {len(df_transformado)}")
    except Exception as e:
        messagebox.showerror("Error", str(e))

# =========================
# GUI: Interfaz estilizada
# =========================
ventana = tk.Tk()
ventana.title("Cargador de Comisiones a SQL Server")
ventana.geometry("680x500")
ventana.configure(bg="#f2f2f2")

# Estilo para la barra de progreso verde
style = ttk.Style()
style.theme_use("clam")
style.configure("green.Horizontal.TProgressbar", background="#27ae60", troughcolor="#d8d8d8", thickness=20)

# Labels y entradas
tk.Label(ventana, text="Archivo Excel:", font=("Arial", 12, "bold"), bg="#f2f2f2").grid(row=0, column=0, sticky="e", padx=10, pady=10)
entry_ruta = tk.Entry(ventana, width=50)
entry_ruta.grid(row=0, column=1)
tk.Button(ventana, text="Buscar", command=seleccionar_archivo, width=10).grid(row=0, column=2, padx=10)

tk.Label(ventana, text="Tipo de archivo:", font=("Arial", 12, "bold"), bg="#f2f2f2").grid(row=1, column=0, sticky="e", padx=10, pady=10)
combo_tipo = ttk.Combobox(ventana, values=["INICIALES", "PERMANENCIA", "PERMANENCIA 2", "RECARGAS"], state="readonly", width=47)
combo_tipo.grid(row=1, column=1)

tk.Label(ventana, text="USUARIO:", font=("Arial", 12, "bold"), bg="#f2f2f2").grid(row=2, column=0, sticky="e", padx=10, pady=10)
tk.Label(ventana, text="sa", font=("Arial", 12, "bold"), bg="#f2f2f2", fg="gray").grid(row=2, column=1, sticky="w")

tk.Label(ventana, text="CONTRASEÑA:", font=("Arial", 12, "bold"), bg="#f2f2f2").grid(row=3, column=0, sticky="e", padx=10, pady=10)
entry_pwd = tk.Entry(ventana, show="*", width=20)
entry_pwd.grid(row=3, column=1, sticky="w")

# Botones
tk.Button(ventana, text="SUBIR", command=procesar_archivo, bg="#2ecc71", fg="white", font=("Arial", 10, "bold"), width=10).grid(row=3, column=2, pady=10)
tk.Button(ventana, text="CANCELAR", command=cancelar_carga, bg="#e74c3c", fg="white", font=("Arial", 10, "bold"), width=10).grid(row=4, column=2)

# Barra de progreso
progress_bar = ttk.Progressbar(ventana, orient="horizontal", length=400, mode="determinate", style="green.Horizontal.TProgressbar")
progress_bar.grid(row=5, column=1, pady=20)

# Etiqueta progreso
label_progreso = tk.Label(ventana, text="", fg="blue", bg="#f2f2f2", font=("Arial", 10, "bold"))
label_progreso.grid(row=6, column=1, pady=10)

ventana.mainloop()
