import os
import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

# =========================
# Config columnas PP/BP
# =========================
PP_LIST = [1, 2, 3, 4, 5, 6, 7]

PP_MONTO_COL = {
    1: "MONTO_REC_PP1",
    2: "MONTO_REC_ PP2",
    3: "MONTO_REC_ PP3",
    4: "MONTO_REC_ PP4",
    5: "MONTO_REC_ PP5",
    6: "MONTO_REC_ PP6",
    7: "MONTO_REC_ PP7",
}
PP_REC_TOTAL_COL = {pp: f"REC_TOTAL_ PP{pp}" for pp in PP_LIST}  # (ya no se usa para INGRESO_TOTAL)
PP_PCT_COL = {pp: f"PCTJE_COM_REC_PP{pp}" for pp in PP_LIST}
PP_EST_COL = {pp: f"ESTATUS_REC_PP{pp}" for pp in PP_LIST}
PP_MOT_COL = {pp: f"MOTIVO_RECHAZO_PP{pp}" for pp in PP_LIST}
PP_MES_COL = {
    1: "MES_REC_PP1",
    2: "MES_REC_PP2",
    3: "MES_REC_PP3",
    4: "MES_PP4",
    5: "MES_PP5",
    6: "MES_PP6",
    7: "MES_PP7",
}

BP1_COLS = ["ESTATUS_BP1", "MOTIVO_RECHAZO_BP1", "MONTO_BP1", "PP_BP1", "MES_BP1"]
BP2_COLS = ["ESTATUS_BP2", "MOTIVO_RECHAZO_BP2", "MONTO_BP2", "PP_BP2", "MES_BP2"]

DATE_PRIORITY_COLS = [
    "FECHA_PRIM_ING",
    "FECHA_CAPTURA",
    "FECHA_EXITOSO",
    "FECHA_PROC_EXITOSO",
    "FECHA_ACTIVACION",
    "FECHA_ALTA",
    "FECHA_PORTOUT",
]

# =========================
# Helpers rápidos
# =========================
def to_float_series(x: pd.Series) -> pd.Series:
    s = x.astype(str).str.strip()
    s = s.str.replace("%", "", regex=False)
    s = s.str.replace(",", "", regex=False)
    s = s.replace({"": None, "None": None, "nan": None, "NaN": None})
    return pd.to_numeric(s, errors="coerce")

def excel_serial_to_datetime(series: pd.Series) -> pd.Series:
    """Serial Excel (ej 45323) o texto a datetime."""
    if series is None:
        return None

    s = series.copy()
    nums = pd.to_numeric(s, errors="coerce")
    is_excel = nums.notna() & (nums > 59) & (nums < 90000)

    out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")

    if is_excel.any():
        out.loc[is_excel] = pd.to_datetime(
            nums.loc[is_excel], unit="D", origin="1899-12-30", errors="coerce"
        )

    other = ~is_excel
    if other.any():
        out.loc[other] = pd.to_datetime(s.loc[other], errors="coerce", dayfirst=False)

    return out

def read_any(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext == ".csv":
        return pd.read_csv(path, dtype=object, encoding="utf-8-sig")
    if ext in [".xlsx", ".xls", ".xlsb"]:
        engine = "pyxlsb" if ext == ".xlsb" else None
        return pd.read_excel(path, dtype=object, engine=engine)
    raise ValueError(f"Extensión no soportada: {path}")

def compute_row_max_date(df: pd.DataFrame) -> pd.Series:
    """Fecha fila = máximo entre columnas de fecha disponibles (vectorizado)."""
    cols = [c for c in DATE_PRIORITY_COLS if c in df.columns]
    if not cols:
        return pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")

    dt_cols = []
    for c in cols:
        dt_cols.append(excel_serial_to_datetime(df[c]))

    dt_df = pd.concat(dt_cols, axis=1)
    return dt_df.max(axis=1)

def non_empty_mask(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    """
    True si la fila tiene ALGO en cualquiera de esas columnas.
    Considera vacío: NaN, "", "nan", "none", "null".
    """
    cols = [c for c in cols if c in df.columns]
    if not cols:
        return pd.Series(False, index=df.index)

    mask = pd.Series(False, index=df.index)
    for c in cols:
        s = df[c]
        m = s.notna() & s.astype(str).str.strip().ne("")
        low = s.astype(str).str.strip().str.lower()
        m = m & ~low.isin({"nan", "none", "null"})
        mask = mask | m
    return mask

def pp_block_cols(pp: int) -> list[str]:
    return [
        PP_EST_COL[pp],
        PP_MOT_COL[pp],
        PP_MONTO_COL[pp],
        PP_PCT_COL[pp],
        PP_REC_TOTAL_COL[pp],  # se conserva por si existe, aunque no lo usemos en ingreso
        PP_MES_COL[pp],
    ]

# =========================
# ✅ INGRESO_TOTAL (lo que tú pediste)
# MONTO_COM_INIC + MONTO_REC_PP1..PP7 + MONTO_BP1 + MONTO_BP2
# =========================
def recompute_ingreso_total(df: pd.DataFrame) -> pd.DataFrame:
    ingreso = to_float_series(df.get("MONTO_COM_INIC", pd.Series([None] * len(df)))).fillna(0)

    for pp in PP_LIST:
        col_monto = PP_MONTO_COL.get(pp)
        if col_monto and col_monto in df.columns:
            ingreso = ingreso + to_float_series(df[col_monto]).fillna(0)

    if "MONTO_BP1" in df.columns:
        ingreso = ingreso + to_float_series(df["MONTO_BP1"]).fillna(0)

    if "MONTO_BP2" in df.columns:
        ingreso = ingreso + to_float_series(df["MONTO_BP2"]).fillna(0)

    df["INGRESO_TOTAL"] = ingreso
    return df

# =========================
# Merge optimizado (rápido)
# =========================
def merge_masters_fast(paths: list[str], progress_cb=None) -> pd.DataFrame:
    dfs = []
    n = max(len(paths), 1)

    # 1) Leer y preparar
    for i, p in enumerate(paths, start=1):
        if callable(progress_cb):
            progress_cb(int((i - 1) / n * 35), f"Leyendo: {os.path.basename(p)}")

        df = read_any(p)
        if "LINEA" not in df.columns:
            raise ValueError(f"El archivo no trae columna LINEA: {p}")

        df["__ROW_DATE__"] = compute_row_max_date(df)
        dfs.append(df)

    if callable(progress_cb):
        progress_cb(40, "Apilando archivos...")

    all_df = pd.concat(dfs, ignore_index=True)

    # 2) Ordenar una sola vez por fecha (estable)
    if callable(progress_cb):
        progress_cb(50, "Ordenando por fecha...")

    all_df["__ORDER__"] = range(len(all_df))
    all_df = all_df.sort_values(["__ROW_DATE__", "__ORDER__"], kind="mergesort")

    # 3) Base general: registro más nuevo por LINEA
    if callable(progress_cb):
        progress_cb(60, "Consolidando base por LINEA...")

    base = all_df.drop_duplicates(subset=["LINEA"], keep="last").copy()
    base = base.set_index("LINEA", drop=False)

    # 4) Para cada PP: tomar el registro más nuevo con data en ese PP y actualizar columnas
    step = 30 / 9.0
    pval = 60.0

    for pp in PP_LIST:
        cols = pp_block_cols(pp)
        cols_present = [c for c in cols if c in all_df.columns]
        if not cols_present:
            pval += step
            continue

        mask = non_empty_mask(all_df, cols_present)
        if mask.any():
            picked = all_df.loc[mask, ["LINEA"] + cols_present + ["__ROW_DATE__", "__ORDER__"]]
            picked = picked.sort_values(["__ROW_DATE__", "__ORDER__"], kind="mergesort")
            picked = picked.drop_duplicates(subset=["LINEA"], keep="last").set_index("LINEA")

            for c in cols_present:
                src = picked[c]
                m = src.notna() & src.astype(str).str.strip().ne("") & ~src.astype(str).str.strip().str.lower().isin({"nan","none","null"})
                idx = m.index[m]
                if len(idx) > 0:
                    base.loc[idx, c] = src.loc[idx]

        pval += step
        if callable(progress_cb):
            progress_cb(int(pval), f"Aplicando PP{pp}...")

    # 5) BP1 y BP2
    for label, cols in [("BP1", BP1_COLS), ("BP2", BP2_COLS)]:
        cols_present = [c for c in cols if c in all_df.columns]
        if cols_present:
            mask = non_empty_mask(all_df, cols_present)
            if mask.any():
                picked = all_df.loc[mask, ["LINEA"] + cols_present + ["__ROW_DATE__", "__ORDER__"]]
                picked = picked.sort_values(["__ROW_DATE__", "__ORDER__"], kind="mergesort")
                picked = picked.drop_duplicates(subset=["LINEA"], keep="last").set_index("LINEA")

                for c in cols_present:
                    src = picked[c]
                    m = src.notna() & src.astype(str).str.strip().ne("") & ~src.astype(str).str.strip().str.lower().isin({"nan","none","null"})
                    idx = m.index[m]
                    if len(idx) > 0:
                        base.loc[idx, c] = src.loc[idx]

        pval += step
        if callable(progress_cb):
            progress_cb(int(pval), f"Aplicando {label}...")

    # 6) Recalcular ingreso total (con tus columnas)
    if callable(progress_cb):
        progress_cb(95, "Recalculando INGRESO_TOTAL...")

    out = base.reset_index(drop=True)

    # limpiar auxiliares
    for c in ["__ROW_DATE__", "__ORDER__"]:
        if c in out.columns:
            out.drop(columns=[c], inplace=True)

    out = recompute_ingreso_total(out)

    if callable(progress_cb):
        progress_cb(100, "Listo ✅")

    return out

# =========================
# GUI
# =========================
class MergeApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Fusionar MAESTROS (rápido)")
        self.geometry("840x420")
        self.resizable(False, False)

        self.paths = []
        self.paths_label = tk.StringVar(value="No has seleccionado archivos.")

        top = tk.Frame(self)
        top.pack(fill="x", padx=20, pady=15)

        tk.Label(top, text="Fusionador de MAESTROS (Optimizado)", font=("Arial", 18, "bold")).pack(anchor="w")
        tk.Label(
            top,
            text="Une por LINEA. Si se repite un PP/BP, gana el registro con fecha más adelantada.",
            fg="gray"
        ).pack(anchor="w", pady=(4, 0))

        card = tk.Frame(self, bd=1, relief="solid")
        card.pack(fill="both", expand=True, padx=20, pady=10)

        row1 = tk.Frame(card)
        row1.pack(fill="x", padx=12, pady=(12, 8))

        tk.Button(row1, text="Seleccionar archivos", width=22, command=self.pick_files).pack(side="left")
        tk.Label(row1, textvariable=self.paths_label, anchor="w").pack(side="left", padx=12, fill="x", expand=True)

        row2 = tk.Frame(card)
        row2.pack(fill="x", padx=12, pady=(0, 8))

        self.btn_merge = tk.Button(row2, text="Fusionar y Guardar", width=22, command=self.merge_and_save, state="disabled")
        self.btn_merge.pack(side="left")

        tk.Button(row2, text="Salir", width=12, command=self.destroy).pack(side="left", padx=10)

        self.progress = ttk.Progressbar(card, orient="horizontal", length=100, mode="determinate")
        self.progress.pack(fill="x", padx=12, pady=(20, 6))

        self.status = tk.Label(card, text="Listo", fg="gray", anchor="w")
        self.status.pack(fill="x", padx=12, pady=(0, 12))

        tips = tk.Frame(card)
        tips.pack(fill="x", padx=12, pady=(0, 12))
        tk.Label(tips, text="INGRESO_TOTAL:", font=("Arial", 10, "bold")).pack(anchor="w")
        tk.Label(
            tips,
            text="Se calcula con MONTO_COM_INIC + MONTO_REC_PP1..PP7 + MONTO_BP1 + MONTO_BP2 (NO usa REC_TOTAL_).",
            fg="gray",
        ).pack(anchor="w")

    def pick_files(self):
        paths = filedialog.askopenfilenames(
            title="Selecciona tus MAESTRO (CSV/XLSX/XLS/XLSB)",
            filetypes=[
                ("MAESTRO (CSV/Excel)", "*.csv *.xlsx *.xls *.xlsb"),
                ("CSV", "*.csv"),
                ("Excel", "*.xlsx *.xls *.xlsb"),
            ],
        )
        if not paths:
            return

        self.paths = list(paths)
        self.paths_label.set(f"{len(self.paths)} archivos seleccionados" if len(self.paths) > 1 else os.path.basename(self.paths[0]))
        self.btn_merge.config(state="normal")
        self.status.config(text="Archivos listos. Presiona 'Fusionar y Guardar'.")
        self.progress["value"] = 0

    def _progress_cb(self, pct, msg):
        self.progress["value"] = max(0, min(100, pct))
        self.status.config(text=msg)
        self.update_idletasks()

    def merge_and_save(self):
        if not self.paths:
            messagebox.showwarning("Falta", "Selecciona archivos primero.")
            return

        out_path = filedialog.asksaveasfilename(
            title="Guardar MAESTRO unificado",
            defaultextension=".csv",
            initialfile="MAESTRO_UNIFICADO.csv",
            filetypes=[("CSV", "*.csv")],
        )
        if not out_path:
            return

        self.btn_merge.config(state="disabled")
        self.progress["value"] = 0
        self.status.config(text="Procesando...")
        self.update_idletasks()

        try:
            merged = merge_masters_fast(self.paths, progress_cb=self._progress_cb)
            self._progress_cb(98, "Guardando CSV...")
            merged.to_csv(out_path, index=False, encoding="utf-8-sig")
            self._progress_cb(100, "Listo ✅")
            messagebox.showinfo("Listo", f"Se creó:\n{out_path}\n\nFilas: {len(merged):,}")
        except Exception as e:
            messagebox.showerror("Error", str(e))
            self.status.config(text="Ocurrió un error.")
            self.progress["value"] = 0
        finally:
            self.btn_merge.config(state="normal")


if __name__ == "__main__":
    # pip install pandas openpyxl pyxlsb
    MergeApp().mainloop()
