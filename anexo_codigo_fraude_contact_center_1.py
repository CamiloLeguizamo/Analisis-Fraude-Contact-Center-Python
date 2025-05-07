
# ANEXO DE CÓDIGO PYTHON - TRABAJO DE GRADO
# Autor: Camilo Leguízamo
# Análisis de fraude operativo en Contact Centers con enfoque en Business Intelligence

import pandas as pd
import datetime

# === BLOQUE 1: Filtrado temporal por trimestre ===
# Se toma el trimestre enero-marzo 2025 como rango de análisis
interacciones["Fecha"] = pd.to_datetime(interacciones["Fecha y hora de interacción"]).dt.date
interacciones = interacciones[
    (interacciones["Fecha"] >= datetime.date(2025, 1, 1)) &
    (interacciones["Fecha"] <= datetime.date(2025, 3, 31))
]

# === BLOQUE 2: Identificación de TMO atípico ===
tmo_diario = interacciones.groupby(["Id_Empleado", "Fecha"]).agg({"Duración": "mean"}).reset_index()
q1 = tmo_diario["Duración"].quantile(0.25)
q3 = tmo_diario["Duración"].quantile(0.75)
iqr = q3 - q1
limite_superior = q3 + 1.5 * iqr
outliers_tmo = tmo_diario[tmo_diario["Duración"] > limite_superior]

# === BLOQUE 3: Créditos sospechosos ===
creditos["Fecha"] = pd.to_datetime(creditos["Fecha y hora de interacción"]).dt.date
creditos = creditos.rename(columns={"Id de empleado": "Id_Empleado"})
creditos_interacciones = pd.merge(creditos, interacciones[["Id_Empleado", "Fecha", "Id del caso", "Duración"]],
                                   on=["Id_Empleado", "Fecha", "Id del caso"], how="left")
csat["Fecha"] = pd.to_datetime(csat["Fecha y hora"]).dt.date
creditos_completo = pd.merge(creditos_interacciones, csat[["Id de empleado", "Fecha", "Id del caso", "Satisfacción"]],
                              left_on=["Id_Empleado", "Fecha", "Id del caso"],
                              right_on=["Id de empleado", "Fecha", "Id del caso"], how="left")
umbral_duracion = interacciones["Duración"].quantile(0.25)
creditos_completo["Sospechoso_Duracion"] = creditos_completo["Duración"] < umbral_duracion
creditos_completo["Satisfacción"] = pd.to_numeric(creditos_completo["Satisfacción"], errors="coerce")
creditos_completo["Sospechoso_CSAT"] = creditos_completo["Satisfacción"] >= 9

# === Validación de horario de turno ===
turnos["Fecha"] = pd.to_datetime(turnos["Fecha"]).dt.date
turnos = turnos.rename(columns={"Id de empleado": "Id_Empleado", "Inicio de turno": "Hora Inicio", "Fin de turno": "Hora Fin"})
creditos_completo = pd.merge(creditos_completo, turnos[["Id_Empleado", "Fecha", "Hora Inicio", "Hora Fin"]],
                              on=["Id_Empleado", "Fecha"], how="left")
creditos_completo["Hora interacción"] = pd.to_datetime(creditos_completo["Fecha y hora de interacción"])

def hora_fuera_turno(hora_interaccion, hora_inicio, hora_fin):
    try:
        h_int = pd.to_datetime(hora_interaccion).time()
        h_ini = pd.to_datetime(hora_inicio).time()
        h_fin = pd.to_datetime(hora_fin).time()
        return not (h_ini <= h_int <= h_fin)
    except:
        return True

creditos_completo["Sospechoso_Horario"] = creditos_completo.apply(
    lambda x: hora_fuera_turno(x["Hora interacción"], x["Hora Inicio"], x["Hora Fin"]), axis=1
)

creditos_sospechosos = creditos_completo[
    (creditos_completo["Sospechoso_Duracion"]) |
    (creditos_completo["Sospechoso_CSAT"]) |
    (creditos_completo["Sospechoso_Horario"])
]
