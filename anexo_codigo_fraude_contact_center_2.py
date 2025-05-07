
# === BLOQUE 4: Interacciones fuera del turno asignado ===
turnos["Fecha"] = pd.to_datetime(turnos["Fecha"]).dt.date
interacciones["Fecha"] = pd.to_datetime(interacciones["Fecha"]).dt.date
interacciones_turno = pd.merge(interacciones, turnos[["Id_Empleado", "Fecha", "Hora Inicio", "Hora Fin"]],
                               on=["Id_Empleado", "Fecha"], how="left")
interacciones_turno["Hora interacción"] = pd.to_datetime(interacciones_turno["Fecha y hora de interacción"])
interacciones_turno["Fuera_de_turno"] = interacciones_turno.apply(
    lambda x: hora_fuera_turno(x["Hora interacción"], x["Hora Inicio"], x["Hora Fin"]), axis=1)
interacciones_fuera_turno = interacciones_turno[interacciones_turno["Fuera_de_turno"] == True]

# === BLOQUE 5: Detección de llamadas colgadas excesivas ===
colgadas["Fecha"] = pd.to_datetime(colgadas["Fecha y hora de interacción"]).dt.date
interacciones_por_agente = interacciones.groupby("Id_Empleado").size().reset_index(name="Total_Interacciones")
colgadas_por_agente = colgadas.groupby("Id_Empleado").size().reset_index(name="Colgadas")
colgado_ratio = pd.merge(colgadas_por_agente, interacciones_por_agente, on="Id_Empleado", how="left")
colgado_ratio["% Colgadas"] = (colgado_ratio["Colgadas"] / colgado_ratio["Total_Interacciones"]) * 100
colgado_ratio["Sospechoso"] = colgado_ratio["% Colgadas"] >= 15

# === BLOQUE 6: Cruce CSAT, duración e impacto ===
csat["Satisfacción"] = pd.to_numeric(csat["Satisfacción"], errors="coerce")
csat["Fecha"] = pd.to_datetime(csat["Fecha"]).dt.date
interacciones_csat = pd.merge(interacciones, csat[["Id de empleado", "Id del caso", "Fecha", "Satisfacción"]],
                              left_on=["Id_Empleado", "Id del caso", "Fecha"],
                              right_on=["Id de empleado", "Id del caso", "Fecha"],
                              how="left")
interacciones_csat_creditos = pd.merge(interacciones_csat,
                                       creditos[["Id_Empleado", "Id del caso", "Fecha", "Crédito otorgado (USD)"]],
                                       on=["Id_Empleado", "Id del caso", "Fecha"], how="left")
interacciones_csat_creditos["Crédito otorgado (USD)"] = pd.to_numeric(interacciones_csat_creditos["Crédito otorgado (USD)"], errors="coerce")
umbral_duracion_baja = interacciones["Duración"].quantile(0.25)
interacciones_csat_creditos["Alta_Satisfacción"] = interacciones_csat_creditos["Satisfacción"] >= 9
interacciones_csat_creditos["Baja_Satisfacción"] = interacciones_csat_creditos["Satisfacción"] <= 4
interacciones_csat_creditos["Duración_Baja"] = interacciones_csat_creditos["Duración"] < umbral_duracion_baja
interacciones_csat_creditos["Crédito"] = interacciones_csat_creditos["Crédito otorgado (USD)"].notnull()
caso_1 = interacciones_csat_creditos[
    (interacciones_csat_creditos["Alta_Satisfacción"]) &
    (interacciones_csat_creditos["Duración_Baja"]) &
    (interacciones_csat_creditos["Crédito"])
]
caso_2 = interacciones_csat_creditos[
    (interacciones_csat_creditos["Baja_Satisfacción"]) &
    (~interacciones_csat_creditos["Duración_Baja"]) &
    (~interacciones_csat_creditos["Crédito"])
]
sospechosos_satisfaccion = pd.concat([caso_1, caso_2])

# === BLOQUE 7: Perfilamiento de agentes ===
from functools import reduce
tmo_riesgo = outliers_tmo[["Id_Empleado"]].copy()
tmo_riesgo["TMO_atipico"] = 1
creditos_riesgo = creditos_sospechosos[["Id_Empleado"]].copy()
creditos_riesgo["Creditos_sospechosos"] = 1
fuera_turno_riesgo = interacciones_fuera_turno[["Id_Empleado"]].copy()
fuera_turno_riesgo["Fuera_de_turno"] = 1
colgadas_riesgo = colgado_ratio[colgado_ratio["Sospechoso"] == True][["Id_Empleado"]].copy()
colgadas_riesgo["Colgadas_excesivas"] = 1
satisfaccion_riesgo = sospechosos_satisfaccion[["Id_Empleado"]].copy()
satisfaccion_riesgo["Cruce_CSAT_Duracion_Credito"] = 1
listas_riesgo = [tmo_riesgo, creditos_riesgo, fuera_turno_riesgo, colgadas_riesgo, satisfaccion_riesgo]
perfil_riesgo = reduce(lambda left, right: pd.merge(left, right, on="Id_Empleado", how="outer"), listas_riesgo).fillna(0)
perfil_riesgo["Score_Riesgo"] = perfil_riesgo[
    ["TMO_atipico", "Creditos_sospechosos", "Fuera_de_turno", 
     "Colgadas_excesivas", "Cruce_CSAT_Duracion_Credito"]
].sum(axis=1)
