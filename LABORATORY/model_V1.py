import os
import json
from dotenv import load_dotenv
from typing import TypedDict
from langgraph.graph import StateGraph, END
from langchain_groq import ChatGroq
from supabase import create_client, Client

# ========================
# 1. Configuración
# ========================
load_dotenv()
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Modelo LLM para la narrativa
llm = ChatGroq(model="Llama-3.1-8B-Instant", api_key=GROQ_API_KEY, temperature=0.1)

# ========================
# 2. Estado
# ========================
class ReportState(TypedDict):
    raw_data: list
    summary_text: str 
    reporte_final: str

# ========================
# 3. Nodos de Procesamiento
# ========================

def fetch_data_node(state: ReportState) -> ReportState:
    """Extrae los datos de Supabase"""
    print("🔍 Consultando datos en Supabase...")
    try:
        response = supabase.table('ClientsSERVEX').select("audit_report_json").execute()
        if not response.data: 
            raise Exception("No se encontraron registros en la tabla.")
        
        state["raw_data"] = response.data[0].get("audit_report_json", [])
        return state
    except Exception as e:
        print(f"Error de conexión: {e}")
        return state

def summarize_data_node(state: ReportState) -> ReportState:
    """Procesamiento lógico: Extrae cambios en Precio Base y Grados de Precio."""
    print("📊 Analizando variaciones (Base + Grados)...")
    data = state["raw_data"]
    
    total_skus = len(data)
    cambios_detectados = []

    for item in data:
        sku = item.get("sku", "N/A")
        
        # 1. Detectar cambio en Precio Base
        base_csv = item.get("nuevo_base_csv")
        if base_csv is not None:
            cambios_detectados.append({
                "sku": sku,
                "tipo": "Precio Base",
                "valor": base_csv,
                "nota": "Actualización de precio base detectada"
            })
        
        # 2. Detectar variaciones en Grados
        grados = item.get("comparativa_grados_xml", [])
        for g in grados:
            if g.get("result") == "MISMATCH":
                cambios_detectados.append({
                    "sku": sku,
                    "tipo": "Grado de Precio",
                    "grado": g.get("grado"),
                    "enviado": g.get("csv_user_total"),
                    "esperado": g.get("xml_expected_total"),
                    "diferencia": round(g.get("csv_user_total", 0) - g.get("xml_expected_total", 0), 2)
                })

    state["summary_text"] = json.dumps({
        "total_revisados": total_skus,
        "total_cambios": len(cambios_detectados),
        "detalles": cambios_detectados
    })
    return state

def generate_report_node(state: ReportState) -> ReportState:
    """Genera la narrativa proactiva de SVX Copilot"""
    print("🤖 SVX Copilot redactando notificación...")
    
    datos = json.loads(state["summary_text"])
    
    prompt = f"""Eres SVX Copilot, el asistente de inteligencia operativa de SERVEX_AI.
Tu función es supervisar la integridad de los datos entre archivos CSV y el núcleo del sistema (XML), operando como un sistema de alta sofisticación técnica.

DATOS DE AUDITORÍA:
{json.dumps(datos, indent=2)}

MÉTRICA DE RENDIMIENTO MANUAL (Referencia):

Tiempo manual por SKU: 15 a 30 segundos.

Volumen actual: 590 SKUs.

Tiempo total manual estimado: ~2.5 a 5 horas hombre.

INSTRUCCIONES DE COMUNICACIÓN:

BREVE INTRODUCCIÓN: Explica que SVX Copilot ha finalizado el análisis de comparación y sincronización de precios.

Nota importante: Subraya que SVX Copilot es un sistema completamente asistido y gestionado por software de alta sofisticación, diseñado a medida para la excelencia en los procesos de Servex US.

RESUMEN DE PROCESAMIENTO: "Se procesaron {datos['total_revisados']} SKUs, identificando {datos['total_cambios']} puntos de atención".

ANÁLISIS DE EFICIENCIA (Ahorro de Tiempo):
Calcula y comunica el tiempo ahorrado. Basado en el volumen de 590 SKUs, indica que el sistema ha automatizado una tarea que manualmente tomaría entre 2.5 y 5 horas críticas de edición, ejecutándola de forma instantánea y precisa.

DETALLE POR TIPO:

Precio Base: Indica detección de actualización en el núcleo.

Grado de Precio: Indica SKU, Grado afectado y la diferencia monetaria exacta.

TONO: Directo, moderno, tecnológico y proactivo.

CIERRE: Instrucción breve para revisar o aprobar los cambios en el panel de control.

RESPUESTA EN ESPAÑOL:
"""

    try:
        respuesta = llm.invoke(prompt).content
        state["reporte_final"] = respuesta
    except Exception:
        state["reporte_final"] = "⚠️ SVX Copilot detectó inconsistencias. Revisa el log de auditoría inmediatamente."
    
    return state

# --- NUEVO NODO DE PERSISTENCIA ---
def save_to_supabase_node(state: ReportState) -> ReportState:
    """Guarda la narrativa final en la columna informa_agent_raw"""
    print("💾 Persistiendo narrativa en informa_agent_raw...")
    try:
        supabase.table('ClientsSERVEX') \
            .update({"informa_agent_raw": state["reporte_final"]}) \
            .eq('company_name', 'LESRO') \
            .execute()
        print("✅ Guardado exitoso en Supabase.")
    except Exception as e:
        print(f"❌ Error al guardar en base de datos: {e}")
    return state

# ========================
# 4. Grafo LangGraph
# ========================
workflow = StateGraph(ReportState)

# Definición de Nodos
workflow.add_node("fetch_data", fetch_data_node)
workflow.add_node("summarize", summarize_data_node)
workflow.add_node("generate_report", generate_report_node)
workflow.add_node("save_to_db", save_to_supabase_node)

# Flujo de Aristas
workflow.set_entry_point("fetch_data")
workflow.add_edge("fetch_data", "summarize")
workflow.add_edge("summarize", "generate_report")
workflow.add_edge("generate_report", "save_to_db")
workflow.add_edge("save_to_db", END)

app = workflow.compile()

# ========================
# 5. Ejecución (Test local)
# ========================
if __name__ == "__main__":
    print("--- INICIANDO SISTEMA DE AUDITORÍA SERVEX ---")
    
    resultado = app.invoke({
        "raw_data": [],
        "summary_text": "",
        "reporte_final": ""
    })
    
    print("\n" + "="*60)
    print("RESUMEN DE AUDITORÍA FINAL (PERSISTIDO):")
    print("="*60 + "\n")
    print(resultado["reporte_final"])
    print("\n" + "="*60)