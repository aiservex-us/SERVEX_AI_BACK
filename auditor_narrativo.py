import os
from typing import TypedDict
from dotenv import load_dotenv
from langchain_groq import ChatGroq
from langgraph.graph import StateGraph, START, END

load_dotenv()

class State(TypedDict):
    resumen_input: dict
    respuesta: str

llm = ChatGroq(
    model="llama-3.1-8b-instant",
    temperature=0.0,
    groq_api_key=os.getenv("GROQ_API_KEY")
)

def auditor_node(state: State):

    resumen = state["resumen_input"]

    prompt = f"""
Eres el Ingeniero Senior de Auditoría de SERVEX_AI.

Redacta un REPORTE TÉCNICO EJECUTIVO basado EXCLUSIVAMENTE en el siguiente resumen:

{resumen}

Estructura obligatoria:
1. Resumen General
2. Análisis de Base
3. Análisis de Upcharges
4. Conclusión Ejecutiva

No inventes datos.
No uses tono conversacional.
Documento formal empresarial.
"""

    resultado = llm.invoke(prompt)

    return {"respuesta": resultado.content}

workflow = StateGraph(State)
workflow.add_node("generador_reporte", auditor_node)
workflow.add_edge(START, "generador_reporte")
workflow.add_edge("generador_reporte", END)
app = workflow.compile()

def generar_auditoria_narrativa(resumen_ejecutivo: dict) -> str:
    resultado = app.invoke({
        "resumen_input": resumen_ejecutivo
    })
    return resultado["respuesta"]