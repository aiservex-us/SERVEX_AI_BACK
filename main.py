import os
import io
import json
from typing import List
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client
from dotenv import load_dotenv

# Importamos la lógica exacta de tu script Reestructure_xml.py
from Reestructure_xml import (
    obtener_datos_maestros,
    auditar_csv_logic,
    extraer_pim_xml,
    generar_xml_editado,
    recalcular_upcharge_dinamico,
    limpiar_monto
)

# --- INTEGRACIÓN CON EL AGENTE NARRATIVO ---
# Importamos el grafo (app) del segundo archivo
from Reestructure_model import app as agent_workflow

# Cargamos entorno
load_dotenv()

app = FastAPI(title="SERVEX_AI - LESRO Data Engine API")

# Configuración de CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://servex-ai-iota.vercel.app",  # Tu frontend en Vercel
        "http://localhost:3000",              # Para pruebas locales
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Inicializamos cliente Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

@app.post("/audit-process")
async def audit_process(file: UploadFile = File(...)):
    # 1. Validación de extensión
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload a CSV.")

    try:
        # 2. Lectura del archivo subido
        content = await file.read()
        csv_usuario = content.decode("utf-8-sig")

        # 3. Obtención de datos maestros
        maestro = obtener_datos_maestros()
        
        # 4. Auditoría de discrepancias
        discrepancias = auditar_csv_logic(csv_usuario, maestro["csv_raw"])
        
        if not discrepancias:
            return {
                "status": "success", 
                "message": "The file matches the master record. No changes needed.",
                "data": {"skus_affected": 0, "audit_report": []}
            }

        # 5. Procesamiento PIM
        skus_err = list({d["sku"] for d in discrepancias})
        pim_data = {p["sku"]: p for p in extraer_pim_xml(maestro["xml_raw"], skus_err)}

        # 6. Construcción del Reporte Detallado
        reporte_final = []
        for d in discrepancias:
            sku = d["sku"]
            pim = pim_data.get(sku, {})
            base_referencia = pim.get("base_price", 0)
            
            grados_disc = []
            nuevo_base_csv = None

            for i, h in enumerate(d["headers"]):
                if "PRICE GRADE 02" in h.strip().upper():
                    val_csv_grade2 = limpiar_monto(d["row_user"][i])
                    if val_csv_grade2 != base_referencia:
                        nuevo_base_csv = val_csv_grade2
                        base_referencia = val_csv_grade2 
                    break

            for i, h in enumerate(d["headers"]):
                h_clean = h.strip()
                if h_clean.upper().startswith("PRICE GRADE"):
                    num_str = "".join(filter(str.isdigit, h_clean))
                    num_int = int(num_str) if num_str else 0
                    
                    if num_int == 2 or num_int > 10: continue

                    val_csv = limpiar_monto(d["row_user"][i])
                    lbl = f"Price Grade {num_str.zfill(2) if num_int < 10 else num_int}"
                    info_xml = pim.get("grados", {}).get(lbl, {"xml_upcharge": "NOT_FOUND", "xml_total_calculado": "NOT_FOUND"})
                    
                    status = "OK"
                    if info_xml["xml_total_calculado"] != "NOT_FOUND":
                        status = "OK" if val_csv == info_xml["xml_total_calculado"] else "MISMATCH"
                    else: 
                        status = "NOT_IN_PIM"

                    if status != "OK":
                        grados_disc.append({
                            "grado": h_clean, 
                            "csv_user_total": val_csv,
                            "xml_upcharge_sugerido": recalcular_upcharge_dinamico(val_csv, base_referencia),
                            "xml_expected_total": info_xml["xml_total_calculado"], 
                            "result": status
                        })

            if grados_disc or nuevo_base_csv is not None:
                reporte_final.append({
                    "sku": sku, 
                    "nuevo_base_csv": nuevo_base_csv,
                    "comparativa_grados_xml": grados_disc
                })

        # 7. Generar el nuevo XML
        xml_final_content = generar_xml_editado(maestro["xml_raw"], reporte_final)

        # 8. Sincronización con Supabase (Datos Técnicos)
        supabase.from_('ClientsSERVEX') \
            .update({
                "audit_report_json": reporte_final,
                "xml_actualizer_raw": xml_final_content
            }) \
            .eq('company_name', 'LESRO') \
            .execute()

        # ======================================================
        # 🚀 ACTIVACIÓN AUTOMÁTICA DEL AGENTE NARRATIVO
        # ======================================================
        print("🤖 Iniciando SVX Copilot para generar informe narrativo...")
        agent_workflow.invoke({
            "raw_data": reporte_final, # Le pasamos los datos recién procesados
            "summary_text": "",
            "reporte_final": ""
        })
        # ======================================================

        # 9. Respuesta enriquecida para el Front-end
        return {
            "status": "success",
            "message": "Data processed, XML updated, and SVX Copilot report generated successfully.",
            "data": {
                "skus_affected": len(reporte_final),
                "audit_report": reporte_final
            }
        }

    except Exception as e:
        print(f"❌ Error en SERVEX_AI Engine: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)