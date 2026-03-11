import os
import json
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client
from dotenv import load_dotenv

from Reestructure_xml import (
    obtener_datos_maestros,
    auditar_csv_logic,
    extraer_pim_xml,
    generar_xml_editado,
    recalcular_upcharge_dinamico,
    limpiar_monto
)

from Reestructure_model import app as agent_workflow

load_dotenv()

app = FastAPI(title="SERVEX_AI - LESRO Data Engine API")

# ================================
# CORS
# ================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # debug mode
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ================================
# Supabase
# ================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("❌ Supabase environment variables missing")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


@app.get("/")
def root():
    return {"status": "online", "service": "SERVEX_AI Engine"}


# ==========================================================
# MAIN PIPELINE
# ==========================================================

@app.post("/audit-process")
async def audit_process(file: UploadFile = File(...)):

    print("\n===============================")
    print("🚀 SERVEX_AI PIPELINE STARTED")
    print("===============================\n")

    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Invalid CSV file")

    try:

        # =========================
        # 1️⃣ READ CSV
        # =========================
        print("📥 STEP 1 — Reading CSV")

        content = await file.read()
        csv_usuario = content.decode("utf-8-sig")

        print("✅ CSV loaded")

        # =========================
        # 2️⃣ MASTER DATA
        # =========================
        print("📡 STEP 2 — Loading master data")

        maestro = obtener_datos_maestros()

        if not maestro:
            raise Exception("Master data returned empty")

        print("✅ Master data loaded")

        # =========================
        # 3️⃣ AUDIT CSV
        # =========================
        print("🔎 STEP 3 — Auditing CSV")

        discrepancias = auditar_csv_logic(csv_usuario, maestro["csv_raw"])

        print(f"✅ Audit finished — discrepancies: {len(discrepancias)}")

        if not discrepancias:
            return {
                "status": "success",
                "message": "No discrepancies found",
                "data": {"skus_affected": 0}
            }

        # =========================
        # 4️⃣ PROCESS PIM XML
        # =========================
        print("📦 STEP 4 — Extracting PIM XML")

        skus_err = list({d["sku"] for d in discrepancias})

        pim_data = {
            p["sku"]: p
            for p in extraer_pim_xml(maestro["xml_raw"], skus_err)
        }

        print("✅ PIM XML processed")

        # =========================
        # 5️⃣ BUILD REPORT
        # =========================
        print("📊 STEP 5 — Building audit report")

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

                    if num_int == 2 or num_int > 10:
                        continue

                    val_csv = limpiar_monto(d["row_user"][i])

                    lbl = f"Price Grade {num_str.zfill(2) if num_int < 10 else num_int}"

                    info_xml = pim.get("grados", {}).get(
                        lbl,
                        {
                            "xml_upcharge": "NOT_FOUND",
                            "xml_total_calculado": "NOT_FOUND"
                        }
                    )

                    status = "OK"

                    if info_xml["xml_total_calculado"] != "NOT_FOUND":
                        status = (
                            "OK"
                            if val_csv == info_xml["xml_total_calculado"]
                            else "MISMATCH"
                        )
                    else:
                        status = "NOT_IN_PIM"

                    if status != "OK":

                        grados_disc.append({
                            "grado": h_clean,
                            "csv_user_total": val_csv,
                            "xml_upcharge_sugerido": recalcular_upcharge_dinamico(
                                val_csv,
                                base_referencia
                            ),
                            "xml_expected_total": info_xml["xml_total_calculado"],
                            "result": status
                        })

            if grados_disc or nuevo_base_csv is not None:

                reporte_final.append({
                    "sku": sku,
                    "nuevo_base_csv": nuevo_base_csv,
                    "comparativa_grados_xml": grados_disc
                })

        print(f"✅ Report built — SKUs affected: {len(reporte_final)}")

        # =========================
        # 6️⃣ GENERATE XML
        # =========================
        print("🧬 STEP 6 — Generating XML")

        xml_final_content = generar_xml_editado(
            maestro["xml_raw"],
            reporte_final
        )

        print("✅ XML generated")

        # =========================
        # 7️⃣ SUPABASE UPDATE
        # =========================
        print("☁️ STEP 7 — Updating Supabase")

        supabase.table("ClientsSERVEX").update({
            "audit_report_json": reporte_final,
            "xml_actualizer_raw": xml_final_content
        }).eq("company_name", "LESRO").execute()

        print("✅ Supabase updated")

        # =========================
        # 8️⃣ AI AGENT
        # =========================
        print("🤖 STEP 8 — Running AI agent")

        agent_workflow.invoke({
            "raw_data": reporte_final,
            "summary_text": "",
            "reporte_final": ""
        })

        print("✅ AI agent completed")

        print("\n🎉 PIPELINE COMPLETED SUCCESSFULLY\n")

        return {
            "status": "success",
            "message": "SERVEX_AI pipeline executed successfully",
            "data": {
                "skus_affected": len(reporte_final),
                "audit_report": reporte_final
            }
        }

    except Exception as e:

        print("\n❌ PIPELINE FAILED")
        print(str(e))
        print("\n")

        raise HTTPException(
            status_code=500,
            detail=str(e)
        )


if __name__ == "__main__":

    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
    )