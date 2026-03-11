import os
import csv
import io
import json
import xml.etree.ElementTree as ET
from typing import List, Dict, Any
from dotenv import load_dotenv
from supabase import create_client

# ======================================================
# 1. CONFIGURACIÓN Y CONEXIÓN (SERVEX_AI CORE)
# ======================================================
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def obtener_datos_maestros() -> Dict[str, Any]:
    res = supabase.from_('ClientsSERVEX') \
        .select('csv_raw, xml_raw') \
        .eq('company_name', 'LESRO') \
        .single() \
        .execute()
    return res.data

# ======================================================
# 2. UTILERÍAS DE PROCESAMIENTO
# ======================================================
def detectar_delimitador(txt: str) -> str:
    if not txt: return ','
    lines = txt.splitlines()
    sample = lines[min(2, len(lines)-1)] if len(lines) > 1 else txt
    return ';' if ';' in sample else ','

def limpiar_monto(val: str) -> int:
    if not val or val.upper() == 'N/A': return 0
    clean = val.replace('$', '').replace(' ', '').replace(',', '').strip()
    if '.' in clean:
        parts = clean.split('.')
        if len(parts[-1]) != 2:
            clean = "".join(parts)
        else:
            clean = "".join(parts[:-1])
    clean = "".join(filter(str.isdigit, clean))
    return int(clean) if clean else 0

# ======================================================
# NUEVA FUNCIÓN: RESUMEN EJECUTIVO (MICRO-REPORTE)
# ======================================================
def generar_resumen_ejecutivo(reporte_final: List[Dict]) -> Dict[str, Any]:
    """Genera un mapa general y legible de las discrepancias encontradas."""
    total_skus = len(reporte_final)
    skus_con_cambio_base = 0
    conteo_grados = {}

    for item in reporte_final:
        if item.get("nuevo_base_csv") is not None:
            skus_con_cambio_base += 1
        
        for comp in item.get("comparativa_grados_xml", []):
            grado = comp.get("grado", "Unknown")
            conteo_grados[grado] = conteo_grados.get(grado, 0) + 1

    resumen = {
        "total_skus_afectados": total_skus,
        "cambios_en_precio_base": skus_con_cambio_base,
        "desglose_por_grados": conteo_grados,
        "mensaje_resumen": f"Se detectaron cambios en {total_skus} productos. {skus_con_cambio_base} requieren actualización de precio base."
    }
    return resumen

# ======================================================
# 3. LÓGICA DE AUDITORÍA CSV
# ======================================================
def auditar_csv_logic(csv_usuario: str, csv_maestro: str) -> List[Dict]:
    delim_u = detectar_delimitador(csv_usuario)
    delim_m = detectar_delimitador(csv_maestro)
    u_rows = list(csv.reader(io.StringIO(csv_usuario.strip()), delimiter=delim_u))
    m_rows = list(csv.reader(io.StringIO(csv_maestro.strip()), delimiter=delim_m))
    
    h_idx = 0
    for i, r in enumerate(u_rows):
        if any(x in "".join(r).upper() for x in ["ID", "SKU", "PRODUCT"]):
            h_idx = i
            break
            
    headers = u_rows[h_idx]
    sku_idx = next(i for i, h in enumerate(headers) if h.upper() in ["ID", "SKU", "PRODUCT"])
    maestro_dict = {r[sku_idx]: r for r in m_rows[h_idx + 1:] if len(r) > sku_idx}
    
    discrepancias = []
    for i in range(h_idx + 1, len(u_rows)):
        row_u = u_rows[i]
        if not row_u or len(row_u) <= sku_idx: continue
        sku = row_u[sku_idx]
        row_m = maestro_dict.get(sku)
        
        if row_m and row_u != row_m:
            diffs = []
            for col in range(min(len(row_u), len(row_m))):
                if row_u[col].strip() != row_m[col].strip():
                    diffs.append({"field": headers[col], "user_value": row_u[col].strip(), "master_value": row_m[col].strip()})
            if diffs:
                discrepancias.append({"sku": sku, "row_user": row_u, "headers": headers, "diffs": diffs})
    return discrepancias

# ======================================================
# 4. XML / PIM - EXTRACCIÓN
# ======================================================
def extraer_pim_xml(xml_raw: str, skus: List[str]) -> List[Dict]:
    root = ET.fromstring(xml_raw)
    results = []
    feature_map = {f.findtext("Code"): f for f in root.findall(".//Feature") if f.findtext("Code")}
    
    for sku in skus:
        prod = root.find(f".//Product[Code='{sku}']")
        if prod is None: continue
        
        base_price_node = prod.find(".//Price/Value")
        base_price = float(base_price_node.text) if base_price_node is not None else 0.0
        
        f_grados = None
        for f_code, f_elem in feature_map.items():
            if sku in f_code and ("UPH" in f_code or "AVERAGE" in f_code):
                f_grados = f_elem
                break

        grados_info = {}
        if f_grados is not None:
            for opt in f_grados.findall("Option"):
                opt_code = opt.findtext("Code", "")
                if "GRD" in opt_code.upper():
                    num_str = "".join(filter(str.isdigit, opt_code))
                    num_int = int(num_str) if num_str else 0
                    
                    if 1 <= num_int <= 10:
                        label = f"Price Grade {num_str.zfill(2) if num_int < 10 else num_int}"
                        val_node = opt.find(".//OptionPrice/Value")
                        upcharge = float(val_node.text) if val_node is not None else 0.0
                        grados_info[label] = {"xml_upcharge": int(upcharge), "xml_total_calculado": int(base_price + upcharge)}

        results.append({"sku": sku, "base_price": int(base_price), "grados": grados_info})
    return results

# ======================================================
# 5. PROCESAMIENTO Y EDICIÓN XML
# ======================================================
def generar_xml_editado(xml_original: str, reporte_detallado: List[Dict]) -> str:
    root = ET.fromstring(xml_original)
    cambios_realizados = 0

    print("\n🔍 PROCESANDO EDICIÓN XML PARA ALMACENAMIENTO...")
    
    for item in reporte_detallado:
        sku = item["sku"]

        if item.get("nuevo_base_csv") is not None:
            prod = root.find(f".//Product[Code='{sku}']")
            if prod is not None:
                price_node = prod.find(".//Price/Value")
                if price_node is not None:
                    if price_node.text != str(item["nuevo_base_csv"]):
                        price_node.text = str(item["nuevo_base_csv"])
                        cambios_realizados += 1

        for g in item.get("comparativa_grados_xml", []):
            if g["result"] == "MISMATCH":
                num_str = "".join(filter(str.isdigit, g["grado"]))
                num_int = int(num_str) if num_str else 0

                if num_int == 2 or num_int > 10:
                    continue

                encontrado = False
                for feature in root.findall(".//Feature"):
                    f_code = feature.findtext("Code", "")
                    if sku in f_code:
                        for option in feature.findall("Option"):
                            o_code = option.findtext("Code", "")
                            if "GRD" in o_code.upper() and (num_str in o_code or str(num_int) in o_code):
                                val_node = option.find(".//OptionPrice/Value")
                                if val_node is not None:
                                    nuevo_val = str(float(g["xml_upcharge_sugerido"]))
                                    if val_node.text != nuevo_val:
                                        val_node.text = nuevo_val
                                        cambios_realizados += 1
                                    encontrado = True
                                    break
                        if encontrado: break

    print(f"✅ CAMBIOS TOTALES PREPARADOS: {cambios_realizados}")
    return ET.tostring(root, encoding='unicode', method='xml')

# ======================================================
# 6. ORQUESTADOR PRINCIPAL (ACTUALIZADO PARA SUPABASE)
# ======================================================
def recalcular_upcharge_dinamico(csv_total: int, base_referencia: int) -> int:
    return max(0, csv_total - base_referencia) if base_referencia > 0 else 0

def run_software_audit(archivo_csv_local: str):
    if not os.path.exists(archivo_csv_local): 
        print(f"Archivo {archivo_csv_local} no encontrado.")
        return
        
    with open(archivo_csv_local, "r", encoding="utf-8-sig") as f:
        csv_usuario = f.read()

    maestro = obtener_datos_maestros()
    discrepancias = auditar_csv_logic(csv_usuario, maestro["csv_raw"])
    
    if not discrepancias:
        print("\n✅ ÉXITO: El archivo coincide con el maestro. No hay cambios que reportar.")
        return

    skus_err = list({d["sku"] for d in discrepancias})
    pim_data = {p["sku"]: p for p in extraer_pim_xml(maestro["xml_raw"], skus_err)}

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

    # Generamos el contenido del XML actualizado
    xml_final_content = generar_xml_editado(maestro["xml_raw"], reporte_final)
    
    # Generamos el Micro-Reporte Resumen
    resumen_ejecutivo = generar_resumen_ejecutivo(reporte_final)

    # ACTUALIZACIÓN EN BASE DE DATOS (EN VEZ DE DESCARGA)
    print("\n🚀 SINCRONIZANDO RESULTADOS CON SUPABASE...")
    try:
        update_res = supabase.from_('ClientsSERVEX') \
            .update({
                "audit_report_json": reporte_final,
                "audit_summary_json": resumen_ejecutivo, # Almacenamos el resumen
                "xml_actualizer_raw": xml_final_content
            }) \
            .eq('company_name', 'LESRO') \
            .execute()
        
        print(f"✅ PROCESO COMPLETADO EXITOSAMENTE.")
        print(f"📊 Reporte Detallado: {len(reporte_final)} SKUs.")
        print(f"📝 Resumen: {resumen_ejecutivo['mensaje_resumen']}")
        
    except Exception as e:
        print(f"❌ ERROR AL ACTUALIZAR LA BASE DE DATOS: {e}")

if __name__ == "__main__":
    TARGET = "LESRO_PRICING_MASTER_for_01_01_26(2026_Pricing_File_RWS)-29.csv"
    run_software_audit(TARGET)