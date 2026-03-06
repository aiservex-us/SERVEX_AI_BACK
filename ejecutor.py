import os
import subprocess
import time

def ejecutar_sistema_completo():
    print("🚀 INICIANDO PIPELINE DE SERVEX_AI...")
    
    # 1. Ejecutar el Reestructurador de XML y Auditoría Técnica
    print("\nStep 1: Ejecutando Reestructure_xml.py...")
    try:
        # Importamos la función directamente para evitar llamadas a sistema lentas
        from Reestructure_xml import run_software_audit
        TARGET_CSV = "LESRO_PRICING_MASTER_for_01_01_26(2026_Pricing_File_RWS)-29.csv"
        run_software_audit(TARGET_CSV)
        print("✅ Reestructure_xml completado con éxito.")
    except Exception as e:
        print(f"❌ Error en Step 1: {e}")
        return

    # Pequeña pausa para asegurar escritura de archivos
    time.sleep(1)

    # 2. Ejecutar el Agente de IA para el Resumen Natural
    print("\nStep 2: Ejecutando Agente IA (Reestructure_model.py)...")
    try:
        from Reestructure_model import run_ai_agent
        run_ai_agent()
        print("✅ Auditoría del Agente IA finalizada.")
    except Exception as e:
        print(f"❌ Error en Step 2: {e}")

    print("\n✨ PIPELINE FINALIZADO: Los archivos maestro_actualizado.xml y resumen_auditoria.txt están listos.")

if __name__ == "__main__":
    ejecutar_sistema_completo()