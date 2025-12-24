import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
import sqlite3
import json
import uuid
from datetime import datetime

# =============================================================================
# INICIALIZACIÓN COMPLETA DE SESSION_STATE
# =============================================================================

def inicializar_session_state():
    """Inicializa todas las variables necesarias en session_state - VERSIÓN MEJORADA"""
    if 'inicializado' not in st.session_state:
        # Variables de usuario y proyecto
        st.session_state.user_authenticated = False
        st.session_state.current_user_email = None
        st.session_state.current_project_id = None
        st.session_state.current_project_name = None
        st.session_state.supabase = None
        st.session_state.modo_visualizacion = False  # True = solo ver, False = editar

        # Variables de cálculo
        st.session_state.emisiones_etapas = {}
        st.session_state.produccion_etapas = {}
        st.session_state.emisiones_fuentes = {
            "Fertilizantes": 0,
            "Agroquímicos": 0,
            "Riego": 0,
            "Maquinaria": 0,
            "Transporte": 0,
            "Residuos": 0,
            "Fin de vida": 0
        }
        st.session_state.emisiones_fuente_etapa = {}
        st.session_state.modo_anterior = ""
        
        # Variables específicas de resultados
        st.session_state.em_total = 0
        st.session_state.prod_total = 0
        st.session_state.emisiones_anuales = []
        st.session_state.emisiones_ciclos = []
        st.session_state.desglose_fuentes_ciclos = []
        
        # Variables de datos de entrada
        st.session_state.fertilizantes_data = []
        st.session_state.agroquimicos_data = []
        st.session_state.riego_data = []
        st.session_state.maquinaria_data = []
        st.session_state.residuos_data = []
        
        # Contador para gráficos
        st.session_state.plot_counter = 0
        
        # Variables de consentimiento
        st.session_state.consentimiento_otorgado = False
        st.session_state.consentimiento_fecha = None
        st.session_state.consentimiento_texto = ""
        
        # Variables de caracterización
        st.session_state.cultivo = ""
        st.session_state.ubicacion = ""
        st.session_state.tipo_suelo = ""
        st.session_state.clima = ""
        st.session_state.morfologia = ""

        # === NUEVA ESTRUCTURA DE DATOS - REEMPLAZA LA ANTERIOR ===
        # Datos mientras se ingresan (volátiles)
        st.session_state.datos_en_edicion = {}
        
        # Datos confirmados (para cálculos)
        st.session_state.datos_confirmados = {
            "caracterizacion": {},
            "fertilizantes": {},
            "agroquimicos": {},
            "riego": {},
            "maquinaria": {},
            "residuos": {},
            "etapas": {}
        }
        
        # Control de guardado
        st.session_state.guardado_pendiente = False  # REEMPLAZA datos_pendientes_guardar
        st.session_state.ultimo_guardado = None
        st.session_state.ultimo_cambio = None
        
        # Estado del proyecto
        st.session_state.proyecto_es_local = True  # True hasta que se guarde en Supabase
        st.session_state.proyecto_version = 1
        
        # Marcar como inicializado
        st.session_state.inicializado = True

# Ejecutar la inicialización al inicio
inicializar_session_state()

# =============================================================================
# SISTEMA DE GUARDADO CONTROLADO
# =============================================================================

def guardar_proyecto_completo():
    """Guarda todo el proyecto actual en Supabase - VERSIÓN DEFINITIVA"""
    
    if st.session_state.supabase is None:
        st.error("❌ No hay conexión con Supabase")
        return False
    
    try:
        # =========================================================================
        # 1. RECOLECTAR DATOS
        # =========================================================================
        datos_completos = recolectar_todos_los_datos_para_guardar()
        
        # Verificar que hay datos
        if not datos_completos.get("characterization") or not datos_completos.get("results"):
            st.error("❌ No hay datos suficientes para guardar")
            return False
        
        # =========================================================================
        # 2. PREPARAR DATOS PARA SUPABASE
        # =========================================================================
        project_data = {
            "user_email": st.session_state.current_user_email,
            "title": st.session_state.current_project_name,
            "mode": "anual" if st.session_state.get('modo_anterior', 'Anual') == 'Anual' else "perenne",
            "characterization": datos_completos["characterization"],
            "sources_data": datos_completos["sources_data"],
            "results": datos_completos["results"],
            "app_version": "2.0",
            "updated_at": datetime.now().isoformat(),
            "created_at": datetime.now().isoformat()  # Siempre agregar fecha creación
        }
        
        # =========================================================================
        # 3. GUARDAR EN SUPABASE (SIEMPRE COMO NUEVO)
        # =========================================================================
        # REGLA SIMPLE: Si el proyecto es local o empieza con 'local_', crear NUEVO
        # Si ya tiene ID de Supabase, intentar actualizar
        
        project_id = st.session_state.get('current_project_id')
        
        # ¿Es un proyecto local?
        es_proyecto_local = False
        if isinstance(project_id, str):
            if project_id.startswith('local_') or project_id.startswith('nuevo_'):
                es_proyecto_local = True
            elif len(project_id) == 36:  # Es un UUID
                # Verificar si existe en Supabase
                try:
                    response = st.session_state.supabase.table("projects")\
                        .select("id")\
                        .eq("id", project_id)\
                        .execute()
                    
                    if not response.data or len(response.data) == 0:
                        # UUID no existe en Supabase = tratar como local
                        es_proyecto_local = True
                except:
                    # Si hay error, asumir que no existe
                    es_proyecto_local = True
        
        if es_proyecto_local or not project_id:
            # CREAR NUEVO PROYECTO
            st.info("🆕 Creando nuevo proyecto en la nube...")
            
            try:
                response = st.session_state.supabase.table("projects")\
                    .insert(project_data)\
                    .execute()
                
                if response.data and len(response.data) > 0:
                    nuevo_id_real = response.data[0]['id']
                    
                    # ACTUALIZAR ESTADO
                    st.session_state.current_project_id = nuevo_id_real
                    st.session_state.proyecto_es_local = False
                    st.session_state.modo_visualizacion = True  # CAMBIAR A MODO VISUALIZACIÓN
                    st.session_state.guardado_pendiente = False
                    st.session_state.ultimo_guardado = datetime.now().strftime("%H:%M:%S")
                    
                    st.success(f"✅ Proyecto '{st.session_state.current_project_name}' guardado en la nube")
                    return True
                else:
                    st.error("❌ Error: Supabase no devolvió ID del proyecto")
                    return False
                    
            except Exception as e:
                st.error(f"❌ Error al crear proyecto: {str(e)}")
                return False
                
        else:
            # ACTUALIZAR PROYECTO EXISTENTE (solo si ya existe en Supabase)
            st.info("🔄 Actualizando proyecto existente...")
            
            try:
                response = st.session_state.supabase.table("projects")\
                    .update(project_data)\
                    .eq("id", str(project_id))\
                    .execute()
                
                if hasattr(response, 'data') and response.data:
                    st.session_state.modo_visualizacion = True  # CAMBIAR A MODO VISUALIZACIÓN
                    st.session_state.guardado_pendiente = False
                    st.session_state.ultimo_guardado = datetime.now().strftime("%H:%M:%S")
                    st.success("✅ Proyecto actualizado en la nube")
                    return True
                else:
                    st.warning("⚠️ Proyecto no encontrado en la nube. Creando como nuevo...")
                    
                    # Crear como nuevo
                    response = st.session_state.supabase.table("projects")\
                        .insert(project_data)\
                        .execute()
                    
                    if response.data and len(response.data) > 0:
                        nuevo_id_real = response.data[0]['id']
                        st.session_state.current_project_id = nuevo_id_real
                        st.session_state.proyecto_es_local = False
                        st.session_state.modo_visualizacion = True
                        st.session_state.guardado_pendiente = False
                        st.session_state.ultimo_guardado = datetime.now().strftime("%H:%M:%S")
                        st.success(f"✅ Proyecto recreado con nuevo ID: {nuevo_id_real}")
                        return True
                    else:
                        st.error("❌ No se pudo recrear el proyecto")
                        return False
                        
            except Exception as e:
                st.error(f"❌ Error al actualizar: {str(e)}")
                return False
        
        return False
        
    except Exception as e:
        st.error(f"❌ Error general al guardar: {str(e)}")
        return False

def es_uuid_valido(uuid_string):
    """Verifica si un string es un UUID válido"""
    import uuid as uuid_lib
    try:
        uuid_obj = uuid_lib.UUID(str(uuid_string))
        return True
    except ValueError:
        return False

def migrar_datos_a_nuevo_id(id_antiguo, id_nuevo):
    """
    Migra todos los datos vinculados al ID antiguo al ID nuevo.
    Esto evita que se pierdan datos cuando un proyecto local se guarda en la nube.
    """
    try:
        # Buscar todas las claves que contienen el ID antiguo
        claves_a_migrar = []
        for clave in st.session_state.keys():
            if isinstance(clave, str) and id_antiguo in clave:
                claves_a_migrar.append(clave)
        
        # Migrar cada clave
        for clave_antigua in claves_a_migrar:
            clave_nueva = clave_antigua.replace(id_antiguo, id_nuevo)
            st.session_state[clave_nueva] = st.session_state[clave_antigua]
            
            # Opcional: eliminar la clave antigua para limpiar
            # del st.session_state[clave_antigua]
        
        return len(claves_a_migrar)
    except Exception as e:
        st.error(f"Error migrando datos: {e}")
        return 0

def recolectar_todos_los_datos_para_guardar():
    """
    Recolecta todos los datos para guardar en Supabase.
    VERSIÓN CORREGIDA DEFINITIVA - Estructura correcta.
    """
    try:
        # =====================================================================
        # DEBUG: Verificar qué hay realmente en datos_confirmados
        # =====================================================================
        st.sidebar.markdown("---")
        st.sidebar.markdown("**🔍 VERIFICACIÓN PRE-GUARDADO**")
        
        # Obtener datos confirmados actuales
        datos_confirmados = st.session_state.get('datos_confirmados', {})
        
        # Mostrar estructura real
        st.sidebar.write("Estructura REAL de datos_confirmados:")
        
        for tipo in ['fertilizantes', 'agroquimicos', 'riego', 'maquinaria', 'residuos']:
            if tipo in datos_confirmados:
                etapas = datos_confirmados[tipo]
                if isinstance(etapas, dict):
                    for etapa, datos in etapas.items():
                        if isinstance(datos, list):
                            st.sidebar.write(f"• {tipo}.{etapa}: {len(datos)} items")
                        elif datos:
                            st.sidebar.write(f"• {tipo}.{etapa}: (dict/list)")
                elif etapas:
                    st.sidebar.write(f"• {tipo}: {type(etapas)}")
        
        # =====================================================================
        # 1. OBTENER DATOS DE CARACTERIZACIÓN
        # =====================================================================
        caracterizacion = {
            "cultivo": st.session_state.get('cultivo', ''),
            "ubicacion": st.session_state.get('ubicacion', ''),
            "tipo_suelo": st.session_state.get('tipo_suelo', ''),
            "clima": st.session_state.get('clima', ''),
            "morfologia": st.session_state.get('morfologia', ''),
            "extra": st.session_state.get('extra', '')
        }
        
        # =====================================================================
        # 2. OBTENER DATOS DE FORMULARIOS - ESTRUCTURA CORRECTA
        # =====================================================================
        # Asegurar que tengamos datos_confirmados
        if 'datos_confirmados' not in st.session_state:
            st.session_state.datos_confirmados = {}
        
        datos_confirmados = st.session_state.datos_confirmados
        
        # Crear sources_data con la estructura CORRECTA que Supabase espera
        sources_data = {}
        
        # Lista de tipos de datos
        tipos = ['fertilizantes', 'agroquimicos', 'riego', 'maquinaria', 'residuos']
        
        for tipo in tipos:
            # Obtener los datos para este tipo
            if tipo in datos_confirmados:
                datos_tipo = datos_confirmados[tipo]
                
                # Verificar la estructura real
                if isinstance(datos_tipo, dict):
                    # Estructura: {'ciclo_tipico': [lista de datos]}
                    # Necesitamos guardar esto tal cual
                    sources_data[tipo] = datos_tipo
                elif isinstance(datos_tipo, list):
                    # Si es una lista directa, convertir a estructura con etapa
                    sources_data[tipo] = {'ciclo_tipico': datos_tipo}
                else:
                    # Estructura vacía
                    sources_data[tipo] = {}
            else:
                # No hay datos de este tipo
                sources_data[tipo] = {}
        
        # =====================================================================
        # 3. OBTENER RESULTADOS CALCULADOS
        # =====================================================================
        em_total = st.session_state.get('em_total', 0)
        prod_total = st.session_state.get('prod_total', 0)
        
        results = {
            "emisiones_totales": float(em_total),
            "produccion_total": float(prod_total),
            "huella_por_kg": float(em_total / prod_total if prod_total > 0 else 0),
            "emisiones_etapas": st.session_state.get('emisiones_etapas', {}),
            "produccion_etapas": st.session_state.get('produccion_etapas', {}),
            "emisiones_fuentes": st.session_state.get('emisiones_fuentes', {}),
            "emisiones_ciclos": st.session_state.get('emisiones_ciclos', []),
            "desglose_fuentes_ciclos": st.session_state.get('desglose_fuentes_ciclos', []),
            "fecha_calculo": datetime.now().isoformat()
        }
        
        # =====================================================================
        # 4. OBTENER CONFIGURACIÓN DE CICLOS
        # =====================================================================
        config_ciclos = {
            "n_ciclos": st.session_state.get('n_ciclos', 1),
            "ciclos_diferentes": st.session_state.get('ciclos_diferentes', 'No, todos los ciclos son iguales')
        }
        
        # =====================================================================
        # 5. DETERMINAR MODO CORRECTO
        # =====================================================================
        modo_actual = st.session_state.get('modo_anterior', 'Anual')
        mode = "anual" if str(modo_actual).strip().lower() == "anual" else "perenne"
        
        # =====================================================================
        # 6. VERIFICACIÓN FINAL - Mostrar qué se va a guardar
        # =====================================================================
        st.sidebar.markdown("**📊 Lo que se ENVIARÁ a Supabase:**")
        
        for tipo in tipos:
            if tipo in sources_data:
                datos = sources_data[tipo]
                if isinstance(datos, dict):
                    for etapa, lista_datos in datos.items():
                        if isinstance(lista_datos, list):
                            st.sidebar.write(f"✓ {tipo}.{etapa}: {len(lista_datos)} registros")
                        elif lista_datos:
                            st.sidebar.write(f"✓ {tipo}.{etapa}: datos presentes")
        
        # =====================================================================
        # 7. ESTRUCTURA COMPLETA PARA SUPABASE
        # =====================================================================
        datos_completos = {
            "characterization": caracterizacion,
            "sources_data": sources_data,
            "results": results,
            "config_ciclos": config_ciclos,
            "mode": mode,
            "app_version": "2.0",
            "updated_at": datetime.now().isoformat()
        }
                
        return datos_completos
        
    except Exception as e:
        st.error(f"❌ Error recolectando datos: {str(e)}")
        st.sidebar.error(f"Error en recolectar_datos: {e}")
        
        # Retornar estructura mínima para evitar errores
        return {
            "characterization": {},
            "sources_data": {},
            "results": {},
            "config_ciclos": {},
            "mode": "anual",
            "app_version": "2.0",
            "updated_at": datetime.now().isoformat()
        }

def verificar_datos_en_supabase():
    """Función temporal para verificar qué hay en Supabase"""
    
    if st.sidebar.button("🔍 Verificar datos en Supabase"):
        if st.session_state.supabase and st.session_state.current_project_id:
            try:
                # Cargar el proyecto desde Supabase
                response = st.session_state.supabase.table("projects")\
                    .select("*")\
                    .eq("id", st.session_state.current_project_id)\
                    .execute()
                
                if response.data:
                    proyecto = response.data[0]
                    
                    st.sidebar.markdown("**📦 Datos en Supabase:**")
                    
                    # Mostrar sources_data
                    sources_data = proyecto.get('sources_data', {})
                    st.sidebar.write("sources_data:")
                    for tipo, datos in sources_data.items():
                        if isinstance(datos, dict):
                            for etapa, lista in datos.items():
                                if isinstance(lista, list):
                                    st.sidebar.write(f"  {tipo}.{etapa}: {len(lista)} items")
                                else:
                                    st.sidebar.write(f"  {tipo}.{etapa}: {type(lista)}")
                        else:
                            st.sidebar.write(f"  {tipo}: {type(datos)}")
                    
                    # Mostrar mode
                    st.sidebar.write(f"mode: {proyecto.get('mode', 'No especificado')}")
                    
                else:
                    st.sidebar.warning("No se encontró el proyecto en Supabase")
                    
            except Exception as e:
                st.sidebar.error(f"Error: {e}")
        else:
            st.sidebar.warning("No hay conexión con Supabase")

def actualizar_datos_desde_widgets():
    """
    Asegura que los datos en datos_confirmados sean los actuales de los widgets.
    Esta función debe llamarse ANTES de guardar en Supabase.
    """
    # Esta función no hace nada activamente porque los widgets de Streamlit
    # ya actualizan session_state automáticamente cuando se renderizan.
    # En su lugar, verificamos que los datos estén en datos_confirmados.
    
    # Solo para debug: mostrar qué hay en datos_confirmados
    if 'datos_confirmados' in st.session_state:
        # Contar cuántos datos hay
        fertilizantes = st.session_state.datos_confirmados.get('fertilizantes', {})
        agroquimicos = st.session_state.datos_confirmados.get('agroquimicos', {})
        
        # Mostrar conteo en el sidebar para debug
        st.sidebar.markdown("---")
        st.sidebar.markdown("**🔍 Datos en memoria:**")
        st.sidebar.write(f"Fertilizantes: {sum(len(v) for v in fertilizantes.values() if isinstance(v, list))} registros")
        st.sidebar.write(f"Agroquímicos: {sum(len(v) for v in agroquimicos.values() if isinstance(v, list))} registros")
        
        # Verificar que haya datos en ciclo_tipico (modo anual)
        if 'ciclo_tipico' in fertilizantes:
            st.sidebar.write(f"Fertilizantes en ciclo_tipico: {len(fertilizantes['ciclo_tipico'])}")

def verificar_datos_para_guardar():
    """Función temporal para verificar qué datos se van a guardar"""
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 🔍 Verificar datos a guardar")
    
    if st.sidebar.button("📊 Ver datos recopilados"):
        datos = recolectar_todos_los_datos_para_guardar()
        
        st.sidebar.markdown("**Datos de caracterización:**")
        st.sidebar.json(datos.get("characterization", {}))
        
        st.sidebar.markdown("**Fuentes de datos:**")
        for tipo, etapas in datos.get("sources_data", {}).items():
            st.sidebar.write(f"{tipo}: {len(etapas)} etapa(s)")
        
        st.sidebar.markdown("**Resultados:**")
        st.sidebar.json({
            "emisiones_totales": datos.get("results", {}).get("emisiones_totales", 0),
            "produccion_total": datos.get("results", {}).get("produccion_total", 0)
        })

def cargar_datos_desde_proyecto(project_data):
    """
    Carga COMPLETAMENTE los datos de un proyecto desde Supabase.
    VERSIÓN COMPLETA Y ROBUSTA.
    """
    try:
        if not project_data:
            st.error("❌ No hay datos del proyecto para cargar")
            return False
        
        # =====================================================================
        # 1. LIMPIAR DATOS ANTERIORES
        # =====================================================================
        # Limpiar solo claves específicas, mantener otras importantes
        claves_a_limpiar = []
        for clave in list(st.session_state.keys()):
            if any(clave.startswith(prefijo) for prefijo in 
                   ['fertilizantes_data_', 'agroquimicos_data_', 'riego_data_', 
                    'maquinaria_data_', 'residuos_data_', 'etapas_data_']):
                claves_a_limpiar.append(clave)
        
        for clave in claves_a_limpiar:
            try:
                del st.session_state[clave]
            except:
                pass
        
        # =====================================================================
        # 2. CARGAR CARACTERIZACIÓN
        # =====================================================================
        if 'characterization' in project_data and project_data['characterization']:
            car = project_data['characterization']
            st.session_state.cultivo = car.get('cultivo', '')
            st.session_state.ubicacion = car.get('ubicacion', '')
            st.session_state.tipo_suelo = car.get('tipo_suelo', '')
            st.session_state.clima = car.get('clima', '')
            st.session_state.morfologia = car.get('morfologia', '')
            st.session_state.extra = car.get('extra', '')
        
        # =====================================================================
        # 3. CARGAR DATOS DE FORMULARIOS
        # =====================================================================
        # Inicializar estructura
        st.session_state.datos_confirmados = {
            "caracterizacion": {},
            "fertilizantes": {},
            "agroquimicos": {},
            "riego": {},
            "maquinaria": {},
            "residuos": {},
            "etapas": {}
        }
        
        if 'sources_data' in project_data and project_data['sources_data']:
            sources = project_data['sources_data']
            
            # Cargar cada tipo de datos
            tipos = ['fertilizantes', 'agroquimicos', 'riego', 'maquinaria', 'residuos']
            for tipo in tipos:
                if tipo in sources and sources[tipo]:
                    st.session_state.datos_confirmados[tipo] = sources[tipo]
        
        # =====================================================================
        # 4. CARGAR CONFIGURACIÓN DE CICLOS
        # =====================================================================
        if 'config_ciclos' in project_data and project_data['config_ciclos']:
            config = project_data['config_ciclos']
            st.session_state.n_ciclos = config.get('n_ciclos', 1)
            st.session_state.ciclos_diferentes = config.get('ciclos_diferentes', 'No, todos los ciclos son iguales')
        else:
            # Valores por defecto
            st.session_state.n_ciclos = 1
            st.session_state.ciclos_diferentes = 'No, todos los ciclos son iguales'
        
        # =====================================================================
        # 5. CARGAR RESULTADOS CALCULADOS
        # =====================================================================
        if 'results' in project_data and project_data['results']:
            res = project_data['results']
            
            # Variables principales
            st.session_state.em_total = float(res.get('emisiones_totales', 0))
            st.session_state.prod_total = float(res.get('produccion_total', 0))
            
            # Estructuras de datos
            st.session_state.emisiones_etapas = res.get('emisiones_etapas', {})
            st.session_state.produccion_etapas = res.get('produccion_etapas', {})
            st.session_state.emisiones_fuentes = res.get('emisiones_fuentes', {})
            st.session_state.emisiones_fuente_etapa = res.get('emisiones_fuente_etapa', {})
            
            # Datos específicos de modo anual
            st.session_state.emisiones_ciclos = res.get('emisiones_ciclos', [])
            st.session_state.desglose_fuentes_ciclos = res.get('desglose_fuentes_ciclos', [])
            
            # Guardar resultados globales para acceso fácil
            st.session_state.resultados_globales = {
                "tipo": "anual",
                "em_total": st.session_state.em_total,
                "prod_total": st.session_state.prod_total,
                "emisiones_ciclos": st.session_state.emisiones_ciclos,
                "desglose_fuentes_ciclos": st.session_state.desglose_fuentes_ciclos
            }
        
        # =====================================================================
        # 6. CARGAR MODO
        # =====================================================================
        if 'mode' in project_data:
            mode_db = project_data['mode']
            st.session_state.modo_anterior = "Anual" if str(mode_db).lower() == "anual" else "Perenne"
        else:
            st.session_state.modo_anterior = "Anual"
        
        # =====================================================================
        # 7. ESTABLECER ESTADO DEL PROYECTO
        # =====================================================================
        st.session_state.modo_visualizacion = True  # Proyecto cargado = solo lectura
        st.session_state.proyecto_es_local = False  # Ya está en la nube
        st.session_state.guardado_pendiente = False  # No hay cambios sin guardar
        
        return True
        
    except Exception as e:
        st.error(f"❌ Error cargando datos del proyecto: {str(e)}")
        return False

def obtener_datos_de_session_state(tipo, etapa):
    """Obtiene datos de session_state"""
    clave = f"{tipo}_data_{etapa}"
    return st.session_state.get(clave, [])

def recolectar_todos_los_datos():
    """Recolecta todos los datos de entrada del usuario en un solo objeto"""
    
    datos = {
        "caracterizacion": {
            "cultivo": st.session_state.get('cultivo', ''),
            "ubicacion": st.session_state.get('ubicacion', ''),
            "tipo_suelo": st.session_state.get('tipo_suelo', ''),
            "clima": st.session_state.get('clima', ''),
            "morfologia": st.session_state.get('morfologia', ''),
            "extra": st.session_state.get('extra', '')
        },
        "fertilizantes": {},
        "agroquimicos": {},
        "riego": {},
        "maquinaria": {},
        "residuos": {},
        "etapas": st.session_state.get('etapas_data', {})
    }
    
    # Recolectar datos por etapa
    for key in st.session_state.keys():
        if key.startswith("fertilizantes_"):
            etapa = key.replace("fertilizantes_", "")
            datos["fertilizantes"][etapa] = st.session_state[key]
        elif key.startswith("agroquimicos_"):
            etapa = key.replace("agroquimicos_", "")
            datos["agroquimicos"][etapa] = st.session_state[key]
        # ... (similar para otros tipos de datos)
    
    return datos

def mostrar_estado_datos():
    """Función temporal para verificar qué datos hay en session_state"""
    if st.sidebar.button("🔍 Ver datos en memoria"):
        st.sidebar.markdown("### Datos en session_state:")
        
        datos_claves = []
        for clave in st.session_state.keys():
            if "data_" in clave or clave in ['cultivo', 'ubicacion', 'em_total', 'prod_total']:
                valor = st.session_state[clave]
                if isinstance(valor, (list, dict)) and valor:
                    datos_claves.append(f"{clave}: {len(valor) if isinstance(valor, list) else 'dict'}")
                elif valor:
                    datos_claves.append(f"{clave}: {valor}")
        
        if datos_claves:
            for dato in datos_claves:
                st.sidebar.text(dato)
        else:
            st.sidebar.warning("No hay datos en memoria")
        
        # Mostrar datos a guardar
        st.sidebar.markdown("### Datos a guardar:")
        datos_a_guardar = {
            "caracterizacion": {
                "cultivo": st.session_state.get('cultivo'),
                "ubicacion": st.session_state.get('ubicacion')
            },
            "fertilizantes": sum(1 for k in st.session_state.keys() if "fertilizantes_data_" in k),
            "agroquimicos": sum(1 for k in st.session_state.keys() if "agroquimicos_data_" in k),
            "em_total": st.session_state.get('em_total', 0)
        }
        st.sidebar.json(datos_a_guardar)

# =============================================================================
# SISTEMA DE CONSENTIMIENTO Y TÉRMINOS
# =============================================================================

def mostrar_consentimiento_privacidad():
    """Muestra y gestiona el consentimiento de privacidad"""
    
    # Si ya dio consentimiento, no mostrar nada
    if st.session_state.get('consentimiento_otorgado', False):
        return True
    
    # Mostrar términos y condiciones
    st.markdown("---")
    st.header("🔐 Consentimiento Informado - Protección de Datos")
    
    st.warning("""
    **ANTES DE CONTINUAR, LEA ATENTAMENTE LOS SIGUIENTES TÉRMINOS Y CONDICIONES**
    
    Al registrarte en la plataforma AgroPrint (desarrollada por ClearPrint), autorizas expresamente 
    el tratamiento de tus datos personales conforme a la Ley N°19.628 sobre Protección de la Vida Privada.
    """)
    
    with st.expander("📄 POLÍTICA DE PRIVACIDAD COMPLETA - HAGA CLIC PARA VER", expanded=False):
        st.markdown("""
        ### 1. Responsable del tratamiento:
        **ClearPrint**, empresa dedicada al desarrollo de herramientas tecnológicas para la estimación y análisis de huella de carbono y sostenibilidad.

        ### 2. Finalidad del tratamiento:
        Los datos personales que proporciones serán utilizados exclusivamente para:
        • **Gestionar tu registro y acceso** como usuario de la plataforma.
        • **Permitir el desarrollo, almacenamiento y administración** de tus proyectos dentro del sistema.
        • **Analizar resultados de uso** de manera agregada y anónima, con fines estadísticos y de mejora de nuestros servicios.
        • En el futuro, **facilitar procesos de contacto, facturación o pagos electrónicos**, previa actualización de esta política y obtención de un nuevo consentimiento.

        ### 3. Datos recolectados:
        En esta etapa, ClearPrint recolectará únicamente los datos necesarios para su registro (nombre de usuario y dirección de correo electrónico). 
        
        Lo anterior con el fin de que el usuario pueda elaborar un portafolio con todos los proyectos que desarrolle en la plataforma y pueda revisarlos y/o corregirlos, en caso de ser necesario. Esta última acción debe ser informada a ClearPrint.

        Posterior a ello, se recolectarán los datos que ingresa en la plataforma, los cuales se almacenarán en una base de datos interna con el fin de realizar una serie de estudios de los resultados obtenidos, los que se procesarán de manera anónima.

        En versiones posteriores, y en caso de incorporar funciones de pago u otras funcionalidades, se podrán solicitar datos adicionales como nombre completo, RUT, número de teléfono o información de facturación, los cuales serán utilizados únicamente para los fines mencionados.

        ### 4. Comunicación de datos a terceros:
        ClearPrint podrá compartir datos personales con proveedores tecnológicos o de servicios de pago, exclusivamente para la operación, mantenimiento y seguridad de la plataforma, garantizando siempre la confidencialidad y protección de la información conforme a la legislación vigente.

        ### 5. Derechos de los titulares:
        Podrás ejercer tus derechos de acceso, rectificación, cancelación y oposición (ARCO) en cualquier momento, comunicándote a través del correo: **privacidad@clearprint.cl**.

        ### 6. Vigencia:
        Este consentimiento entra en vigencia desde octubre de 2025 y permanecerá activo hasta su modificación o revocación por parte del titular.
        """)
    
    st.markdown("---")
    
    # Checkbox de consentimiento
    col1, col2 = st.columns([1, 4])
    with col1:
        consentimiento = st.checkbox("**✓**", key="check_consentimiento")
    with col2:
        st.markdown("""
        **Declaro haber leído y comprendido esta política y otorgo mi consentimiento libre, 
        informado y específico para el tratamiento de mis datos personales.**
        """)
    
    if consentimiento:
        if st.button("✅ Aceptar y Continuar", type="primary", use_container_width=True):
            st.session_state.consentimiento_otorgado = True
            st.session_state.consentimiento_fecha = datetime.now().isoformat()
            st.session_state.consentimiento_texto = """Al registrarte en la plataforma ClearPrint, autorizas expresamente el tratamiento de tus datos personales conforme a la Ley N°19.628 sobre Protección de la Vida Privada."""
            st.rerun()
    else:
        st.error("❌ Debe aceptar los términos y condiciones para continuar")
        st.stop()
    
    return False

# =============================================================================
# CONEXIÓN CON SUPABASE - NUEVO SISTEMA DE ALMACENAMIENTO
# =============================================================================

import os
from supabase import create_client, Client

def init_supabase_connection():
    """Inicializa la conexión con Supabase"""
    try:
        # PRIMERO intentar con variables de entorno (Streamlit Cloud)
        supabase_url = os.environ.get("SUPABASE_URL")
        supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        
        # SI NO HAY variables de entorno, intentar con secrets.toml (local)
        if not supabase_url or not supabase_key:
            try:
                supabase_url = st.secrets.get("SUPABASE_URL")
                supabase_key = st.secrets.get("SUPABASE_SERVICE_ROLE_KEY")
            except (FileNotFoundError, AttributeError):
                pass
        
        if not supabase_url or not supabase_key:
            st.error("❌ No se encontraron las credenciales de Supabase")
            st.info("""
            **Configuración necesaria:**
            
            **Para desarrollo local:**
            1. Crea una carpeta `.streamlit/`
            2. Dentro crea `secrets.toml`
            3. Agrega:
               SUPABASE_URL = "tu-url.supabase.co"
               SUPABASE_SERVICE_ROLE_KEY = "tu-clave-larga"
            
            **Para producción:**
            - Configura las variables en Streamlit Cloud
            """)
            return None
        
        supabase: Client = create_client(supabase_url, supabase_key)
        
        # Verificar que la conexión funciona
        test_response = supabase.table("projects").select("count", count="exact").limit(1).execute()
        
        st.sidebar.success("✅ Conectado a Supabase")
        return supabase
    except Exception as e:
        st.error(f"❌ Error conectando con Supabase: {str(e)}")
        st.info("""
        **Solución de problemas:**
        1. Verifica tu conexión a internet
        2. Revisa que las credenciales sean correctas
        3. Asegúrate de que Supabase esté activo
        """)
        return None

def save_project_to_supabase(supabase, project_data):
    """Guarda un proyecto completo en Supabase"""
    try:
        # Insertar en la tabla projects
        response = supabase.table("projects").insert(project_data).execute()
        
        if hasattr(response, 'error') and response.error:
            st.error(f"Error guardando proyecto: {response.error}")
            return None
        
        st.success("✅ Proyecto guardado correctamente en Supabase")
        return response.data[0]['id'] if response.data else None
    except Exception as e:
        st.error(f"Error guardando proyecto en Supabase: {e}")
        return None

def list_user_projects(supabase, user_email):
    """Lista todos los proyectos de un usuario"""
    try:
        response = supabase.table("projects")\
                         .select("*")\
                         .eq("user_email", user_email)\
                         .order("created_at", desc=True)\
                         .execute()
        
        if hasattr(response, 'error') and response.error:
            st.error(f"Error obteniendo proyectos: {response.error}")
            return []
        
        return response.data
    except Exception as e:
        st.error(f"Error obteniendo proyectos: {e}")
        return []

def load_project_by_id(supabase, project_id):
    """Carga un proyecto específico por ID"""
    try:
        response = supabase.table("projects")\
                         .select("*")\
                         .eq("id", project_id)\
                         .execute()
        
        if hasattr(response, 'error') and response.error:
            st.error(f"Error cargando proyecto: {response.error}")
            return None
        
        return response.data[0] if response.data else None
    except Exception as e:
        st.error(f"Error cargando proyecto: {e}")
        return None

def upload_excel_to_storage(supabase, file_path, project_id):
    """Sube un archivo Excel al Storage de Supabase"""
    try:
        with open(file_path, 'rb') as file:
            file_data = file.read()
        
        file_name = f"project_{project_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        # Subir a Supabase Storage
        response = supabase.storage.from_("project_files")\
                                .upload(file_name, file_data)
        
        if hasattr(response, 'error') and response.error:
            st.error(f"Error subiendo archivo: {response.error}")
            return None
        
        # Guardar referencia en la tabla project_files
        file_record = {
            "project_id": project_id,
            "filename": file_name,
            "storage_path": response.data.get('Key', file_name)
        }
        
        supabase.table("project_files").insert(file_record).execute()
        
        st.success("✅ Archivo Excel guardado correctamente")
        return file_name
    except Exception as e:
        st.error(f"Error subiendo archivo: {e}")
        return None

def guardar_proyecto_manual():
    """Guarda manualmente el proyecto actual en Supabase - VERSIÓN CONTROLADA"""
    
    # Crear un formulario dedicado solo para guardar
    with st.form(key="guardar_proyecto_form", border=False):
        st.markdown("### 💾 Guardar Proyecto")
        
        # Nombre del proyecto (editable)
        nuevo_nombre = st.text_input(
            "Nombre del proyecto",
            value=st.session_state.current_project_name,
            help="Puedes cambiar el nombre del proyecto si lo deseas"
        )
        
        st.warning("""
        **⚠️ ADVERTENCIA: Esto guardará TODOS los datos ingresados hasta ahora.**
        - Los datos se guardarán en la nube
        - Podrás recuperarlos en cualquier momento
        - Se sobrescribirá la versión anterior si ya existe
        """)
        
        col1, col2, col3 = st.columns([1, 1, 1])
        
        with col2:
            guardar = st.form_submit_button(
                "✅ GUARDAR PROYECTO",
                type="primary",
                use_container_width=True
            )
    
    # SOLO ejecutar guardado si se presionó el botón
    if guardar:
        # Actualizar nombre si cambió
        if nuevo_nombre and nuevo_nombre != st.session_state.current_project_name:
            st.session_state.current_project_name = nuevo_nombre
        
        # Llamar a la función de guardado controlado
        if guardar_proyecto_completo():
            st.rerun()  # Forzar actualización
        else:
            st.error("No se pudo guardar el proyecto")
    
    return False

# =============================================================================
# --- Factores de emisión y parámetros configurables (modificar aquí) ---
# =============================================================================

# --- Potenciales de calentamiento global (GWP) ---
# Unidades: adimensional (relación respecto a CO2)
# Fuente: IPCC AR6 (2021), 100 años
GWP = {
    "CO2": 1,      # IPCC AR6
    "CH4": 27,     # IPCC AR6, metano no fósil
    "N2O": 273     # IPCC AR6
}

# --- Factores IPCC 2006 para emisiones de N2O ---
# Unidades: kg N2O-N / kg N
# Fuente: IPCC 2006 Vol.4 Cap.11 Tabla 11.1. 2019 REFINEMENT
EF1 = 0.01   # Emisión directa de N2O-N por aplicación de N
EF4 = 0.01   # Emisión indirecta de N2O-N por volatilización
EF5 = 0.011 # Emisión indirecta de N2O-N por lixiviación/escurrimiento

# --- Factor IPCC 2006 para emisiones de CO2 por hidrólisis de urea ---
# Unidades: kg CO2 / kg urea
# Fuente: IPCC 2006 Vol.4 Cap.11 Eq. 11.13
# Procedimiento: FE = 0.20 (contenido C en urea) × 44/12 (conversión CO2-C a CO2)
EF_CO2_UREA = 0.20 * (44/12)  # = 0.733 kg CO2 / kg urea

# --- Fracciones por defecto (modificables) ---
# Unidades: adimensional
# Fuente: IPCC 2006 Vol.4 Cap.11 Tabla 11.1. Refinement 2019
FRAC_VOLATILIZACION_INORG = 0.11   # Fracción de N volatilizado de fertilizantes inorgánicos (IPCC)
FRAC_VOLATILIZACION_ORG = 0.21     # Fracción de N volatilizado de fertilizantes orgánicos (IPCC 2006 Vol.4 Cap.11 Tabla 11.1, nota: estiércol sólido 0.2, líquido 0.4; se usa 0.2 como valor conservador)
FRAC_LIXIVIACION = 0.24            # Fracción de N lixiviado (aplica a todo N si precipitación > 1,000 mm) (IPCC)
# Nota: El IPCC no diferencia entre inorgánico y orgánico para lixiviación, usa 0.3 para ambos si corresponde.

# --- Factores de emisión para quema de residuos agrícolas ---
# Unidades: kg gas / kg materia seca quemada
# Fuente: IPCC 2006 Vol.4 Cap.2 Tablas 2.5 y 2.6
EF_CH4_QUEMA = 2.7 / 1000   # kg CH4 / kg MS
EF_N2O_QUEMA = 0.07 / 1000  # kg N2O / kg MS
FRACCION_SECA_QUEMA = 0.8   # adimensional, típico IPCC. ESTE VALOR NO ESTOY 100% SEGURO
FRACCION_QUEMADA = 0.85      # adimensional, típico IPCC

# --- Factores sugeridos para fertilizantes orgánicos (estructura eficiente y compacta) ---
# Unidades: fraccion_seca (adimensional), N/P2O5/K2O (% peso fresco)
# Fuente: IPCC 2006 Vol.4 Cap.10, Tablas 10A.2 y 10A.3, literatura FAO y valores de uso común
FACTORES_ORGANICOS = {
    "Tierra de hoja (quillota)": {
        "fraccion_seca": 1.00,  # 100%
        "N": 0.7,
        "P2O5": 0.0,
        "K2O": 0.0,
        "fuente": "https://biblioteca.inia.cl/server/api/core/bitstreams/102077ad-5b60-46b2-8b35-c0e8250a2965/content"
    },
    "Guano de pavo": {
        "fraccion_seca": 1.00,
        "N": 4.1,
        "P2O5": 0.0,
        "K2O": 0.0,
        "fuente": "https://biblioteca.inia.cl/server/api/core/bitstreams/102077ad-5b60-46b2-8b35-c0e8250a2965/content"
    },
    "Guano de vacuno": {
        "fraccion_seca": 1.00,
        "N": 3.1,
        "P2O5": 0.0,
        "K2O": 0.0,
        "fuente": "https://biblioteca.inia.cl/server/api/core/bitstreams/102077ad-5b60-46b2-8b35-c0e8250a2965/content"
    },
    "Guano de cabra": {
        "fraccion_seca": 1.00,
        "N": 2.2,
        "P2O5": 0.0,
        "K2O": 0.0,
        "fuente": "https://biblioteca.inia.cl/server/api/core/bitstreams/102077ad-5b60-46b2-8b35-c0e8250a2965/content"
    },
    "Guano rojo": {
        "fraccion_seca": 1.00,
        "N": 6.0,
        "P2O5": 9.0,
        "K2O": 1.0,
        "fuente": "https://www.indap.gob.cl/sites/default/files/2022-02/n%C2%BA8-manual-de-produccio%CC%81n-agroecologica.pdf"
    },
    "Harina de sangre": {
        "fraccion_seca": 1.00,
        "N": 13.0,
        "P2O5": 0.0,
        "K2O": 0.0,
        "fuente": "https://www.indap.gob.cl/sites/default/files/2022-02/n%C2%BA8-manual-de-produccio%CC%81n-agroecologica.pdf"
    },
    "Turba de copiapó": {
        "fraccion_seca": 1.00,
        "N": 0.64,
        "P2O5": 0.0,
        "K2O": 0.0,
        "fuente": "https://biblioteca.inia.cl/server/api/core/bitstreams/102077ad-5b60-46b2-8b35-c0e8250a2965/content"
    },
    "Estiercol de vacuno sólido": {
        "fraccion_seca": 0.215,  # 21,5%
        "N": 0.565,
        "P2O5": 0.17,
        "K2O": 0.475,
        "fuente": "https://biblioteca.inia.cl/server/api/core/bitstreams/102077ad-5b60-46b2-8b35-c0e8250a2965/content"
    },
    "Purin de vacuno": {
        "fraccion_seca": 0.075,
        "N": 0.405,
        "P2O5": 0.085,
        "K2O": 0.35,
        "fuente": "https://biblioteca.inia.cl/server/api/core/bitstreams/102077ad-5b60-46b2-8b35-c0e8250a2965/content"
    },
    "Estiércol de cerdo sólido": {
        "fraccion_seca": 0.215,
        "N": 0.58,
        "P2O5": 0.355,
        "K2O": 0.33,
        "fuente": "https://biblioteca.inia.cl/server/api/core/bitstreams/102077ad-5b60-46b2-8b35-c0e8250a2965/content"
    },
    "Purin de cerdo": {
        "fraccion_seca": 0.0665,
        "N": 0.535,
        "P2O5": 0.145,
        "K2O": 0.305,
        "fuente": "https://biblioteca.inia.cl/server/api/core/bitstreams/102077ad-5b60-46b2-8b35-c0e8250a2965/content"
    },
    "Estiércol sólido de ave": {
        "fraccion_seca": 0.475,
        "N": 1.925,
        "P2O5": 1.07,
        "K2O": 1.05,
        "fuente": "https://biblioteca.inia.cl/server/api/core/bitstreams/102077ad-5b60-46b2-8b35-c0e8250a2965/content"
    },
    "Purín de ave": {
        "fraccion_seca": 0.1175,
        "N": 0.895,
        "P2O5": 0.33,
        "K2O": 0.555,
        "fuente": "https://biblioteca.inia.cl/server/api/core/bitstreams/102077ad-5b60-46b2-8b35-c0e8250a2965/content"
    },
    "Otros": {  # Entrada genérica para evitar KeyError
        "fraccion_seca": 1.0,
        "N": 0.0,
        "P2O5": 0.0,
        "K2O": 0.0,
        "fuente": ""
    }
}

# --- Factores de emisión genéricos para nutrientes (por producción) ---
# Unidades: kg CO2e/kg nutriente
# Fuente: Ecoinvent, Agri-footprint, literatura LCA
FE_N_GEN = 3.0    # kg CO2e/kg N
FE_P2O5_GEN = 1.5 # kg CO2e/kg P2O5
FE_K2O_GEN = 1.2  # kg CO2e/kg K2O

# --- Valores por defecto y factores de emisión centralizados ---
valores_defecto = {
    "fe_electricidad": 0.2021,        # kg CO2e/kWh (SEN, promedio 2024, Chile)
    "fe_combustible_generico": 3.98648,   # kg CO2e/litro (LUBRICANTE)
    "fe_agua": 0.00015,               # kg CO2e/litro de agua de riego (DEFRA)
    "fe_maquinaria": 2.5,             # kg CO2e/litro (valor genérico maquinaria)
    "fe_transporte": 0.15,            # kg CO2e/km recorrido (valor genérico transporte)
    "fe_agroquimico": 5.0,            # kg CO2e/kg ingrediente activo (valor genérico)
    "rendimiento_motor": 0.25,        # litros/kWh (valor genérico motor diésel/gasolina)
}

# --- Factores de fertilizantes inorgánicos (puedes modificar aquí) ---
# N_porcentaje: fracción de N en el fertilizante (adimensional)
# Frac_volatilizacion: fracción de N volatilizado (adimensional)
# Frac_lixiviacion: fracción de N lixiviado (adimensional)
# FE_produccion_producto: kg CO2e / kg producto (LCA, Ecoinvent/Agri-footprint)
# FE_produccion_N: kg CO2e / kg N (LCA, Ecoinvent/Agri-footprint)
# Fuente de volatilización/lixiviación: IPCC 2006 Vol.4 Cap.11 Tabla 11.1 y literatura LCA para producción
factores_fertilizantes = {
    "Nitrato de amonio (AN)": [
        {"origen": "Unión Europea", "N_porcentaje": 0.335, "Frac_volatilizacion": 0.05, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 1.112, "Fuente": "https://www.fertilizerseurope.com/wp-content/uploads/2020/01/The-carbon-footprint-of-fertilizer-production_Regional-reference-values.pdf"},
        {"origen": "Norte América", "N_porcentaje": 0.335, "Frac_volatilizacion": 0.05, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 2.249, "Fuente": "https://www.fertilizerseurope.com/wp-content/uploads/2020/01/The-carbon-footprint-of-fertilizer-production_Regional-reference-values.pdf"},
        {"origen": "Latino América", "N_porcentaje": 0.335, "Frac_volatilizacion": 0.05, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 2.124, "Fuente": "https://www.fertilizerseurope.com/wp-content/uploads/2020/01/The-carbon-footprint-of-fertilizer-production_Regional-reference-values.pdf"},
        {"origen": "China, carbón", "N_porcentaje": 0.335, "Frac_volatilizacion": 0.05, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 3.643, "Fuente": "https://www.fertilizerseurope.com/wp-content/uploads/2020/01/The-carbon-footprint-of-fertilizer-production_Regional-reference_values.pdf"},
        {"origen": "Rusia", "N_porcentaje": 0.335, "Frac_volatilizacion": 0.05, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 2.850, "Fuente": "https://www.researchgate.net/profile/Frank-Brentrup-2/publication/312553933_Carbon_footprint_analysis_of_mineral_fertilizer_production_in_Europe_and_other_world_regions/links/5881ec8d4585150dde4012fe/Carbon-footprint-analysis-of-mineral-fertilizer-production-in-Europe-and-other-world-regions.pdf"},
        {"origen": "China, gas", "N_porcentaje": 0.335, "Frac_volatilizacion": 0.05, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 2.836, "Fuente": "https://www.fertilizerseurope.com/wp-content/uploads/2020/01/The-carbon-footprint-of-fertilizer-production_Regional-reference-values.pdf"},
        {"origen": "Promedio", "N_porcentaje": 0.335, "Frac_volatilizacion": 0.05, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 2.469, "Fuente": ""}
    ],
    "Nitrato de amonio cálcico (CAN)": [
        {"origen": "Unión Europea", "N_porcentaje": 0.27, "Frac_volatilizacion": 0.05, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 0.951, "Fuente": "https://www.fertilizerseurope.com/wp-content/uploads/2020/01/The-carbon-footprint-of-fertilizer-production_Regional-reference-values.pdf"},
        {"origen": "Norte América", "N_porcentaje": 0.27, "Frac_volatilizacion": 0.05, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 1.870, "Fuente": "https://www.fertilizerseurope.com/wp-content/uploads/2020/01/The-carbon-footprint-of-fertilizer-production_Regional-reference-values.pdf"},
        {"origen": "Latino América", "N_porcentaje": 0.27, "Frac_volatilizacion": 0.05, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 1.779, "Fuente": "https://www.fertilizerseurope.com/wp-content/uploads/2020/01/The-carbon-footprint-of-fertilizer-production_Regional-reference-values.pdf"},
        {"origen": "China, carbón", "N_porcentaje": 0.27, "Frac_volatilizacion": 0.05, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 3.023, "Fuente": "https://www.fertilizerseurope.com/wp-content/uploads/2020/01/The-carbon-footprint-of-fertilizer-production_Regional-reference-values.pdf"},
        {"origen": "Rusia", "N_porcentaje": 0.27, "Frac_volatilizacion": 0.05, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 2.350, "Fuente": "https://www.researchgate.net/profile/Frank-Brentrup-2/publication/312553933_Carbon_footprint_analysis_of_mineral_fertilizer_production_in_Europe_and_other_world_regions/links/5881ec8d4585150dde4012fe/Carbon-footprint-analysis-of-mineral-fertilizer-production-in-Europe-and-other-world-regions.pdf"},
        {"origen": "China, gas", "N_porcentaje": 0.27, "Frac_volatilizacion": 0.05, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 2.358, "Fuente": "https://www.fertilizerseurope.com/wp-content/uploads/2020/01/The-carbon-footprint-of-fertilizer-production_Regional-reference-values.pdf"},
        {"origen": "Promedio", "N_porcentaje": 0.27, "Frac_volatilizacion": 0.05, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 2.055, "Fuente": ""}
    ],
    "Urea": [
        {"origen": "Unión Europea", "N_porcentaje": 0.46, "Frac_volatilizacion": 0.15, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 1.611, "Fuente": "https://www.fertilizerseurope.com/wp-content/uploads/2020/01/The-carbon-footprint-of-fertilizer-production_Regional-reference-values.pdf"},
        {"origen": "Norte América", "N_porcentaje": 0.46, "Frac_volatilizacion": 0.15, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 1.739, "Fuente": "https://www.fertilizerseurope.com/wp-content/uploads/2020/01/The-carbon-footprint-of-fertilizer-production_Regional-reference-values.pdf"},
        {"origen": "Latino América", "N_porcentaje": 0.46, "Frac_volatilizacion": 0.15, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 1.746, "Fuente": "https://www.fertilizerseurope.com/wp-content/uploads/2020/01/The-carbon-footprint-of-fertilizer-production_Regional-reference-values.pdf"},
        {"origen": "China, carbón", "N_porcentaje": 0.46, "Frac_volatilizacion": 0.15, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 3.002, "Fuente": "https://www.fertilizerseurope.com/wp-content/uploads/2020/01/The-carbon-footprint-of-fertilizer-production_Regional-reference-values.pdf"},
        {"origen": "Rusia", "N_porcentaje": 0.46, "Frac_volatilizacion": 0.15, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 1.180, "Fuente": "https://www.researchgate.net/profile/Frank-Brentrup-2/publication/312553933_Carbon_footprint_analysis_of_mineral_fertilizer_production_in_Europe_and_other_world_regions/links/5881ec8d4585150dde4012fe/Carbon-footprint-analysis-of-mineral-fertilizer-production-in-Europe-and-other-world-regions.pdf"},
        {"origen": "China, gas", "N_porcentaje": 0.46, "Frac_volatilizacion": 0.15, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 1.905, "Fuente": "https://www.fertilizerseurope.com/wp-content/uploads/2020/01/The-carbon-footprint-of-fertilizer-production_Regional-reference-values.pdf"},
        {"origen": "Promedio", "N_porcentaje": 0.46, "Frac_volatilizacion": 0.15, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 1.864, "Fuente": ""}
    ],
    "Nitrato de Amonio y Urea (UAN)": [
        {"origen": "Unión Europea", "N_porcentaje": 0.30, "Frac_volatilizacion": 0.10, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 1.021, "Fuente": "https://www.fertilizerseurope.com/wp-content/uploads/2020/01/The-carbon-footprint-of-fertilizer-production_Regional-reference-values.pdf"},
        {"origen": "Norte América", "N_porcentaje": 0.30, "Frac_volatilizacion": 0.10, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 1.571, "Fuente": "https://www.fertilizerseurope.com/wp-content/uploads/2020/01/The-carbon-footprint-of-fertilizer-production_Regional-reference-values.pdf"},
        {"origen": "Latino América", "N_porcentaje": 0.30, "Frac_volatilizacion": 0.10, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 1.526, "Fuente": "https://www.fertilizerseurope.com/wp-content/uploads/2020/01/The-carbon-footprint-of-fertilizer-production_Regional-reference-values.pdf"},
        {"origen": "China, carbón", "N_porcentaje": 0.30, "Frac_volatilizacion": 0.10, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 2.615, "Fuente": "https://www.fertilizerseurope.com/wp-content/uploads/2020/01/The-carbon-footprint-of-fertilizer-production_Regional-reference-values.pdf"},
        {"origen": "Rusia", "N_porcentaje": 0.30, "Frac_volatilizacion": 0.10, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 1.650, "Fuente": "https://www.researchgate.net/profile/Frank-Brentrup-2/publication/312553933_Carbon_footprint_analysis_of_mineral_fertilizer_production_in_Europe_and_other_world_regions/links/5881ec8d4585150dde4012fe/Carbon-footprint-analysis-of-mineral-fertilizer-production-in-Europe-and-other-world-regions.pdf"},
        {"origen": "China, gas", "N_porcentaje": 0.30, "Frac_volatilizacion": 0.10, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 1.896, "Fuente": "https://www.fertilizerseurope.com/wp-content/uploads/2020/01/The-carbon-footprint-of-fertilizer-production_Regional-reference-values.pdf"},
        {"origen": "Promedio", "N_porcentaje": 0.30, "Frac_volatilizacion": 0.10, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 1.713, "Fuente": ""}
    ],
    "Nitrosulfato de amonio (ANS)": [
        {"origen": "Europa", "N_porcentaje": 0.26, "Frac_volatilizacion": 0.05, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 0.820, "Fuente": "https://www.researchgate.net/profile/Frank-Brentrup-2/publication/312553933_Carbon_footprint_analysis_of_mineral_fertilizer_production_in_Europe_and_other_world_regions/links/5881ec8d4585150dde4012fe/Carbon-footprint-analysis-of-mineral-fertilizer-production-in-Europe-and-other-world-regions.pdf"},
        {"origen": "Rusia", "N_porcentaje": 0.26, "Frac_volatilizacion": 0.05, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 1.580, "Fuente": "https://www.researchgate.net/profile/Frank-Brentrup-2/publication/312553933_Carbon_footprint_analysis_of_mineral_fertilizer_production_in_Europe_and_other_world_regions/links/5881ec8d4585150dde4012fe/Carbon-footprint-analysis-of-mineral-fertilizer-production-in-Europe-and-other-world-regions.pdf"},
        {"origen": "Estados Unidos", "N_porcentaje": 0.26, "Frac_volatilizacion": 0.05, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 1.440, "Fuente": "https://www.researchgate.net/profile/Frank-Brentrup-2/publication/312553933_Carbon_footprint_analysis_of_mineral_fertilizer_production_in_Europe_and_other_world_regions/links/5881ec8d4585150dde4012fe/Carbon-footprint-analysis-of-mineral-fertilizer-production-in-Europe-and-other-world-regions.pdf"},
        {"origen": "China", "N_porcentaje": 0.26, "Frac_volatilizacion": 0.05, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 2.220, "Fuente": "https://www.researchgate.net/profile/Frank-Brentrup-2/publication/312553933_Carbon_footprint_analysis_of_mineral_fertilizer_production_in_Europe_and_other_world_regions/links/5881ec8d4585150dde4012fe/Carbon-footprint-analysis-of-mineral-fertilizer-production-in-Europe-and-other-world-regions.pdf"},
        {"origen": "Promedio", "N_porcentaje": 0.26, "Frac_volatilizacion": 0.05, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 1.515, "Fuente": ""}
    ],
    "Nitrato de calcio (CN)": [
        {"origen": "Europa", "N_porcentaje": 0.155, "Frac_volatilizacion": 0.01, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 0.670, "Fuente": "https://www.researchgate.net/profile/Frank-Brentrup-2/publication/312553933_Carbon_footprint_analysis_of_mineral_fertilizer_production_in_Europe_and_other_world_regions/links/5881ec8d4585150dde4012fe/Carbon-footprint-analysis-of-mineral-fertilizer-production-in-Europe-and-other-world-regions.pdf"},
        {"origen": "Rusia", "N_porcentaje": 0.155, "Frac_volatilizacion": 0.01, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 2.030, "Fuente": "https://www.researchgate.net/profile/Frank-Brentrup-2/publication/312553933_Carbon_footprint_analysis_of_mineral_fertilizer_production_in_Europe_and_other_world_regions/links/5881ec8d4585150dde4012fe/Carbon-footprint-analysis-of-mineral-fertilizer-production-in-Europe-and-other-world-regions.pdf"},
        {"origen": "Estados Unidos", "N_porcentaje": 0.155, "Frac_volatilizacion": 0.01, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 1.760, "Fuente": "https://www.researchgate.net/profile/Frank-Brentrup-2/publication/312553933_Carbon_footprint_analysis_of_mineral_fertilizer_production_in_Europe_and_other_world_regions/links/5881ec8d4585150dde4012fe/Carbon-footprint-analysis-of-mineral-fertilizer-production-in-Europe-and-other-world-regions.pdf"},
        {"origen": "China", "N_porcentaje": 0.155, "Frac_volatilizacion": 0.01, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 2.200, "Fuente": "https://www.researchgate.net/profile/Frank-Brentrup-2/publication/312553933_Carbon_footprint_analysis_of_mineral_fertilizer_production_in_Europe_and_other_world_regions/links/5881ec8d4585150dde4012fe/Carbon-footprint-analysis-of-mineral-fertilizer-production-in-Europe-and-other-world-regions.pdf"},
        {"origen": "Promedio", "N_porcentaje": 0.155, "Frac_volatilizacion": 0.01, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 1.665, "Fuente": ""}
    ],
    "Sulfato de amonio (AS)": [
        {"origen": "Europa", "N_porcentaje": 0.21, "Frac_volatilizacion": 0.08, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 0.570, "Fuente": "https://www.researchgate.net/profile/Frank-Brentrup-2/publication/312553933_Carbon_footprint_analysis_of_mineral_fertilizer_production_in_Europe_and_other_world_regions/links/5881ec8d4585150dde4012fe/Carbon-footprint-analysis-of-mineral-fertilizer-production-in-Europe-and-other-world-regions.pdf"},
        {"origen": "Rusia", "N_porcentaje": 0.21, "Frac_volatilizacion": 0.08, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 0.710, "Fuente": "https://www.researchgate.net/profile/Frank-Brentrup-2/publication/312553933_Carbon_footprint_analysis_of_mineral_fertilizer_production_in_Europe_and_other_world_regions/links/5881ec8d4585150dde4012fe/Carbon-footprint-analysis-of-mineral-fertilizer-production-in-Europe-and-other-world-regions.pdf"},
        {"origen": "Estados Unidos", "N_porcentaje": 0.21, "Frac_volatilizacion": 0.08, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 0.690, "Fuente": "https://www.researchgate.net/profile/Frank-Brentrup-2/publication/312553933_Carbon_footprint_analysis_of_mineral_fertilizer_production_in_Europe_and_other_world_regions/links/5881ec8d4585150dde4012fe/Carbon-footprint-analysis-of-mineral-fertilizer-production-in-Europe-and-other-world-regions.pdf"},
        {"origen": "China", "N_porcentaje": 0.21, "Frac_volatilizacion": 0.08, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 1.360, "Fuente": "https://www.researchgate.net/profile/Frank-Brentrup-2/publication/312553933_Carbon_footprint_analysis_of_mineral_fertilizer_production_in_Europe_and_other_world_regions/links/5881ec8d4585150dde4012fe/Carbon-footprint-analysis-of-mineral-fertilizer-production-in-Europe-and-other-world-regions.pdf"},
        {"origen": "Promedio", "N_porcentaje": 0.21, "Frac_volatilizacion": 0.08, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 0.833, "Fuente": ""}
    ],
    "Fosfato monoamónico (MAP)": [
        {"origen": "Chile", "N_porcentaje": 0.10, "Frac_volatilizacion": 0.08, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 0.380, "Fuente": "https://www.climatiq.io/data/emission-factor/941370dd-318b-46ad-941b-80b9c861cf69"}
    ],
    "Fosfato diamonico (DAP)": [
        {"origen": "Europa", "N_porcentaje": 0.18, "Frac_volatilizacion": 0.08, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 0.640, "Fuente": "https://www.researchgate.net/profile/Frank-Brentrup-2/publication/312553933_Carbon_footprint_analysis_of_mineral_fertilizer_production_in_Europe_and_other_world_regions/links/5881ec8d4585150dde4012fe/Carbon-footprint-analysis-of-mineral-fertilizer-production-in-Europe-and-other-world-regions.pdf"},
        {"origen": "Rusia", "N_porcentaje": 0.18, "Frac_volatilizacion": 0.08, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 0.810, "Fuente": "https://www.researchgate.net/profile/Frank-Brentrup-2/publication/312553933_Carbon_footprint_analysis_of_mineral_fertilizer_production_in_Europe_and_other_world_regions/links/5881ec8d4585150dde4012fe/Carbon-footprint-analysis-of-mineral-fertilizer-production-in-Europe-and-other-world-regions.pdf"},
        {"origen": "Estados Unidos", "N_porcentaje": 0.18, "Frac_volatilizacion": 0.08, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 0.730, "Fuente": "https://www.researchgate.net/profile/Frank-Brentrup-2/publication/312553933_Carbon_footprint_analysis_of_mineral_fertilizer_production_in_Europe_and_other_world_regions/links/5881ec8d4585150dde4012fe/Carbon-footprint-analysis-of-mineral-fertilizer-production-in-Europe-and-other-world-regions.pdf"},
        {"origen": "China", "N_porcentaje": 0.18, "Frac_volatilizacion": 0.08, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 1.330, "Fuente": "https://www.researchgate.net/profile/Frank-Brentrup-2/publication/312553933_Carbon_footprint_analysis_of_mineral_fertilizer_production_in_Europe_and_other_world_regions/links/5881ec8d4585150dde4012fe/Carbon-footprint-analysis-of-mineral-fertilizer-production-in-Europe-and-other-world-regions.pdf"},
        {"origen": "Promedio", "N_porcentaje": 0.18, "Frac_volatilizacion": 0.08, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 0.878, "Fuente": ""}
    ],
    "Superfosfato triple (TSP)": [
        {"origen": "Europa", "N_porcentaje": 0, "Frac_volatilizacion": 0.08, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 0.18, "Año": 2011, "Fuente": "https://www.researchgate.net/profile/Frank-Brentrup-2/publication/312553933_Carbon_footprint_analysis_of_mineral_fertilizer_production_in_Europe_and_other_world_regions/links/5881ec8d4585150dde4012fe/Carbon-footprint-analysis-of-mineral-fertilizer-production-in-Europe-and-other-world-regions.pdf"},
        {"origen": "Rusia", "N_porcentaje": 0, "Frac_volatilizacion": 0.08, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 0.25, "Año": 2011, "Fuente": "https://www.researchgate.net/profile/Frank-Brentrup-2/publication/312553933_Carbon_footprint_analysis_of_mineral_fertilizer_production_in_Europe_and_other_world_regions/links/5881ec8d4585150dde4012fe/Carbon-footprint-analysis-of-mineral-fertilizer-production-in-Europe-and-other-world-regions.pdf"},
        {"origen": "Estados Unidos", "N_porcentaje": 0, "Frac_volatilizacion": 0.08, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 0.19, "Año": 2011, "Fuente": "https://www.researchgate.net/profile/Frank-Brentrup-2/publication/312553933_Carbon_footprint_analysis_of_mineral_fertilizer_production_in_Europe_and_other_world_regions/links/5881ec8d4585150dde4012fe/Carbon-footprint-analysis-of-mineral-fertilizer-production-in-Europe-and-other-world-regions.pdf"},
        {"origen": "China", "N_porcentaje": 0, "Frac_volatilizacion": 0.08, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 0.26, "Año": 2011, "Fuente": "https://www.researchgate.net/profile/Frank-Brentrup-2/publication/312553933_Carbon_footprint_analysis_of_mineral_fertilizer_production_in_Europe_and_other_world_regions/links/5881ec8d4585150dde4012fe/Carbon-footprint-analysis-of-mineral-fertilizer-production-in-Europe-and-other-world-regions.pdf"},
        {"origen": "Promedio", "N_porcentaje": 0, "Frac_volatilizacion": 0.08, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 0.22, "Año": 2011, "Fuente": "https://www.researchgate.net/profile/Frank-Brentrup-2/publication/312553933_Carbon_footprint_analysis_of_mineral_fertilizer_production_in_Europe_and_other_world_regions/links/5881ec8d4585150dde4012fe/Carbon-footprint-analysis-of-mineral-fertilizer-production-in-Europe-and-other-world-regions.pdf"}
    ],
    "Cloruro de Potasio (MOP)": [
        {"origen": "Europa", "N_porcentaje": 0, "Frac_volatilizacion": 0.11, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 0.23, "Año": 2011, "Fuente": "https://www.researchgate.net/profile/Frank-Brentrup-2/publication/312553933_Carbon_footprint_analysis_of_mineral_fertilizer_production_in_Europe_and_other_world_regions/links/5881ec8d4585150dde4012fe/Carbon-footprint-analysis-of-mineral-fertilizer-production-in-Europe-and-other-world-regions.pdf"},
        {"origen": "Rusia", "N_porcentaje": 0, "Frac_volatilizacion": 0.11, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 0.23, "Año": 2011, "Fuente": "https://www.researchgate.net/profile/Frank-Brentrup-2/publication/312553933_Carbon_footprint_analysis_of_mineral_fertilizer_production_in_Europe_and_other_world_regions/links/5881ec8d4585150dde4012fe/Carbon-footprint-analysis-of-mineral-fertilizer-production-in-Europe-and-other-world-regions.pdf"},
        {"origen": "Estados Unidos", "N_porcentaje": 0, "Frac_volatilizacion": 0.11, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 0.23, "Año": 2011, "Fuente": "https://www.researchgate.net/profile/Frank-Brentrup-2/publication/312553933_Carbon_footprint_analysis_of_mineral_fertilizer_production_in_Europe_and_other_world_regions/links/5881ec8d4585150dde4012fe/Carbon-footprint-analysis-of-mineral-fertilizer-production-in-Europe-and-other-world-regions.pdf"},
        {"origen": "China", "N_porcentaje": 0, "Frac_volatilizacion": 0.11, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 0.23, "Año": 2011, "Fuente": "https://www.researchgate.net/profile/Frank-Brentrup-2/publication/312553933_Carbon_footprint_analysis_of_mineral_fertilizer_production_in_Europe_and_other_world_regions/links/5881ec8d4585150dde4012fe/Carbon-footprint-analysis-of-mineral-fertilizer-production-in-Europe-and-other-world-regions.pdf"},
        {"origen": "Promedio", "N_porcentaje": 0, "Frac_volatilizacion": 0.11, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 0.23, "Año": 2011, "Fuente": "https://www.researchgate.net/profile/Frank-Brentrup-2/publication/312553933_Carbon_footprint_analysis_of_mineral_fertilizer_production_in_Europe_and_other_world_regions/links/5881ec8d4585150dde4012fe/Carbon-footprint-analysis-of-mineral-fertilizer-production-in-Europe-and-other-world-regions.pdf"}
    ],
    "Ácido bórico": [
        {"origen": "Promedio", "N_porcentaje": 0.00, "Frac_volatilizacion": 0.11, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 5.52, "Fuente": "https://www.researchgate.net/publication/351106329_Life_cycle_assessment_on_boron_production_is_boric_acid_extraction_from_salt-lake_brine_environmentally_friendly"}
    ],
    "Ácido fosfórico": [
        {"origen": "Promedio", "N_porcentaje": 0.00, "Frac_volatilizacion": 0.11, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 5.52, "Fuente": "https://apps.carboncloud.com/climatehub/product-reports/id/216857142454"}
    ],
    "Cloruro de potasio": [
        {"origen": "Promedio", "N_porcentaje": 0.00, "Frac_volatilizacion": 0.11, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 0.22, "Fuente": "https://apps.carboncloud.com/climatehub/product-reports/id/216857142454"}
    ],
    "Hidróxido de potasio": [
        {"origen": "Promedio", "N_porcentaje": 0.00, "Frac_volatilizacion": 0.11, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 1.48, "Fuente": "https://apps.carboncloud.com/climatehub/product-reports/id/216857142454"}
    ],
    "NPK": [
        {"origen": "Europa", "N_porcentaje": 0.15, "Frac_volatilizacion": 0.11, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 0.730, "Fuente": "https://www.researchgate.net/profile/Frank-Brentrup-2/publication/312553933_Carbon_footprint_analysis_of_mineral_fertilizer_production_in_Europe_and_other_world_regions/links/5881ec8d4585150dde4012fe/Carbon-footprint-analysis-of-mineral-fertilizer-production-in-Europe-and-other-world-regions.pdf"},
        {"origen": "Rusia", "N_porcentaje": 0.15, "Frac_volatilizacion": 0.11, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 1.400, "Fuente": "https://www.researchgate.net/profile/Frank-Brentrup-2/publication/312553933_Carbon_footprint_analysis_of_mineral_fertilizer_production_in_Europe_and_other_world_regions/links/5881ec8d4585150dde4012fe/Carbon-footprint-analysis-of-mineral-fertilizer-production-in-Europe-and-other-world-regions.pdf"},
        {"origen": "Estados Unidos", "N_porcentaje": 0.15, "Frac_volatilizacion": 0.11, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 1.270, "Fuente": "https://www.researchgate.net/profile/Frank-Brentrup-2/publication/312553933_Carbon_footprint_analysis_of_mineral_fertilizer_production_in_Europe_and_other_world_regions/links/5881ec8d4585150dde4012fe/Carbon-footprint-analysis-of-mineral-fertilizer-production-in-Europe-and-other-world-regions.pdf"},
        {"origen": "China", "N_porcentaje": 0.15, "Frac_volatilizacion": 0.11, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 1.730, "Fuente": "https://www.researchgate.net/profile/Frank-Brentrup-2/publication/312553933_Carbon_footprint_analysis_of_mineral_fertilizer_production_in_Europe_and_other_world_regions/links/5881ec8d4585150dde4012fe/Carbon-footprint-analysis-of-mineral-fertilizer-production-in-Europe-and-other-world-regions.pdf"},
        {"origen": "Promedio", "N_porcentaje": 0.15, "Frac_volatilizacion": 0.11, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 1.283, "Fuente": ""}
    ],
    "Otros": [
        {"origen": "Otros", "N_porcentaje": 0.15, "Frac_volatilizacion": 0.11, "Frac_lixiviacion": 0.24, "FE_produccion_producto": 0, "Fuente": ""}
    ]
}

# --- Factores de emisión organizados por categoría (actualizado con datos detallados y fuentes) ---
factores_emision = {
    'pesticidas': {
        'Media': 5.1,  # kg CO2e / kg i.a. (https://doi.org/10.1016/j.envint.2004.03.005)
    },
    'fungicidas': {
        'Media': 3.9,  # kg CO2e / kg i.a. (https://doi.org/10.1016/j.envint.2004.03.005)
        'Ferbam': 1.2,  # https://doi.org/10.1016/j.envint.2004.03.028
        'Maneb': 2.0,   # https://doi.org/10.1016/j.envint.2004.03.029
        'Capitan': 2.3, # https://doi.org/10.1016/j.envint.2004.03.030
        'Benomilo': 8.0 # https://doi.org/10.1016/j.envint.2004.03.031
    },
    'insecticidas': {
        'Media': 5.1,  # kg CO2e / kg i.a. (https://doi.org/10.1016/j.envint.2004.03.005)
        'Metil paratión': 3.2,   # https://doi.org/10.1016/j.envint.2004.03.032
        'Forato': 4.2,           # https://doi.org/10.1016/j.envint.2004.03.033
        'Carbofurano': 9.1,      # https://doi.org/10.1016/j.envint.2004.03.034
        'Carbaril': 3.1,         # https://doi.org/10.1016/j.envint.2004.03.035
        'Taxafeno': 1.2,         # https://doi.org/10.1016/j.envint.2004.03.036
        'Cipermetrina': 11.7,    # https://doi.org/10.1016/j.envint.2004.03.037
        'Clorodimeformo': 5.0,   # https://doi.org/10.1016/j.envint.2004.03.038
        'lindano': 1.2,          # https://doi.org/10.1016/j.envint.2004.03.039
        'Malatión': 4.6,         # https://doi.org/10.1016/j.envint.2004.03.040
        'Partión': 2.8,          # https://doi.org/10.1016/j.envint.2004.03.041
        'Metoxicloro': 1.4       # https://doi.org/10.1016/j.envint.2004.03.042
    },
    'herbicidas': {
        'Media': 6.3,        # https://doi.org/10.1016/j.envint.2004.03.005
        '2, 4-D': 1.7,       # https://doi.org/10.1016/j.envint.2004.03.005
        '2, 4, 5-T': 2.7,    # https://doi.org/10.1016/j.envint.2004.03.006
        'Alacloro': 5.6,     # https://doi.org/10.1016/j.envint.2004.03.007
        'Atrazina': 3.8,     # https://doi.org/10.1016/j.envint.2004.03.008
        'Bentazón': 8.7,     # https://doi.org/10.1016/j.envint.2004.03.009
        'Butilato': 2.8,     # https://doi.org/10.1016/j.envint.2004.03.010
        'Cloramben': 3.4,    # https://doi.org/10.1016/j.envint.2004.03.011
        'Clorsulfurón': 7.3, # https://doi.org/10.1016/j.envint.2004.03.012
        'Cianazina': 4.0,    # https://doi.org/10.1016/j.envint.2004.03.013
        'Dicamba': 5.9,      # https://doi.org/10.1016/j.envint.2004.03.014
        'Dinosaurio': 1.6,   # https://doi.org/10.1016/j.envint.2004.03.015
        'Diquat': 8.0,       # https://doi.org/10.1016/j.envint.2004.03.016
        'Diurón': 5.4,       # https://doi.org/10.1016/j.envint.2004.03.017
        'EPTC': 3.2,         # https://doi.org/10.1016/j.envint.2004.03.018
        'Fluazifop-butilo': 10.4, # https://doi.org/10.1016/j.envint.2004.03.019
        'Fluometurón': 7.1,  # https://doi.org/10.1016/j.envint.2004.03.020
        'Glifosato': 9.1,    # https://doi.org/10.1016/j.envint.2004.03.021
        'Linuron': 5.8,      # https://doi.org/10.1016/j.envint.2004.03.022
        'MCPA': 2.6,         # https://doi.org/10.1016/j.envint.2004.03.023
        'Metolaclor': 5.5,   # https://doi.org/10.1016/j.envint.2004.03.024
        'Paraquat': 9.2,     # https://doi.org/10.1016/j.envint.2004.03.025
        'Propaclor': 5.8,    # https://doi.org/10.1016/j.envint.2004.03.026
        'Trifluralina': 3.0  # https://doi.org/10.1016/j.envint.2004.03.027
    },
    'agua': valores_defecto["fe_agua"],                # kg CO2e / litro de agua de riego (LCA)
    'maquinaria': valores_defecto["fe_maquinaria"],    # kg CO2e / litro de combustible (valor genérico, no se usa si tienes factores_combustible)
    'materiales': {
        'PET': 2.1,                # kg CO2e / kg material (LCA)
        'HDPE': 1.9,               # kg CO2e / kg material (LCA)
        'Cartón': 0.7,             # kg CO2e / kg material (LCA)
        'Vidrio': 1.2,             # kg CO2e / kg material (LCA)
        'Otro': 1.0                # kg CO2e / kg material (LCA)
    },
    'transporte': valores_defecto["fe_transporte"]     # kg CO2e / km recorrido (valor genérico, puede variar según tipo de transporte)
}

# --- Factores de emisión para gestión de residuos vegetales (IPCC 2006 Vol.5, Cap.3, Tabla 3.4) ---
# Compostaje aeróbico de residuos vegetales - factores de emisión IPCC
factores_residuos = {
    "fraccion_seca": 0.8,  # Fracción seca de biomasa (adimensional, típico 0.8, IPCC)
    "compostaje": {
        "base_seca": {
            "EF_CH4": 0.010,    # kg CH4 / kg materia seca compostada (IPCC 2006 Vol.5 Cap.3 Tabla 3.4)
            "EF_N2O": 0.0006    # kg N2O / kg materia seca compostada (IPCC 2006 Vol.5 Cap.3 Tabla 3.4)
        },
        "base_humeda": {
            "EF_CH4": 0.004,    # kg CH4 / kg materia húmeda compostada (IPCC 2006 Vol.5 Cap.3 Tabla 3.4)
            "EF_N2O": 0.0003    # kg N2O / kg materia húmeda compostada (IPCC 2006 Vol.5 Cap.3 Tabla 3.4)
        }
    },
    "incorporacion": {
        "fraccion_C": 0.45,        # Fracción de C en biomasa seca (adimensional, IPCC 2006 Vol.4 Cap.2)
        "fraccion_estabilizada": 0.1  # Fracción de C estabilizada en suelo (adimensional, solo opción avanzada, IPCC)
    }
}

# --- Factores de emisión de combustibles ---
factores_combustible = {
    "Diesel (mezcla promedio biocombustibles)": 2.51279,        # kg CO2e / litro (DEFRA)
    "Diesel (100% mineral)": 2.66155,                           # kg CO2e / litro (DEFRA)
    "Gasolina (mezcla media de biocombustibles)": 2.0844,       # kg CO2e / litro (DEFRA)
    "Gasolina (100% gasolina mineral)": 2.66155,                # kg CO2e / litro (DEFRA)
    "Gas Natural Comprimido": 0.44942,                          # kg CO2e / litro (DEFRA)
    "Gas Natural Licuado": 1.17216,                             # kg CO2e / litro (DEFRA)
    "Gas Licuado de petróleo": 1.55713,                         # kg CO2e / litro (DEFRA)
    "Aceite combustible": 3.17493,                              # kg CO2e / litro (DEFRA)
    "Gasóleo": 2.75541,                                         # kg CO2e / litro (DEFRA) (original:)
    "Lubricante": 2.74934,                                      # kg CO2e / litro (DEFRA) (original:)
    "Nafta": 2.11894,                                           # kg CO2e / litro (DEFRA)
    "Butano": 1.74532,                                          # kg CO2e / litro (DEFRA)
    "Otros gases de petróleo": 0.94441,                         # kg CO2e / litro (DEFRA)
    "Propano": 1.54357,                                         # kg CO2e / litro (DEFRA)
    "Aceite quemado": 2.54015,                                  # kg CO2e / litro (DEFRA)
    "Eléctrico": valores_defecto["fe_electricidad"],            # kg CO2e / kWh (valor genérico)
    "Otro": valores_defecto["fe_combustible_generico"]
}

# --- Rendimientos de maquinaria (litros/hora) ---
rendimientos_maquinaria = {
    "Tractor": 10,         # litros de combustible / hora de uso (valor típico)
    "Cosechadora": 15,     # litros de combustible / hora de uso (valor típico)
    "Camión": 25,          # litros de combustible / hora de uso (valor típico)
    "Pulverizadora": 8,    # litros de combustible / hora de uso (valor típico)
    "Otro": 10             # litros de combustible / hora de uso (valor genérico)
}

# --- Opciones de labores ---
opciones_labores = [
    "Siembra", "Cosecha", "Fertilización", "Aplicación de agroquímicos",
    "Riego", "Poda", "Transporte interno", "Otro"
]

# =============================================================================
# --- FIN DE BLOQUE DE FACTORES Y UNIDADES ---
# =============================================================================

# =============================================================================
# --- GENERADOR DE CLAVES ÚNICAS PARA GRÁFICOS ---
# =============================================================================

if 'plot_counter' not in st.session_state:
    st.session_state.plot_counter = 0

def get_unique_key():
    st.session_state.plot_counter += 1
    return f"plot_{st.session_state.plot_counter}"

# =============================================================================
# SISTEMA DE USUARIOS Y PROYECTOS - OBLIGATORIO
# =============================================================================

def mostrar_sistema_usuarios():
    """Sistema simplificado de usuarios usando Supabase - VERSIÓN COMPLETAMENTE CORREGIDA"""
    
    # =========================================================================
    # 1. INICIALIZACIÓN DE CONEXIÓN
    # =========================================================================
    if 'supabase' not in st.session_state:
        st.session_state.supabase = init_supabase_connection()
    
    # Si no hay conexión, mostrar opciones de recuperación
    if st.session_state.supabase is None:
        st.error("""
        ❌ **Error de conexión con la base de datos**
        
        No se pudo conectar con Supabase. Por favor:
        """)
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🔄 Reintentar conexión"):
                st.session_state.supabase = init_supabase_connection()
                st.rerun()
        
        with col2:
            if st.button("🚀 Continuar sin conexión"):
                st.session_state.user_authenticated = True
                st.session_state.current_user_email = "usuario_local@ejemplo.com"
                st.session_state.current_project_id = "local_" + str(uuid.uuid4())
                st.session_state.current_project_name = "Proyecto Local"
                st.session_state.supabase = None
                st.rerun()
        
        st.info("""
        **Modo sin conexión:**
        - Podrás usar la calculadora normalmente
        - Los datos se guardarán temporalmente en tu navegador
        - Para guardar permanentemente, necesitarás conexión a Supabase
        """)
        
        if st.session_state.get('user_authenticated', False) and st.session_state.get('supabase') is None:
            return True
        else:
            st.stop()
    
    # =========================================================================
    # 2. INICIALIZACIÓN DE ESTADO
    # =========================================================================
    estados_requeridos = {
        'user_authenticated': False,
        'current_user_email': None,
        'current_project_id': None,
        'current_project_name': None
    }
    
    for estado, valor_inicial in estados_requeridos.items():
        if estado not in st.session_state:
            st.session_state[estado] = valor_inicial
    
    # =========================================================================
    # 3. PANTALLA DE INICIO DE SESIÓN
    # =========================================================================
    if not st.session_state.user_authenticated:
        st.markdown("---")
        st.header("🔐 Acceso a AgroPrint")
        
        with st.form("login_form"):
            user_email = st.text_input("📧 Correo electrónico", placeholder="tu.email@ejemplo.com")
            
            submitted = st.form_submit_button("🚀 Ingresar a AgroPrint", type="primary", use_container_width=True)
            
            if submitted:
                if not user_email:
                    st.error("❌ El correo electrónico es obligatorio")
                else:
                    st.session_state.user_authenticated = True
                    st.session_state.current_user_email = user_email
                    st.session_state.current_project_id = None
                    st.session_state.current_project_name = None
                    st.success(f"✅ ¡Bienvenido {user_email}!")
                    st.rerun()
        
        st.info("""
        **💡 ¿Primera vez?**
        - Solo ingresa tu correo electrónico
        - Después podrás crear un nuevo proyecto o cargar uno existente
        - Todos los datos se guardarán bajo demanda cuando tú decidas
        """)
        st.stop()
    
    # =========================================================================
    # 4. SELECCIÓN O CREACIÓN DE PROYECTO
    # =========================================================================
    if st.session_state.user_authenticated and st.session_state.current_project_id is None:
        st.markdown("---")
        st.header("📋 Selecciona o crea un proyecto")
        
        user_projects = list_user_projects(st.session_state.supabase, st.session_state.current_user_email)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🆕 Crear nuevo proyecto")
            with st.form("nuevo_proyecto_form"):
                nuevo_nombre = st.text_input("Nombre del nuevo proyecto", placeholder="Mi Huerto de Manzanas 2024")
                crear_proyecto = st.form_submit_button("✨ Crear Nuevo Proyecto", type="primary", use_container_width=True)
                
                if crear_proyecto:
                    if not nuevo_nombre:
                        st.error("❌ El nombre del proyecto es obligatorio")
                    else:
                        # CREAR NUEVO PROYECTO LOCAL
                        st.session_state.current_project_id = str(uuid.uuid4())
                        st.session_state.proyecto_es_local = True
                        st.session_state.current_project_name = nuevo_nombre

                        # LIMPIAR DATOS ANTERIORES COMPLETAMENTE
                        claves_a_limpiar = []
                        for clave in list(st.session_state.keys()):
                            if any(clave.startswith(prefijo) for prefijo in 
                                   ['fertilizantes_data_', 'agroquimicos_data_', 'riego_data_', 
                                    'maquinaria_data_', 'residuos_data_', 'etapas_data_']):
                                claves_a_limpiar.append(clave)
                        
                        for clave in claves_a_limpiar:
                            try:
                                del st.session_state[clave]
                            except:
                                pass

                        # INICIALIZAR ESTRUCTURAS NUEVAS
                        st.session_state.datos_confirmados = {
                            "caracterizacion": {},
                            "fertilizantes": {},
                            "agroquimicos": {},
                            "riego": {},
                            "maquinaria": {},
                            "residuos": {},
                            "etapas": {}
                        }
                        
                        # ESTADO INICIAL
                        st.session_state.guardado_pendiente = True
                        st.session_state.modo_visualizacion = False
                        st.session_state.em_total = 0
                        st.session_state.prod_total = 0
                        st.session_state.n_ciclos = 1
                        st.session_state.ciclos_diferentes = 'No, todos los ciclos son iguales'

                        st.success(f"✅ Proyecto '{nuevo_nombre}' creado en memoria!")
                        st.info("⚠️ **Recuerda:** Usa el botón '💾 Guardar Proyecto' para guardar en la nube.")
                        st.rerun()
        
        with col2:
            st.subheader("📂 Cargar proyecto existente")
            if user_projects:
                st.info(f"Tienes {len(user_projects)} proyecto(s) guardado(s)")
                for project in user_projects:
                    if st.button(f"📂 {project['title']}", 
                                key=f"load_proj_{project['id']}",
                                use_container_width=True):
                        
                        # CARGAR PROYECTO COMPLETO
                        proyecto_completo = load_project_by_id(st.session_state.supabase, project['id'])
                        
                        if proyecto_completo:
                            # ESTABLECER ID Y NOMBRE
                            st.session_state.current_project_id = project['id']
                            st.session_state.current_project_name = project['title']
                            
                            # LIMPIAR DATOS ANTES DE CARGAR
                            claves_a_limpiar = []
                            for clave in list(st.session_state.keys()):
                                if any(clave.startswith(prefijo) for prefijo in 
                                       ['fertilizantes_data_', 'agroquimicos_data_', 'riego_data_', 
                                        'maquinaria_data_', 'residuos_data_', 'etapas_data_']):
                                    claves_a_limpiar.append(clave)
                            
                            for clave in claves_a_limpiar:
                                try:
                                    del st.session_state[clave]
                                except:
                                    pass
                            
                            # CARGAR DATOS COMPLETOS
                            if cargar_datos_desde_proyecto(proyecto_completo):
                                st.success(f"✅ Proyecto '{project['title']}' cargado completamente!")
                            else:
                                st.warning(f"⚠️ Proyecto '{project['title']}' cargado, pero algunos datos no se pudieron recuperar")
                            
                            st.rerun()
                        else:
                            st.error("❌ No se pudo cargar el proyecto")
            else:
                st.info("No tienes proyectos guardados. Crea tu primer proyecto.")
        
        st.stop()
    
    # =========================================================================
    # 5. BARRA LATERAL - CUANDO HAY PROYECTO SELECCIONADO
    # =========================================================================
    
    with st.sidebar:
        # ENCABEZADO INFORMATIVO
        st.markdown(f"### 👋 Hola, {st.session_state.current_user_email}")
        st.markdown(f"**Proyecto:** {st.session_state.current_project_name}")
        
        # INDICADOR DE ESTADO
        if st.session_state.supabase is None:
            st.warning("📱 **Modo local activo**")
            st.caption("Los datos se guardan temporalmente en tu navegador")
        else:
            st.success("☁️ **Conectado a la nube**")
            st.caption("Los datos se guardan bajo demanda")
        
        st.markdown("---")
        
        # SECCIÓN DE PROYECTOS GUARDADOS (CRÍTICA - CORREGIDA)
        if st.session_state.supabase is not None:
            st.markdown("### 📁 Mis Proyectos")
            
            user_projects = list_user_projects(st.session_state.supabase, st.session_state.current_user_email)
            
            if user_projects:
                proyectos_recientes = user_projects[:5]
                
                for project in proyectos_recientes:
                    if project['id'] == st.session_state.current_project_id:
                        st.button(f"📍 {project['title']}", 
                                 key=f"current_{project['id']}",
                                 use_container_width=True,
                                 disabled=True,
                                 help="Proyecto actualmente seleccionado")
                    else:
                        if st.button(f"📂 {project['title']}", 
                                    key=f"load_{project['id']}",
                                    use_container_width=True):
                            
                            # =================================================
                            # CORRECCIÓN CRÍTICA: CARGA COMPLETA DE PROYECTO
                            # =================================================
                            # 1. Establecer ID y nombre
                            st.session_state.current_project_id = project['id']
                            st.session_state.current_project_name = project['title']
                            
                            # 2. Limpiar datos anteriores
                            claves_a_limpiar = []
                            for clave in list(st.session_state.keys()):
                                if any(clave.startswith(prefijo) for prefijo in 
                                       ['fertilizantes_data_', 'agroquimicos_data_', 'riego_data_', 
                                        'maquinaria_data_', 'residuos_data_', 'etapas_data_']):
                                    claves_a_limpiar.append(clave)
                            
                            for clave in claves_a_limpiar:
                                try:
                                    del st.session_state[clave]
                                except:
                                    pass
                            
                            # 3. Cargar proyecto completo desde Supabase
                            proyecto_completo = load_project_by_id(st.session_state.supabase, project['id'])
                            
                            if proyecto_completo:
                                # 4. Cargar todos los datos
                                if cargar_datos_desde_proyecto(proyecto_completo):
                                    # 5. Establecer modo correcto
                                    st.session_state.modo_visualizacion = True
                                    st.session_state.proyecto_es_local = False
                                    st.session_state.guardado_pendiente = False
                                    
                                    st.success(f"✅ Proyecto '{project['title']}' cargado")
                                else:
                                    st.warning(f"⚠️ Algunos datos no se pudieron cargar")
                            else:
                                st.error("❌ No se pudo cargar el proyecto desde la base de datos")
                            
                            st.rerun()
                
                if len(user_projects) > 5:
                    st.caption(f"... y {len(user_projects) - 5} proyectos más")
        
        st.markdown("---")
        
        # =====================================================================
        # SECCIÓN DE GUARDADO
        # =====================================================================
        modo_vis = st.session_state.get('modo_visualizacion', False)
        proyecto_es_local = st.session_state.get('proyecto_es_local', True)
        
        st.sidebar.markdown(f"**Estado:** {'🔒 Guardado' if not proyecto_es_local else '📝 En edición'}")
        
        if proyecto_es_local and not modo_vis:
            # MODO EDICIÓN (proyecto local no guardado)
            st.markdown("### 💾 Guardar Proyecto")
    
            st.warning("""
            **⚠️ ADVERTENCIA CRÍTICA**
    
            Al guardar el proyecto:
            1. **Se guardará en la nube permanentemente**
            2. **NO podrás volver a editar** los datos ingresados
            3. **Solo podrás visualizar** los resultados finales
    
            ¿Estás seguro de que todos los datos son correctos?
            """)
    
            confirmar_guardado = st.checkbox(
                "✅ **CONFIRMO** que los datos son correctos y entiendo que NO podré editarlos después",
                value=False,
                help="Debes marcar esta casilla para poder guardar",
                key="confirmar_guardado_checkbox"
            )
            
            nuevo_nombre = st.text_input(
                "Renombrar proyecto (opcional)",
                value=st.session_state.current_project_name,
                help="Puedes cambiar el nombre antes de guardar",
                key="renombrar_proyecto_input"
            )
    
            if confirmar_guardado:
                col1, col2, col3 = st.columns([1, 2, 1])
                with col2:
                    if st.button(
                        "💾 **GUARDAR PROYECTO DEFINITIVAMENTE**",
                        type="primary",
                        use_container_width=True,
                        key="boton_guardado_definitivo"
                    ):
                        if nuevo_nombre and nuevo_nombre != st.session_state.current_project_name:
                            st.session_state.current_project_name = nuevo_nombre
            
                        if guardar_proyecto_completo():
                            st.success("✅ Proyecto guardado correctamente!")
                            st.session_state.modo_visualizacion = True
                            st.session_state.proyecto_es_local = False
                        else:
                            st.error("❌ No se pudo guardar el proyecto")
            else:
                st.info("🔒 **Marque la casilla de confirmación para habilitar el guardado**")
    
            if st.session_state.get('guardado_pendiente', False):
                st.info("📝 **Hay cambios sin guardar**")
    
            st.markdown("---")
        else:
            # MODO VISUALIZACIÓN (proyecto ya guardado)
            st.success("✅ **PROYECTO GUARDADO**")
            st.info("Este proyecto ya está guardado. Solo puedes visualizar los resultados.")
    
            if st.button("🆕 Crear nueva versión (copia editable)", use_container_width=True):
                st.session_state.current_project_id = f"local_{uuid.uuid4()}"
                st.session_state.current_project_name = f"{st.session_state.current_project_name} - Copia"
                st.session_state.modo_visualizacion = False
                st.session_state.proyecto_es_local = True
                st.session_state.guardado_pendiente = True
                st.rerun()
    
            st.markdown("---")
        
        # BOTONES DE NAVEGACIÓN
        col_nav1, col_nav2 = st.columns(2)
        with col_nav1:
            if st.button("🆕 Nuevo", use_container_width=True, help="Crear nuevo proyecto vacío"):
                st.session_state.current_project_id = None
                st.session_state.current_project_name = None
                st.rerun()
        
        with col_nav2:
            if st.button("🏠 Inicio", use_container_width=True, type="secondary", help="Volver a la página principal"):
                st.session_state.current_project_id = None
                st.session_state.current_project_name = None
                st.rerun()
        
        # BOTÓN DE REINICIO
        if st.session_state.get('em_total', 0) > 0:
            if st.button("🔄 Reiniciar cálculos", use_container_width=True, type="secondary"):
                with st.expander("⚠️ Confirmar reinicio", expanded=True):
                    st.warning("¿Estás seguro de reiniciar todos los cálculos?")
                    if st.button("Sí, reiniciar todo", type="primary"):
                        st.session_state.em_total = 0
                        st.session_state.prod_total = 0
                        st.session_state.emisiones_etapas = {}
                        st.session_state.produccion_etapas = {}
                        st.session_state.emisiones_fuentes = {k: 0 for k in st.session_state.emisiones_fuentes}
                        st.session_state.emisiones_fuente_etapa = {}
                        st.session_state.guardado_pendiente = True
                        st.success("Cálculos reiniciados")
                        st.rerun()
    
    # =========================================================================
    # 6. RETORNO FINAL
    # =========================================================================
    return True

def verificar_guardado():
    """Función temporal para verificar el guardado"""
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 🔍 Diagnóstico")
    
    if st.sidebar.button("Ver datos a guardar"):
        datos_prueba = {
            "user_email": st.session_state.current_user_email,
            "title": st.session_state.current_project_name,
            "fertilizantes": len(st.session_state.get('fertilizantes_data', [])),
            "agroquimicos": len(st.session_state.get('agroquimicos_data', [])),
            "em_total": st.session_state.get('em_total', 0),
            "prod_total": st.session_state.get('prod_total', 0)
        }
        st.sidebar.json(datos_prueba)
    
    if st.sidebar.button("Ver estructura BD"):
        if st.session_state.supabase:
            try:
                response = st.session_state.supabase.table("projects").select("*").limit(1).execute()
                if response.data:
                    st.sidebar.write("Primer proyecto en BD:", list(response.data[0].keys()))
            except Exception as e:
                st.sidebar.error(f"Error: {e}")

# =============================================================================
# BARRA DE NAVEGACIÓN MEJORADA
# =============================================================================

def mostrar_navegacion():
    """Muestra la barra de navegación en el sidebar - VERSIÓN SIMPLIFICADA"""
    # Esta función ahora está integrada en mostrar_sistema_usuarios()
    # Se mantiene por compatibilidad, pero no hace nada
    pass

# Llamar la función de navegación
mostrar_navegacion()

# =============================================================================
# --- DATOS DE ENTRADA Y BIENVENIDA---
# =============================================================================

# --- DATOS DE ENTRADA ---
st.set_page_config(layout="wide")

def mostrar_bienvenida():
    """Página de bienvenida con información general"""
    st.title("AgroPrint - Calculadora de huella de carbono para productos frutícolas")
    
    st.markdown("""
<div style="border: 2px solid #1976d2; border-radius: 12px; padding: 1.5em; background: linear-gradient(135deg, #f0f7ff 0%, #e8f4fd 100%); box-shadow: 0 4px 6px rgba(0,0,0,0.1);">

<div style="text-align: center; margin-bottom: 1.5em;">
<span style="font-size: 2em;">🌱</span>
<h2 style="color: #1976d2; margin: 0.5em 0; font-size: 1.8em;">¡Bienvenido a AgroPrint!</h2>
<p style="font-size: 1.2em; color: #555; margin: 0;">Calculadora de huella de carbono para agricultores</p>
</div>

<div style="background: white; border-radius: 8px; padding: 1.2em; margin: 1.5em 0; border-left: 4px solid #4CAF50;">
<h3 style="color: #2E7D32; margin-top: 0;">🎯 ¿Por qué medir tu huella de carbono?</h3>
<p style="margin-bottom: 0;">Cada vez más compradores y mercados internacionales valoran la <strong>agricultura sostenible</strong>. Conocer y reducir tu huella de carbono te ayuda a:</p>
<ul style="margin: 0.5em 0;">
<li>📈 <strong>Acceder a mejores precios</strong> y mercados premium</li>
<li>🏆 <strong>Obtener certificaciones</strong> de sostenibilidad</li>
<li>💰 <strong>Reducir costos</strong> optimizando el uso de insumos</li>
<li>🌍 <strong>Contribuir</strong> al cuidado del medio ambiente</li>
</ul>
</div>

<div style="background: white; border-radius: 8px; padding: 1.2em; margin: 1.5em 0;">
<h3 style="color: #1976d2; margin-top: 0;">📊 ¿Qué hace esta herramienta?</h3>
<p>AgroPrint calcula la huella de carbono de gases de efecto invernadero de tu cultivo, considerando todo el proceso desde la siembra hasta la cosecha. Analiza:</p>
<div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 10px; margin: 1em 0;">
<div style="background: #E3F2FD; padding: 0.8em; border-radius: 6px;">🌾 <strong>Fertilizantes</strong></div>
<div style="background: #E8F5E8; padding: 0.8em; border-radius: 6px;">🚜 <strong>Labores y Maquinaria</strong></div>
<div style="background: #FFF3E0; padding: 0.8em; border-radius: 6px;">💧 <strong>Riego</strong></div>
<div style="background: #F3E5F5; padding: 0.8em; border-radius: 6px;">🧪 <strong>Agroquímicos</strong></div>
<div style="background: #E0F2F1; padding: 0.8em; border-radius: 6px;">♻️ <strong>Gestión de Residuos</strong></div>
</div>
</div>

<div style="background: #FFF8E1; border-radius: 8px; padding: 1.2em; margin: 1.5em 0; border-left: 4px solid #FFA000;">
<h3 style="color: #F57C00; margin-top: 0;">📋 ¿Qué información necesitas tener lista?</h3>
<p><strong>Antes de comenzar, reúne esta información de tu última temporada:</strong></p>
<div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 15px; margin: 1em 0;">
<div>
<strong>🌾 Fertilizantes:</strong><br>
• Tipos y cantidades de fertilizantes (orgánicos e inorgánicos) utilizados<br>
• Contenido nutricional si lo conoces
</div>
<div>
<strong>🚜 Labores y Maquinaria:</strong><br>
• Qué labores realizas (siembra, cosecha, poda, etc.)<br>
• Consumo de combustible para labores mecanizadas
</div>
<div>
<strong>💧 Riego:</strong><br>
• Tipo de sistema de riego<br>
• Consumo de agua y energía para bombeo
</div>
<div>
<strong>🧪 Agroquímicos:</strong><br>
• Cantidades de pesticidas, fungicidas, herbicidas e insecticidas aplicados<br>
• Tipos de productos utilizados
</div>
<div>
<strong>♻️ Gestión de Residuos:</strong><br>
• Manejo de residuos vegetales<br>
• Métodos: quema, compostaje, incorporación al suelo
</div>
</div>
</div>

<div style="background: white; border-radius: 8px; padding: 1.2em; margin: 1.5em 0;">
<h3 style="color: #1976d2; margin-top: 0;">📊 Tipos de Análisis Disponibles</h3>
<div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(350px, 1fr)); gap: 20px; margin: 1em 0;">
<div style="border: 2px solid #4CAF50; border-radius: 8px; padding: 1em; background: #f8fff8;">
<h4 style="color: #2E7D32; margin-top: 0;">🍎 Análisis Anual</h4>
<p style="margin: 0.5em 0;"><strong>Ideal para:</strong></p>
<ul style="margin: 0.5em 0; padding-left: 1.2em;">
<li>Cultivos anuales (maíz, hortalizas, cereales)</li>
<li>Análisis de un año específico de frutales establecidos</li>
<li>Evaluación rápida de una temporada</li>
</ul>
<p style="margin: 0.5em 0;"><strong>Analiza:</strong> Un ciclo productivo o año específico</p>
<p style="margin: 0.5em 0; color: #2E7D32;"><strong>⏱️ Tiempo:</strong> Más rápido (15-20 min)</p>
</div>
<div style="border: 2px solid #FF9800; border-radius: 8px; padding: 1em; background: #fffbf0;">
<h4 style="color: #F57C00; margin-top: 0;">🌳 Análisis de Ciclo de Vida Completo</h4>
<p style="margin: 0.5em 0;"><strong>Ideal para:</strong></p>
<ul style="margin: 0.5em 0; padding-left: 1.2em;">
<li>Cultivos perennes (frutales, viñedos)</li>
<li>Incluir inversión de establecimiento</li>
<li>Análisis completo desde plantación</li>
</ul>
<p style="margin: 0.5em 0;"><strong>Analiza:</strong> Implantación + crecimiento + producción</p>
<p style="margin: 0.5em 0; color: #F57C00;"><strong>⏱️ Tiempo:</strong> Más completo (25-35 min)</p>
</div>
</div>
<p style="text-align: center; color: #666; font-style: italic; margin: 1em 0;">
💡 Si tienes dudas, el Análisis Anual es más simple y cubre la mayoría de necesidades
</p>
</div>

<div style="background: white; border-radius: 8px; padding: 1.2em; margin: 1.5em 0;">
<h3 style="color: #1976d2; margin-top: 0;">🛤️ ¿Cómo funciona?</h3>
<div style="display: flex; align-items: center; justify-content: space-around; flex-wrap: wrap; margin: 1em 0;">
<div style="text-align: center; margin: 0.5em;">
<div style="background: #1976d2; color: white; border-radius: 50%; width: 40px; height: 40px; display: flex; align-items: center; justify-content: center; margin: 0 auto 0.5em; font-weight: bold;">1</div>
<small>Selecciona tipo<br>de análisis</small>
</div>
<div style="font-size: 1.5em; color: #1976d2;">→</div>
<div style="text-align: center; margin: 0.5em;">
<div style="background: #1976d2; color: white; border-radius: 50%; width: 40px; height: 40px; display: flex; align-items: center; justify-content: center; margin: 0 auto 0.5em; font-weight: bold;">2</div>
<small>Ingresa tus<br>datos</small>
</div>
<div style="font-size: 1.5em; color: #1976d2;">→</div>
<div style="text-align: center; margin: 0.5em;">
<div style="background: #1976d2; color: white; border-radius: 50%; width: 40px; height: 40px; display: flex; align-items: center; justify-content: center; margin: 0 auto 0.5em; font-weight: bold;">3</div>
<small>Obtén tu<br>reporte</small>
</div>
</div>
</div>

<div style="background: #E8F5E8; border-radius: 8px; padding: 1.2em; margin: 1.5em 0; border-left: 4px solid #4CAF50;">
<h3 style="color: #2E7D32; margin-top: 0;">🎁 ¿Qué obtienes al final?</h3>
<ul style="margin: 0.5em 0;">
<li>📊 <strong>Reporte completo</strong> de tu huella de carbono</li>
<li>📈 <strong>Gráficos visuales</strong> fáciles de entender</li>
<li>📄 <strong>Documentos PDF y Excel</strong> para presentar a compradores</li>
<li>💡 <strong>Identificación</strong> de las principales fuentes de huella de carbono</li>
<li>🎯 <strong>Oportunidades</strong> para reducir costos e impacto ambiental</li>
</ul>
</div>

<div style="text-align: center; margin-top: 2em; padding: 1em; background: #f8f9fa; border-radius: 8px;">
<p style="margin: 0; color: #666; font-size: 0.9em;">
<strong>Metodología científica:</strong> Basado en estándares internacionales IPCC 2006 y PAS 2050<br>
<strong>Tiempo estimado:</strong> 15-30 minutos (dependiendo del tipo de cultivo)
</p>
</div>

</div>
""", unsafe_allow_html=True)

    st.markdown("---")

# DIAGNÓSTICO TEMPORAL
if st.session_state.get('current_project_id'):
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 🐛 Debug Info")
    
    # Contar datos
    count_data = {}
    for key in st.session_state.keys():
        if 'data' in key.lower():
            val = st.session_state[key]
            if isinstance(val, list):
                count_data[key] = len(val)
            elif val:
                count_data[key] = 1
    
    st.sidebar.write("Datos encontrados:", count_data)
    
    if st.sidebar.button("Ver todos los datos"):
        for key in sorted(st.session_state.keys()):
            if 'data' in key.lower() or 'em_' in key or 'prod_' in key:
                st.sidebar.write(f"{key}: {type(st.session_state[key])}")

def formulario_solo_lectura(label, valor_actual, key=None):
    """
    Muestra un campo de formulario en modo solo lectura.
    Para formularios que deben ser visibles pero no editables.
    """
    # Usar st.text_input con disabled=True para modo solo lectura
    return st.text_input(
        label,
        value=str(valor_actual) if valor_actual else "",
        disabled=True,
        key=key
    )

def numero_solo_lectura(label, valor_actual, key=None):
    """
    Muestra un número en modo solo lectura.
    """
    return st.number_input(
        label,
        value=float(valor_actual) if valor_actual else 0.0,
        disabled=True,
        key=key
    )

def selectbox_solo_lectura(label, opciones, valor_actual, key=None):
    """
    Muestra un selectbox en modo solo lectura.
    """
    # Encontrar el índice del valor actual
    if valor_actual in opciones:
        indice = opciones.index(valor_actual)
    else:
        indice = 0
    
    return st.selectbox(
        label,
        options=opciones,
        index=indice,
        disabled=True,
        key=key
    )

# =============================================================================
# FLUJO PRINCIPAL DE LA APLICACIÓN
# =============================================================================

# 1. Mostrar consentimiento de privacidad (OBLIGATORIO)
mostrar_consentimiento_privacidad()

# 2. Mostrar sistema de usuarios OBLIGATORIO
if not mostrar_sistema_usuarios():
    st.stop()  # Detener la app si no hay autenticación

# 3. Mostrar bienvenida
mostrar_bienvenida()

# 4. INICIALIZAR datos temporales si no existen
if 'datos_temporales' not in st.session_state:
    st.session_state.datos_temporales = {
        "caracterizacion": {},
        "entradas": {},
        "calculos": {}
    }

# =============================================================================
# INICIALIZACIÓN DE SESSION_STATE - REEMPLAZA VARIABLES GLOBALES
# =============================================================================

# Inicializar todas las variables en session_state si no existen
if 'emisiones_etapas' not in st.session_state:
    st.session_state.emisiones_etapas = {}

if 'produccion_etapas' not in st.session_state:
    st.session_state.produccion_etapas = {}

if 'emisiones_fuentes' not in st.session_state:
    st.session_state.emisiones_fuentes = {
        "Fertilizantes": 0,
        "Agroquímicos": 0,
        "Riego": 0,
        "Maquinaria": 0,
        "Transporte": 0,
        "Residuos": 0,
        "Fin de vida": 0
    }

if 'emisiones_fuente_etapa' not in st.session_state:
    st.session_state.emisiones_fuente_etapa = {}

if 'modo_anterior' not in st.session_state:
    st.session_state.modo_anterior = ""

# Crear variables locales como referencias a session_state para facilitar el uso
emisiones_etapas = st.session_state.emisiones_etapas
produccion_etapas = st.session_state.produccion_etapas
emisiones_fuentes = st.session_state.emisiones_fuentes
emisiones_fuente_etapa = st.session_state.emisiones_fuente_etapa

# =============================================================================
# Sección 1: Caracterización General
# =============================================================================
st.header("1. Caracterización General")

# Verificar modo visualización
if st.session_state.get('modo_visualizacion', False):
    # MODO VISUALIZACIÓN: Solo lectura
    st.info("🔒 **MODO VISUALIZACIÓN** - Los datos no se pueden modificar")
    
    cultivo = formulario_solo_lectura("Nombre del cultivo o fruta", st.session_state.get('cultivo', ''), "cultivo_visual")
    anual = selectbox_solo_lectura("¿Es un cultivo anual o perenne?", ["Anual", "Perenne"], 
                                   st.session_state.get('modo_anterior', 'Anual'), "anual_visual")
    morfologia = selectbox_solo_lectura("Morfología", ["Árbol", "Arbusto", "Hierba", "Otro"], 
                                       st.session_state.get('morfologia', ''), "morfologia_visual")
    ubicacion = formulario_solo_lectura("Ubicación geográfica del cultivo (región, país)", 
                                       st.session_state.get('ubicacion', ''), "ubicacion_visual")
    tipo_suelo = selectbox_solo_lectura("Tipo de suelo", 
                                       ["Franco", "Arenoso", "Arcilloso", "Franco-arenoso", "Franco-arcilloso", "Otro"],
                                       st.session_state.get('tipo_suelo', ''), "tipo_suelo_visual")
    clima = selectbox_solo_lectura("Zona agroclimática o clima predominante",
                                  ["Mediterráneo", "Tropical", "Templado", "Desértico", "Húmedo", "Otro"],
                                  st.session_state.get('clima', ''), "clima_visual")
    extra = formulario_solo_lectura("Información complementaria (opcional)", 
                                   st.session_state.get('extra', ''), "extra_visual")
    
else:
    # MODO EDICIÓN: Campos editables normales
    cultivo = st.text_input("Nombre del cultivo o fruta", key="cultivo_edit")
    anual = st.radio("¿Es un cultivo anual o perenne?", ["Anual", "Perenne"], key="anual_edit")
    morfologia = st.selectbox("Morfología", ["Árbol", "Arbusto", "Hierba", "Otro"], key="morfologia_edit")
    ubicacion = st.text_input("Ubicación geográfica del cultivo (región, país)", key="ubicacion_edit")
    tipo_suelo = st.selectbox("Tipo de suelo", [
        "Franco", "Arenoso", "Arcilloso", "Franco-arenoso", "Franco-arcilloso", "Otro"
    ], key="tipo_suelo_edit")
    clima = st.selectbox("Zona agroclimática o clima predominante", [
        "Mediterráneo", "Tropical", "Templado", "Desértico", "Húmedo", "Otro"
    ], key="clima_edit")
    extra = st.text_area("Información complementaria (opcional)", key="extra_edit")
    
    # Guardar en session_state
    st.session_state.cultivo = cultivo
    st.session_state.modo_anterior = anual
    st.session_state.morfologia = morfologia
    st.session_state.ubicacion = ubicacion
    st.session_state.tipo_suelo = tipo_suelo
    st.session_state.clima = clima
    st.session_state.extra = extra

# --- Inicialización de resultados según modo anual/perenne ---
if 'modo_anterior' not in st.session_state or st.session_state.modo_anterior != anual:
    # Limpiar todas las estructuras de datos usando session_state
    st.session_state.emisiones_etapas.clear()
    st.session_state.produccion_etapas.clear()
    
    # Reiniciar emisiones_fuentes
    for k in st.session_state.emisiones_fuentes:
        st.session_state.emisiones_fuentes[k] = 0
    
    st.session_state.emisiones_anuales = []
    st.session_state.emisiones_ciclos = []
    st.session_state.modo_anterior = anual
    st.session_state.emisiones_fuente_etapa = {}
    
    # Actualizar las referencias locales
    emisiones_etapas.clear()
    produccion_etapas.clear()
    for k in emisiones_fuentes:
        emisiones_fuentes[k] = 0
    emisiones_fuente_etapa.clear()

# Asegurar que la referencia local esté actualizada
emisiones_fuente_etapa = st.session_state.emisiones_fuente_etapa

# =============================================================================
# Funciones de ingreso y cálculo
# =============================================================================

def ingresar_fertilizantes(etapa, unidad_cantidad="ciclo"):
    """
    Ingresa o muestra fertilizantes.
    VERSIÓN CON GUARDADO CONFIABLE.
    """
    # =====================================================================
    # INICIALIZACIÓN OBLIGATORIA
    # =====================================================================
    # Asegurar que datos_confirmados exista
    if 'datos_confirmados' not in st.session_state:
        st.session_state.datos_confirmados = {}
    
    if 'fertilizantes' not in st.session_state.datos_confirmados:
        st.session_state.datos_confirmados['fertilizantes'] = {}
    
    # =====================================================================
    # VERIFICAR MODO VISUALIZACIÓN
    # =====================================================================
    modo_vis = st.session_state.get('modo_visualizacion', False)
    
    if modo_vis:
        # MODO VISUALIZACIÓN: Mostrar resumen
        st.markdown(f"##### Fertilizantes - {etapa}")
        st.info("🔒 **MODO VISUALIZACIÓN** - Los datos no se pueden modificar")
        
        # Obtener datos confirmados
        fertilizantes_confirmados = obtener_datos_confirmados('fertilizantes', etapa)
        
        if not fertilizantes_confirmados:
            st.info("📝 No hay fertilizantes registrados para esta etapa")
            
            # GUARDAR LISTA VACÍA (IMPORTANTE)
            st.session_state.datos_confirmados['fertilizantes'][etapa] = []
            return {"fertilizantes": []}
        
        # Mostrar tabla resumen
        datos_tabla = []
        for fert in fertilizantes_confirmados:
            if fert.get("es_organico", False):
                datos_tabla.append({
                    "Tipo": "Orgánico",
                    "Nombre": fert.get("tipo", "Sin nombre"),
                    "Cantidad (kg/ha)": format_num(fert.get("cantidad", 0)),
                    "N (%)": format_num(fert.get("N", 0), 1),
                    "Fracción seca": format_num(fert.get("fraccion_seca", 0) * 100 if fert.get("fraccion_seca") else 0, 1) + "%"
                })
            else:
                datos_tabla.append({
                    "Tipo": "Inorgánico",
                    "Nombre": fert.get("tipo", ""),
                    "Origen": fert.get("origen", "No especificado"),
                    "Cantidad (kg/ha)": format_num(fert.get("cantidad", 0)),
                    "N (%)": format_num(fert.get("N", 0), 1)
                })
        
        if datos_tabla:
            df = pd.DataFrame(datos_tabla)
            st.dataframe(df, use_container_width=True, hide_index=True)
        
        # Los datos ya están guardados, solo retornarlos
        return {"fertilizantes": fertilizantes_confirmados}
    
    # =========================================================================
    # MODO EDICIÓN: Mostrar formularios completos
    # =========================================================================
    st.markdown("##### Fertilizantes")
    tipos_inorg = list(factores_fertilizantes.keys())
    tipos_org = list(FACTORES_ORGANICOS.keys())

    sufijo = "ciclo" if unidad_cantidad == "ciclo" else "año"

    # Obtener datos guardados previamente si existen
    clave_guardado = f"fertilizantes_data_{etapa}"
    datos_previos = st.session_state.get(clave_guardado, [])

    n_fert = st.number_input(
        f"Ingrese la cantidad de fertilizantes que utiliza (orgánicos e inorgánicos)",
        min_value=0, step=1, format="%.6g", key=f"num_fert_total_{etapa}"
    )
    fertilizantes = []

    for i in range(int(n_fert)):
        with st.expander(f"Fertilizante #{i+1}"):
            modo = st.radio(
                "¿Qué tipo de fertilizante desea ingresar?",
                ["Inorgánico (sintético)", "Orgánico (estiércol, compost, guano, etc.)"],
                key=f"modo_fert_{etapa}_{i}"
            )
            if modo == "Inorgánico (sintético)":
                tipo = st.selectbox("Tipo de fertilizante inorgánico", tipos_inorg, key=f"tipo_inorg_{etapa}_{i}")
                if tipo == "Otros":
                    nombre_otro = st.text_input(
                        "Ingrese un nombre representativo para este fertilizante 'Otro' (ej: Nitrato especial, Compost local, etc.)",
                        key=f"nombre_otro_{etapa}_{i}"
                    )
                    modo_otros = st.radio(
                        "¿Cómo desea ingresar el fertilizante 'Otro'?",
                        ["porcentaje", "nutriente"],
                        key=f"modo_otros_{etapa}_{i}"
                    )
                    if modo_otros == "porcentaje":
                        cantidad = st.number_input(f"Cantidad aplicada (kg/ha·{sufijo})", min_value=0.0, format="%.10g", key=f"cant_otros_{etapa}_{i}")
                        n = st.number_input("Contenido de N (%)", min_value=0.0, max_value=100.0, format="%.10g", key=f"N_otros_{etapa}_{i}")
                        p = st.number_input("Contenido de P₂O₅ (%)", min_value=0.0, max_value=100.0, format="%.10g", key=f"P_otros_{etapa}_{i}")
                        k = st.number_input("Contenido de K₂O (%)", min_value=0.0, max_value=100.0, format="%.10g", key=f"K_otros_{etapa}_{i}")
                        usar_fe_personalizado = st.checkbox("¿Desea ingresar un factor de emisión personalizado para la producción de este fertilizante?", key=f"usar_fe_otros_{etapa}_{i}")
                        if usar_fe_personalizado:
                            fe_personalizado = st.number_input("Factor de emisión personalizado (kg CO₂e/kg producto)", min_value=0.0, step=0.000001, format="%.6g", key=f"fe_personalizado_otros_{etapa}_{i}")
                        else:
                            fe_personalizado = None
                        fertilizantes.append({
                            "tipo": nombre_otro if nombre_otro else "Otros",
                            "cantidad": cantidad,
                            "N": n,
                            "P": p,
                            "K": k,
                            "modo_otros": "porcentaje",
                            "es_organico": False,
                            "fe_personalizado": fe_personalizado
                        })
                    else:  # modo_otros == "nutriente"
                        nutriente = st.selectbox("Nutriente aplicado", ["N", "P", "K"], key=f"nutriente_otros_{etapa}_{i}")
                        cantidad = st.number_input(f"Cantidad de {nutriente} aplicada (kg {nutriente}/ha·{sufijo})", min_value=0.0, format="%.6g", key=f"cant_nutriente_otros_{etapa}_{i}")
                        usar_fe_personalizado = st.checkbox("¿Desea ingresar un factor de emisión personalizado para la producción de este fertilizante?", key=f"usar_fe_otros_nutriente_{etapa}_{i}")
                        if usar_fe_personalizado:
                            fe_personalizado = st.number_input("Factor de emisión personalizado (kg CO₂e/kg producto)", min_value=0.0, step=0.000001, format="%.6g", key=f"fe_personalizado_otros_nutriente_{etapa}_{i}")
                        else:
                            fe_personalizado = None
                        fertilizantes.append({
                            "tipo": nombre_otro if nombre_otro else "Otros",
                            "cantidad": cantidad,
                            "nutriente": nutriente,
                            "modo_otros": "nutriente",
                            "es_organico": False,
                            "fe_personalizado": fe_personalizado
                        })
                else:
                    variantes = factores_fertilizantes[tipo]
                    origenes = [v["origen"] for v in variantes]
                    origen = st.selectbox("Origen del fertilizante", origenes, key=f"origen_inorg_{etapa}_{i}")
                    variante = next((v for v in variantes if v["origen"] == origen), variantes[0])
                    cantidad = st.number_input(f"Cantidad aplicada (kg/ha·{sufijo})", min_value=0.0, format="%.6g", key=f"cant_inorg_{etapa}_{i}")
                    # CORRECCIÓN: fuerza el tipo de value a float para evitar errores de Streamlit
                    n = st.number_input(
                        "Contenido de N (%)",
                        min_value=0.0,
                        max_value=100.0,
                        value=float(variante["N_porcentaje"])*100,
                        format="%.10g",
                        key=f"N_inorg_{etapa}_{i}"
                    )
                    usar_fe_personalizado = st.checkbox("¿Desea ingresar un factor de emisión personalizado para la producción de este fertilizante?", key=f"usar_fe_inorg_{etapa}_{i}")
                    if usar_fe_personalizado:
                        fe_personalizado = st.number_input("Factor de emisión personalizado (kg CO₂e/kg producto)", min_value=0.0, step=0.000001, format="%.6g", key=f"fe_personalizado_inorg_{etapa}_{i}")
                    else:
                        fe_personalizado = None
                    fertilizantes.append({
                        "tipo": tipo,
                        "origen": origen,
                        "cantidad": cantidad,
                        "N": n,
                        "es_organico": False,
                        "fe_personalizado": fe_personalizado
                    })
            else:
                tipo = st.selectbox("Tipo de fertilizante orgánico", tipos_org, key=f"tipo_org_{etapa}_{i}")
                valores = FACTORES_ORGANICOS[tipo]
                if tipo == "Otros":
                    nombre_otro_org = st.text_input(
                        "Ingrese un nombre representativo para este fertilizante orgánico 'Otro' (ej: Compost especial, Guano local, etc.)",
                        key=f"nombre_otro_org_{etapa}_{i}"
                    )
                else:
                    nombre_otro_org = None
                st.warning(
                    f"Valores sugeridos para '{tipo}': "
                    f"N = {valores['N']}%, "
                    f"P₂O₅ = {valores['P2O5']}%, "
                    f"K₂O = {valores['K2O']}%, "
                    f"Fracción seca = {format_fraction_as_percent(valores['fraccion_seca'], decimales=1)}"
                )
                cantidad = st.number_input(f"Cantidad aplicada (kg/ha·{sufijo}, base húmeda)", min_value=0.0, format="%.6g", key=f"cant_org_{etapa}_{i}")
                n = st.number_input("Contenido de N (%)", min_value=0.0, max_value=100.0, value=float(valores['N']), format="%.6g", key=f"N_org_{etapa}_{i}")
                p = st.number_input("Contenido de P₂O₅ (%)", min_value=0.0, max_value=100.0, value=float(valores['P2O5']), format="%.6g", key=f"P_org_{etapa}_{i}")
                k = st.number_input("Contenido de K₂O (%)", min_value=0.0, max_value=100.0, value=float(valores['K2O']), format="%.6g", key=f"K_org_{etapa}_{i}")
                fraccion_seca_pct = st.number_input("Fracción seca del fertilizante (%)", min_value=0.0, max_value=100.0, value=float(valores['fraccion_seca'])*100, format="%.6g", key=f"fraccion_seca_org_{etapa}_{i}")
                st.info("Para el cálculo de huella de carbono, el contenido de N es el principal responsable de la huella de carbono de N₂O. Si no dispone de los otros nutrientes, puede dejarlos en cero.")
                fertilizantes.append({
                    "tipo": nombre_otro_org if (tipo == "Otros" and nombre_otro_org) else tipo,
                    "cantidad": cantidad,
                    "N": n,
                    "P": p,
                    "K": k,
                    "fraccion_seca": fraccion_seca_pct / 100,  # Convierte a fracción
                    "es_organico": True
                })

    # =====================================================================
    # GUARDADO OBLIGATORIO (SIEMPRE SE EJECUTA)
    # =====================================================================
    # Guardar SIEMPRE, incluso si la lista está vacía
    st.session_state.datos_confirmados['fertilizantes'][etapa] = fertilizantes
    
    # Marcar cambios pendientes
    st.session_state.guardado_pendiente = True
    st.session_state.ultimo_cambio = datetime.now().isoformat()
    
    # Mostrar confirmación
    if fertilizantes:
        st.success(f"✅ {len(fertilizantes)} fertilizante(s) ingresado(s)")
    else:
        st.info("📝 No se ingresaron fertilizantes")
    
    return {"fertilizantes": fertilizantes}

def obtener_datos_confirmados(tipo, etapa):
    """
    Obtiene datos confirmados de session_state.
    VERSIÓN SIMPLIFICADA Y CONSISTENTE.
    """
    try:
        # PRIMERO: Buscar en datos_confirmados (sistema unificado)
        if 'datos_confirmados' in st.session_state:
            datos_confirmados = st.session_state.datos_confirmados
            
            # Verificar que exista el tipo y la etapa
            if tipo in datos_confirmados and etapa in datos_confirmados[tipo]:
                datos = datos_confirmados[tipo][etapa]
                
                # Para fertilizantes, agroquímicos, maquinaria: retornar lista
                if tipo in ['fertilizantes', 'agroquimicos', 'maquinaria']:
                    return datos if isinstance(datos, list) else []
                
                # Para riego, residuos: retornar diccionario completo
                elif tipo in ['riego', 'residuos']:
                    return datos if isinstance(datos, dict) else {}
                
                # Para otros tipos: retornar tal cual
                else:
                    return datos
        
        # SEGUNDO: Para compatibilidad (código antiguo)
        clave_antigua = f"{tipo}_data_{etapa}"
        if clave_antigua in st.session_state:
            datos = st.session_state[clave_antigua]
            
            # Convertir a estructura esperada si es necesario
            if tipo in ['fertilizantes', 'agroquimicos', 'maquinaria'] and not isinstance(datos, list):
                return []
            elif tipo in ['riego', 'residuos'] and not isinstance(datos, dict):
                return {}
            else:
                return datos
        
        # TERCERO: Si no hay nada, retornar estructura vacía
        if tipo in ['fertilizantes', 'agroquimicos', 'maquinaria']:
            return []
        elif tipo in ['riego', 'residuos']:
            return {}
        else:
            return None
            
    except Exception as e:
        # st.error(f"Error obteniendo datos confirmados: {e}")
        # Retornar estructura vacía en caso de error
        if tipo in ['fertilizantes', 'agroquimicos', 'maquinaria']:
            return []
        elif tipo in ['riego', 'residuos']:
            return {}
        else:
            return None

def mostrar_resumen_datos_confirmados():
    """Muestra un resumen de los datos confirmados hasta el momento"""
    
    # =========================================================================
    # VERIFICAR MODO VISUALIZACIÓN
    # =========================================================================
    if st.session_state.get('modo_visualizacion', False):
        # MODO VISUALIZACIÓN: Mostrar título diferente
        st.markdown("### 📊 Datos del Proyecto (Guardado)")
        
        # Mostrar mensaje importante
        st.success("✅ **PROYECTO GUARDADO** - Solo modo visualización")
        st.info("Este proyecto ya está guardado. Para modificar datos, crea una nueva versión desde el sidebar.")
    else:
        # MODO EDICIÓN: Mostrar título normal
        st.markdown("### 📋 Resumen de datos confirmados")
    
    # EL RESTO DEL CÓDIGO ORIGINAL SE MANTIENE PERO CON MEJORAS:
    if 'datos_confirmados' not in st.session_state:
        if st.session_state.get('modo_visualizacion', False):
            st.info("📊 No hay datos almacenados para este proyecto")
        else:
            st.info("📝 No hay datos confirmados todavía")
        return
    
    datos = st.session_state.datos_confirmados
    has_data = False
    
    # Crear una tabla más organizada
    tabla_datos = []
    
    for tipo, etapas in datos.items():
        if etapas and isinstance(etapas, dict):  # Si hay datos y es un diccionario
            for etapa, items in etapas.items():
                # Verificar si hay items REALES (no estructuras vacías)
                tiene_items_reales = False
            
                if isinstance(items, list):
                    # Para listas: verificar que no esté vacía
                    if items:
                        tiene_items_reales = True
                        count = len(items)
                elif isinstance(items, dict):
                    # Para diccionarios: verificar que no sea estructura vacía
                    # Para riego: verificar si hay actividades o emisiones
                    if tipo == 'riego' and items:
                        em_agua = items.get('em_agua_total', 0)
                        em_energia = items.get('em_energia_total', 0)
                        actividades = items.get('energia_actividades', [])
                        tiene_items_reales = (em_agua > 0 or em_energia > 0 or 
                                            any(a.get('agua_total_m3', 0) > 0 or 
                                                a.get('consumo_energia', 0) > 0 
                                                for a in actividades))
                
                    # Para residuos: verificar si hay biomasa
                    elif tipo == 'residuos' and items:
                        total_biomasa = items.get('total_biomasa', 0)
                        tiene_items_reales = total_biomasa > 0
                
                    # Para otros tipos de diccionarios
                    elif items:
                        tiene_items_reales = True
                
                    count = 1 if tiene_items_reales else 0
                else:
                    # Para otros tipos
                    tiene_items_reales = bool(items)
                    count = 1 if items else 0
            
                if tiene_items_reales:
                    has_data = True
                    tipo_formateado = tipo.capitalize().replace("_", " ")
                    st.write(f"• **{tipo_formateado}** en *{etapa}*: {count} registro(s)")
    
    # Para modo visualización, mostrar tabla bonita
    if st.session_state.get('modo_visualizacion', False) and tabla_datos:
        df = pd.DataFrame(tabla_datos)
        st.dataframe(df, use_container_width=True, hide_index=True)
    
    if not has_data:
        if st.session_state.get('modo_visualizacion', False):
            st.info("📊 No hay datos almacenados para este proyecto")
        else:
            st.info("No hay datos confirmados. Ingresa y confirma datos en cada sección.")
    
    # Mostrar estado de guardado (solo en modo edición)
    if not st.session_state.get('modo_visualizacion', False):
        if st.session_state.get('guardado_pendiente', False):
            st.warning("⚠️ Hay cambios pendientes de guardar en la nube")
        
        if st.session_state.get('ultimo_guardado'):
            st.caption(f"Último guardado: {st.session_state.ultimo_guardado}")
    else:
        # En modo visualización, mostrar fecha del proyecto si existe
        if st.session_state.get('ultimo_guardado'):
            st.caption(f"📅 Proyecto guardado el: {st.session_state.ultimo_guardado}")

def calcular_emisiones_n2o_fertilizantes_desglosado(fertilizantes, duracion):
    total_n_aplicado = 0
    total_n_volatilizado = 0
    total_n_lixiviado = 0

    for fert in fertilizantes:
        if fert.get("es_organico", False):
            cantidad = fert.get("cantidad", 0)  # kg/ha
            tipo = fert.get("tipo", "Otros")
            valores = FACTORES_ORGANICOS.get(tipo, FACTORES_ORGANICOS["Otros"])
            fraccion_seca = fert.get("fraccion_seca", valores["fraccion_seca"])
            n = fert.get("N", valores["N"]) / 100  # %
            n_aplicado = cantidad * fraccion_seca * n
            frac_vol = FRAC_VOLATILIZACION_ORG
            frac_lix = FRAC_LIXIVIACION
        elif fert["tipo"] == "Otros":
            if fert.get("modo_otros") == "porcentaje":
                cantidad = fert.get("cantidad", 0)
                n = fert.get("N", 0) / 100
                n_aplicado = cantidad * n
                frac_vol = FRAC_VOLATILIZACION_INORG
                frac_lix = FRAC_LIXIVIACION
            elif fert.get("modo_otros") == "nutriente":
                nutriente = fert.get("nutriente")
                cantidad = fert.get("cantidad", 0)
                n_aplicado = cantidad if nutriente == "N" else 0
                frac_vol = FRAC_VOLATILIZACION_INORG
                frac_lix = FRAC_LIXIVIACION
            else:
                n_aplicado = 0
                frac_vol = 0
                frac_lix = 0
        else:
            tipo = fert["tipo"]
            origen = fert.get("origen", None)
            variantes = factores_fertilizantes.get(tipo, [])
            if isinstance(variantes, list):
                variante = next((v for v in variantes if v["origen"] == origen), variantes[0] if variantes else None)
            else:
                variante = None
            if variante:
                cantidad = fert.get("cantidad", 0)
                n_porcentaje = variante.get("N_porcentaje", 0)
                n_aplicado = cantidad * n_porcentaje
                frac_vol = variante.get("Frac_volatilizacion", FRAC_VOLATILIZACION_INORG)
                frac_lix = variante.get("Frac_lixiviacion", FRAC_LIXIVIACION)
            else:
                n_aplicado = 0
                frac_vol = FRAC_VOLATILIZACION_INORG
                frac_lix = FRAC_LIXIVIACION

        n_volatilizado = n_aplicado * frac_vol
        n_lixiviado = n_aplicado * frac_lix

        total_n_aplicado += n_aplicado * duracion
        total_n_volatilizado += n_volatilizado * duracion
        total_n_lixiviado += n_lixiviado * duracion

    n2o_n_directo = total_n_aplicado * EF1
    n2o_n_ind_vol = total_n_volatilizado * EF4
    n2o_n_ind_lix = total_n_lixiviado * EF5

    n2o_n_indirecto = n2o_n_ind_vol + n2o_n_ind_lix

    n2o_directo = n2o_n_directo * (44/28)
    n2o_indirecto = n2o_n_indirecto * (44/28)
    n2o_total = n2o_directo + n2o_indirecto

    n2o_directo_co2e = n2o_directo * GWP["N2O"]
    n2o_indirecto_co2e = n2o_indirecto * GWP["N2O"]
    emision_n2o_co2e_total = n2o_total * GWP["N2O"]

    return emision_n2o_co2e_total, total_n_aplicado, n2o_directo_co2e, n2o_indirecto_co2e

def calcular_emisiones_fertilizantes(fert_data, duracion):
    fertilizantes = fert_data.get("fertilizantes", [])

    emision_produccion = 0
    emision_co2_urea = 0  # ✅ INICIALIZAR LA VARIABLE QUE FALTABA
    n_aplicado_inorg = 0
    n_aplicado_org = 0
    volatilizacion_inorg = 0
    lixiviacion_inorg = 0
    volatilizacion_org = 0
    lixiviacion_org = 0

    desglose = []

    for fert in fertilizantes:
        em_prod = 0
        em_co2_urea_individual = 0  # CO2 de urea para este fertilizante específico
        em_n2o_dir = 0
        em_n2o_ind = 0
        em_n2o_ind_vol = 0
        em_n2o_ind_lix = 0

        tipo_fertilizante = "Orgánico" if fert.get("es_organico", False) else "Inorgánico"

        # --- Cálculo de N aplicado y fracciones ---
        n_aplicado = 0
        frac_vol = 0
        frac_lix = 0

        if fert.get("es_organico", False):
            cantidad = fert.get("cantidad", 0)
            tipo = fert.get("tipo", "Otros")
            valores = FACTORES_ORGANICOS.get(tipo, FACTORES_ORGANICOS["Otros"])
            fraccion_seca = fert.get("fraccion_seca", valores["fraccion_seca"])
            n = fert.get("N", valores["N"]) / 100
            n_aplicado = cantidad * fraccion_seca * n
            n_aplicado_org += n_aplicado
            frac_vol = FRAC_VOLATILIZACION_ORG
            frac_lix = FRAC_LIXIVIACION
            volatilizacion_org += n_aplicado * frac_vol
            lixiviacion_org += n_aplicado * frac_lix

        elif fert.get("tipo", "") == "Otros" or fert.get("modo_otros") in ["porcentaje", "nutriente"]:
            nombre_otro = fert.get("tipo", "Otros")
            if fert.get("modo_otros") == "porcentaje":
                cantidad = fert.get("cantidad", 0)
                n = fert.get("N", 0) / 100
                n_aplicado = cantidad * n
            elif fert.get("modo_otros") == "nutriente":
                nutriente = fert.get("nutriente", "").strip().upper()
                cantidad = fert.get("cantidad", 0)
                n_aplicado = cantidad if nutriente == "N" else 0
            else:
                n_aplicado = 0

            if n_aplicado > 0:
                n_aplicado_inorg += n_aplicado
                frac_vol = FRAC_VOLATILIZACION_INORG
                frac_lix = FRAC_LIXIVIACION
                volatilizacion_inorg += n_aplicado * frac_vol
                lixiviacion_inorg += n_aplicado * frac_lix
            else:
                frac_vol = 0
                frac_lix = 0

            # FE personalizado para "Otros"
            fe = fert.get("fe_personalizado", None)
            if fe is not None and fe > 0:
                em_prod = cantidad * fe * duracion
            else:
                em_prod = 0

        else:
            tipo = fert.get("tipo", "")
            origen = fert.get("origen", None)
            variantes = factores_fertilizantes.get(tipo, [])
            if isinstance(variantes, list):
                variante = next((v for v in variantes if v["origen"] == origen), variantes[0] if variantes else None)
            else:
                variante = None
            if variante:
                cantidad = fert.get("cantidad", 0)
                n_porcentaje = variante.get("N_porcentaje", 0)
                n_aplicado = cantidad * n_porcentaje
                n_aplicado_inorg += n_aplicado
                frac_vol = variante.get("Frac_volatilizacion", FRAC_VOLATILIZACION_INORG)
                frac_lix = variante.get("Frac_lixiviacion", FRAC_LIXIVIACION)
                volatilizacion_inorg += n_aplicado * frac_vol
                lixiviacion_inorg += n_aplicado * frac_lix
                
                # --- CÁLCULO DE EMISIONES CO2 POR HIDRÓLISIS DE UREA (IPCC 2006 Vol.4 Cap.2) ---
                if tipo == "Urea" or "Urea" in tipo:
                    em_co2_urea_individual = cantidad * EF_CO2_UREA * duracion
                    emision_co2_urea += em_co2_urea_individual
                
                # FE personalizado
                fe = fert.get("fe_personalizado", None)
                if fe is not None and fe > 0:
                    em_prod = cantidad * fe * duracion
                else:
                    fe_default = variante.get("FE_produccion_producto", 0)
                    em_prod = cantidad * fe_default * duracion if fe_default else 0
            else:
                cantidad = 0
                n_aplicado = 0
                frac_vol = FRAC_VOLATILIZACION_INORG
                frac_lix = FRAC_LIXIVIACION

        # --- Emisiones N2O directas e indirectas por fertilizante individual ---
        n_volatilizado = n_aplicado * frac_vol
        n_lixiviado = n_aplicado * frac_lix

        n2o_n_directo = n_aplicado * EF1
        n2o_n_ind_vol = n_volatilizado * EF4
        n2o_n_ind_lix = n_lixiviado * EF5
        n2o_n_indirecto = n2o_n_ind_vol + n2o_n_ind_lix
        n2o_directo = n2o_n_directo * (44/28)
        n2o_ind_vol = n2o_n_ind_vol * (44/28)
        n2o_ind_lix = n2o_n_ind_lix * (44/28)
        n2o_indirecto = n2o_ind_vol + n2o_ind_lix
        em_n2o_dir = n2o_directo * GWP["N2O"]
        em_n2o_ind_vol = n2o_ind_vol * GWP["N2O"]
        em_n2o_ind_lix = n2o_ind_lix * GWP["N2O"]
        em_n2o_ind = em_n2o_ind_vol + em_n2o_ind_lix

        desglose.append({
            "Tipo fertilizante": tipo_fertilizante,
            "tipo": fert.get("tipo", fert.get("nutriente", "")),
            "origen": fert.get("origen", ""),
            "cantidad": fert.get("cantidad", 0),
            "emision_produccion": em_prod,
            "emision_co2_urea": em_co2_urea_individual,  # Nueva columna en desglose
            "emision_n2o_directa": em_n2o_dir,
            "emision_n2o_indirecta": em_n2o_ind,
            "emision_n2o_ind_volatilizacion": em_n2o_ind_vol,
            "emision_n2o_ind_lixiviacion": em_n2o_ind_lix,
            "total": em_prod + em_co2_urea_individual + em_n2o_dir + em_n2o_ind  # Incluye CO2 urea en total
        })

        emision_produccion += em_prod

    # --- EMISIONES N2O DIRECTAS E INDIRECTAS (totales) ---
    total_n_aplicado_inorg = n_aplicado_inorg * duracion
    total_n_volatilizado_inorg = volatilizacion_inorg * duracion
    total_n_lixiviado_inorg = lixiviacion_inorg * duracion
    total_n_aplicado_org = n_aplicado_org * duracion
    total_n_volatilizado_org = volatilizacion_org * duracion
    total_n_lixiviado_org = lixiviacion_org * duracion

    total_n_aplicado = total_n_aplicado_inorg + total_n_aplicado_org
    total_n_volatilizado = total_n_volatilizado_inorg + total_n_volatilizado_org
    total_n_lixiviado = total_n_lixiviado_inorg + total_n_lixiviado_org

    n2o_n_directo = total_n_aplicado * EF1
    n2o_n_ind_vol = total_n_volatilizado * EF4
    n2o_n_ind_lix = total_n_lixiviado * EF5
    n2o_n_indirecto = n2o_n_ind_vol + n2o_n_ind_lix
    n2o_directo = n2o_n_directo * (44/28)
    n2o_ind_vol = n2o_n_ind_vol * (44/28)
    n2o_ind_lix = n2o_n_ind_lix * (44/28)
    n2o_indirecto = n2o_ind_vol + n2o_ind_lix
    n2o_directo_co2e = n2o_directo * GWP["N2O"]
    n2o_indirecto_co2e = n2o_indirecto * GWP["N2O"]
    emision_n2o_co2e_total = n2o_directo_co2e + n2o_indirecto_co2e

    return emision_produccion, emision_co2_urea, n2o_directo_co2e, n2o_indirecto_co2e, desglose

# =============================================================================
# AGROQUÍMICOS
# =============================================================================

def ingresar_agroquimicos(etapa):
    """
    Ingresa o muestra agroquímicos.
    VERSIÓN CON GUARDADO CONFIABLE.
    """
    # =====================================================================
    # INICIALIZACIÓN OBLIGATORIA
    # =====================================================================
    if 'datos_confirmados' not in st.session_state:
        st.session_state.datos_confirmados = {}
    
    if 'agroquimicos' not in st.session_state.datos_confirmados:
        st.session_state.datos_confirmados['agroquimicos'] = {}
    
    # =====================================================================
    # VERIFICAR MODO VISUALIZACIÓN
    # =====================================================================
    modo_vis = st.session_state.get('modo_visualizacion', False)
    
    if modo_vis:
        # MODO VISUALIZACIÓN: Mostrar resumen
        st.markdown(f"##### Agroquímicos - {etapa}")
        st.info("🔒 **MODO VISUALIZACIÓN** - Los datos no se pueden modificar")
        
        agroquimicos_confirmados = obtener_datos_confirmados('agroquimicos', etapa)
        
        if not agroquimicos_confirmados:
            st.info("📝 No hay agroquímicos confirmados para esta etapa")
            
            # GUARDAR LISTA VACÍA
            st.session_state.datos_confirmados['agroquimicos'][etapa] = []
            return []
        
        # Mostrar tabla resumen
        datos_tabla = []
        for agro in agroquimicos_confirmados:
            datos_tabla.append({
                "Nombre": agro.get("nombre_comercial", "Sin nombre"),
                "Categoría": agro.get("categoria", ""),
                "Tipo": agro.get("tipo", ""),
                "Cantidad IA (kg)": format_num(agro.get("cantidad_ia", 0)),
                "Emisiones (kg CO₂e)": format_num(agro.get("emisiones", 0))
            })
        
        if datos_tabla:
            df = pd.DataFrame(datos_tabla)
            st.dataframe(df, use_container_width=True, hide_index=True)
        
        return agroquimicos_confirmados
    
    # =========================================================================
    # MODO EDICIÓN: Mostrar formularios completos
    # =========================================================================
    st.markdown("##### Agroquímicos y pesticidas")
    agroquimicos = []
    nombres_comerciales_usados = []  # Para controlar duplicados
    contadores_categoria = {}  # Para contar por categoría
    categorias = [
        ("Pesticida", "pesticidas"),
        ("Fungicida", "fungicidas"),
        ("Insecticida", "insecticidas"),
        ("Herbicida", "herbicidas")
    ]
    tipos_dict = {
        "pesticidas": list(factores_emision["pesticidas"].keys()),
        "fungicidas": (
            ["Media"] +
            sorted([k for k in factores_emision["fungicidas"].keys() if k != "Media"])
        ),
        "insecticidas": (
            ["Media"] +
            sorted([k for k in factores_emision["insecticidas"].keys() if k != "Media"])
        ),
        "herbicidas": list(factores_emision["herbicidas"].keys())
    }
    n_agro = st.number_input(
        "Ingrese la cantidad de agroquímicos y/o pesticidas diferentes que utiliza",
        min_value=0, step=1, format="%.10g", key=f"num_agroquimicos_{etapa}"
    )
    for i in range(n_agro):
        with st.expander(f"Agroquímico #{i+1}"):
            categoria = st.selectbox(
                "Categoría",
                [nombre for nombre, _ in categorias],
                key=f"cat_agro_{etapa}_{i}"
            )
            clave_categoria = dict(categorias)[categoria]
            
            nombre_comercial = st.text_input(
                "Nombre comercial del agroquímico",
                placeholder="Ej: Roundup, Furadan, etc.",
                key=f"nombre_comercial_agro_{etapa}_{i}"
            )
            
            # Lógica de nombres por defecto basada en categoría
            if not nombre_comercial.strip():
                # Incrementar contador para esta categoría
                if categoria not in contadores_categoria:
                    contadores_categoria[categoria] = 0
                contadores_categoria[categoria] += 1
                nombre_final = f"{categoria.lower()} {contadores_categoria[categoria]}"
            else:
                nombre_final = nombre_comercial.strip()
            
            # Manejo de nombres duplicados con sufijos automáticos
            if nombre_final in nombres_comerciales_usados:
                contador = 1
                nombre_base = nombre_final
                while f"{nombre_base} {contador}" in nombres_comerciales_usados:
                    contador += 1
                nombre_final = f"{nombre_base} {contador}"
            nombres_comerciales_usados.append(nombre_final)
            
            tipo = st.selectbox(
                f"Tipo de {categoria.lower()}",
                tipos_dict[clave_categoria],
                key=f"tipo_agro_{etapa}_{i}"
            )
            modo = st.radio(
                "¿Cómo desea ingresar la cantidad?",
                ["Producto comercial (kg/ha·ciclo)", "Ingrediente activo (kg/ha·ciclo)"],
                key=f"modo_agro_{etapa}_{i}"
            )
            if modo == "Producto comercial (kg/ha·ciclo)":
                cantidad = st.number_input(
                    "Cantidad de producto comercial aplicada por hectárea en el ciclo (kg/ha·ciclo)",
                    min_value=0.0, format="%.10g", key=f"cantidad_agro_{etapa}_{i}"
                )
                concentracion = st.number_input(
                    "Concentración de ingrediente activo (%)",
                    min_value=0.0, max_value=100.0, value=100.0, format="%.10g",
                    key=f"concentracion_agro_{etapa}_{i}"
                )
                cantidad_ia = cantidad * (concentracion / 100)
            else:
                cantidad_ia = st.number_input(
                    "Cantidad de ingrediente activo aplicada por hectárea en el ciclo (kg/ha·ciclo)",
                    min_value=0.0, format="%.10g", key=f"cantidad_ia_agro_{etapa}_{i}"
                )
            # Permitir FE personalizado con hasta 6 decimales
            usar_fe_personalizado = st.checkbox(
                "¿Desea ingresar un factor de emisión personalizado para este agroquímico?",
                key=f"usar_fe_agro_{etapa}_{i}"
            )
            if usar_fe_personalizado:
                fe = st.number_input(
                    "Factor de emisión personalizado (kg CO₂e/kg ingrediente activo)",
                    min_value=0.0, step=0.000001, format="%.10g", key=f"fe_personalizado_agro_{etapa}_{i}"
                )
            else:
                fe = factores_emision[clave_categoria].get(tipo, valores_defecto["fe_agroquimico"])
            emisiones = cantidad_ia * fe
            agroquimicos.append({
                "categoria": clave_categoria,
                "tipo": tipo,
                "nombre_comercial": nombre_final,
                "cantidad_ia": cantidad_ia,
                "fe": fe,
                "emisiones": emisiones
            })
    # =====================================================================
    # GUARDADO OBLIGATORIO
    # =====================================================================
    st.session_state.datos_confirmados['agroquimicos'][etapa] = agroquimicos
    
    # Marcar cambios
    st.session_state.guardado_pendiente = True
    st.session_state.ultimo_cambio = datetime.now().isoformat()
    
    if agroquimicos:
        st.success(f"✅ {len(agroquimicos)} agroquímico(s) ingresado(s)")
    
    return agroquimicos

def calcular_emisiones_agroquimicos(agroquimicos, duracion):
    total = 0
    for ag in agroquimicos:
        total += ag["emisiones"] * duracion
    return total

# =============================================================================
# MAQUINARIA
# =============================================================================

def ingresar_maquinaria_ciclo(etapa):
    """
    Ingresa o muestra datos de maquinaria.
    VERSIÓN CON GUARDADO CONFIABLE.
    """
    # =====================================================================
    # INICIALIZACIÓN OBLIGATORIA
    # =====================================================================
    if 'datos_confirmados' not in st.session_state:
        st.session_state.datos_confirmados = {}
    
    if 'maquinaria' not in st.session_state.datos_confirmados:
        st.session_state.datos_confirmados['maquinaria'] = {}
    
    # =====================================================================
    # VERIFICAR MODO VISUALIZACIÓN
    # =====================================================================
    modo_vis = st.session_state.get('modo_visualizacion', False)
    
    if modo_vis:
        # MODO VISUALIZACIÓN
        st.markdown(f"##### Labores y maquinaria - {etapa}")
        st.info("🔒 **MODO VISUALIZACIÓN** - Los datos no se pueden modificar")
        
        labores_confirmadas = obtener_datos_confirmados('maquinaria', etapa)
        
        if not labores_confirmadas:
            st.info("📝 No hay labores confirmadas para esta etapa")
            
            # GUARDAR LISTA VACÍA
            st.session_state.datos_confirmados['maquinaria'][etapa] = []
            return []
        
        # Mostrar tabla
        datos_tabla = []
        for labor in labores_confirmadas:
            datos_tabla.append({
                "Labor": labor.get("nombre_labor", "Sin nombre"),
                "Maquinaria": labor.get("tipo_maquinaria", "Manual"),
                "Combustible": labor.get("tipo_combustible", "N/A"),
                "Consumo (L)": format_num(labor.get("litros", 0)),
                "Emisiones (kg CO₂e)": format_num(labor.get("emisiones", 0))
            })
        
        if datos_tabla:
            df = pd.DataFrame(datos_tabla)
            st.dataframe(df, use_container_width=True, hide_index=True)
        
        return labores_confirmadas
    
    # =========================================================================
    # MODO EDICIÓN: Mostrar formularios completos
    # =========================================================================
    st.markdown("##### Labores y maquinaria")
    labores = []
    n_labores = st.number_input(f"¿Cuántas labores desea agregar en el ciclo?", min_value=0, step=1, key=f"num_labores_{etapa}")
    
    for i in range(n_labores):
        with st.expander(f"Labor #{i+1}"):
            nombre_labor_opcion = st.selectbox("Nombre de la labor", opciones_labores, key=f"nombre_labor_opcion_{etapa}_{i}")
            if nombre_labor_opcion == "Otro":
                nombre_labor = st.text_input("Ingrese el nombre de la labor", key=f"nombre_labor_otro_{etapa}_{i}")
            else:
                nombre_labor = nombre_labor_opcion

            tipo_labor = st.radio("¿La labor es manual o mecanizada?", ["Manual", "Mecanizada"], key=f"tipo_labor_{etapa}_{i}")

            if tipo_labor == "Manual":
                st.info("Labor manual: no se considera huella de carbono directa de maquinaria ni combustible.")
                labores.append({
                    "nombre_labor": nombre_labor,
                    "tipo_maquinaria": "Manual",
                    "tipo_combustible": "N/A",
                    "litros": 0,
                    "emisiones": 0,
                    "fe_personalizado": None
                })
            else:
                n_maquinas = st.number_input(f"¿Cuántas maquinarias para esta labor?", min_value=1, step=1, key=f"num_maquinas_{etapa}_{i}")
                tipos_maquinaria = list(rendimientos_maquinaria.keys())
                for j in range(n_maquinas):
                    if j > 0:
                        st.markdown("---")
                    st.markdown(f"**Maquinaria #{j+1}**")
                    tipo_maq = st.selectbox("Tipo de maquinaria", tipos_maquinaria, key=f"tipo_maq_{etapa}_{i}_{j}")

                    if tipo_maq == "Otro":
                        nombre_maq = st.text_input("Ingrese el nombre de la maquinaria", key=f"nombre_maq_otro_{etapa}_{i}_{j}")
                        rendimiento_recomendado = float(rendimientos_maquinaria["Otro"])
                    else:
                        nombre_maq = tipo_maq
                        rendimiento_recomendado = float(rendimientos_maquinaria.get(tipo_maq, 10))

                    tipo_comb = st.selectbox("Tipo de combustible", list(factores_combustible.keys()), key=f"tipo_comb_{etapa}_{i}_{j}")
                    fe_comb_default = factores_combustible.get(tipo_comb, 0)

                    repeticiones = st.number_input("Número de pasadas o repeticiones en el ciclo", min_value=1, step=1, key=f"reps_ciclo_{etapa}_{i}_{j}")

                    modo = st.radio(
                        "¿Cómo desea ingresar el consumo por pasada?",
                        ["Litros de combustible por pasada", "Horas de uso por pasada"],
                        key=f"modo_lab_{etapa}_{i}_{j}"
                    )

                    if modo == "Horas de uso por pasada":
                        rendimiento = st.number_input(
                            "Ingrese el rendimiento real de su maquinaria (litros/hora)",
                            min_value=0.0,
                            value=rendimiento_recomendado,
                            step=0.1,
                            format="%.10g",
                            key=f"rendimiento_{etapa}_{i}_{j}"
                        )
                        horas = st.number_input("Horas de uso por pasada (h/ha·pasada)", min_value=0.0, format="%.10g", key=f"horas_{etapa}_{i}_{j}")
                        litros_por_pasada = horas * rendimiento
                    else:
                        litros_por_pasada = st.number_input("Litros de combustible por pasada (L/ha·pasada)", min_value=0.0, format="%.10g", key=f"litros_{etapa}_{i}_{j}")

                    # Permitir FE personalizado para el combustible
                    usar_fe_personalizado = st.checkbox(
                        "¿Desea ingresar un factor de emisión personalizado para el tipo de combustible?",
                        key=f"usar_fe_maq_{etapa}_{i}_{j}"
                    )
                    if usar_fe_personalizado:
                        fe_comb = st.number_input(
                            "Factor de emisión personalizado (kg CO₂e/litro)",
                            min_value=0.0,
                            step=0.000001,
                            format="%.10g",
                            key=f"fe_personalizado_maq_{etapa}_{i}_{j}"
                        )
                    else:
                        fe_comb = fe_comb_default

                    litros_totales = litros_por_pasada * repeticiones
                    emisiones = litros_totales * fe_comb

                    labores.append({
                        "nombre_labor": nombre_labor,
                        "tipo_maquinaria": nombre_maq,
                        "tipo_combustible": tipo_comb,
                        "litros": litros_totales,
                        "emisiones": emisiones,
                        "fe_personalizado": fe_comb if usar_fe_personalizado else None
                    })
    
    # =====================================================================
    # GUARDADO OBLIGATORIO
    # =====================================================================
    st.session_state.datos_confirmados['maquinaria'][etapa] = labores
    
    # Marcar cambios
    st.session_state.guardado_pendiente = True
    st.session_state.ultimo_cambio = datetime.now().isoformat()
    
    if labores:
        st.success(f"✅ {len(labores)} labor(es) ingresada(s)")
    
    return labores

def ingresar_maquinaria_perenne(etapa, tipo_etapa):
    st.markdown(f"Labores y maquinaria ({tipo_etapa})")
    if not opciones_labores:
        st.error("No hay labores definidas en la base de datos.")
        return []
    labores = []
    n_labores = st.number_input(
        f"¿Cuántas labores desea agregar en la etapa '{tipo_etapa}'?",
        min_value=0,
        step=1,
        value=0,
        key=f"num_labores_{etapa}_{tipo_etapa}"
    )
    for i in range(n_labores):
        with st.expander(f"Labor #{i+1}"):
            nombre_labor_opcion = st.selectbox(
                "Nombre de la labor",
                opciones_labores,
                key=f"nombre_labor_opcion_{etapa}_{tipo_etapa}_{i}"
            )
            if nombre_labor_opcion == "Otro":
                nombre_labor = st.text_input(
                    "Ingrese el nombre de la labor",
                    key=f"nombre_labor_otro_{etapa}_{tipo_etapa}_{i}"
                )
            else:
                nombre_labor = nombre_labor_opcion

            tipo_labor = st.radio(
                "¿La labor es manual o mecanizada?",
                ["Manual", "Mecanizada"],
                key=f"tipo_labor_{etapa}_{tipo_etapa}_{i}"
            )

            if tipo_labor == "Manual":
                st.info("Labor manual: no se considera huella de carbono directa de maquinaria ni combustible.")
                labores.append({
                    "nombre_labor": nombre_labor,
                    "tipo_maquinaria": "Manual",
                    "tipo_combustible": "N/A",
                    "litros": 0,
                    "emisiones": 0,
                    "fe_personalizado": None
                })
            else:
                if not rendimientos_maquinaria:
                    st.error("No hay tipos de maquinaria definidos en la base de datos.")
                    continue
                n_maquinas = st.number_input(
                    f"¿Cuántas maquinarias para esta labor?",
                    min_value=1,
                    step=1,
                    value=1,
                    key=f"num_maquinas_{etapa}_{tipo_etapa}_{i}"
                )
                tipos_maquinaria = list(rendimientos_maquinaria.keys())
                for j in range(n_maquinas):
                    if j > 0:
                        st.markdown("---")
                    st.markdown(f"**Maquinaria #{j+1}**")
                    tipo_maq = st.selectbox(
                        "Tipo de maquinaria",
                        tipos_maquinaria,
                        key=f"tipo_maq_{etapa}_{tipo_etapa}_{i}_{j}"
                    )

                    if tipo_maq == "Otro":
                        nombre_maq = st.text_input(
                            "Ingrese el nombre de la maquinaria",
                            key=f"nombre_maq_otro_{etapa}_{tipo_etapa}_{i}_{j}"
                        )
                        rendimiento_recomendado = float(rendimientos_maquinaria.get("Otro", 10))
                    else:
                        nombre_maq = tipo_maq
                        rendimiento_recomendado = float(rendimientos_maquinaria.get(tipo_maq, 10))

                    if not factores_combustible:
                        st.error("No hay tipos de combustible definidos en la base de datos.")
                        continue
                    tipo_comb = st.selectbox(
                        "Tipo de combustible",
                        list(factores_combustible.keys()),
                        key=f"tipo_comb_{etapa}_{tipo_etapa}_{i}_{j}"
                    )
                    fe_comb_default = factores_combustible.get(tipo_comb, 0)

                    repeticiones = st.number_input(
                        f"Número de pasadas o repeticiones en la etapa '{tipo_etapa}'",
                        min_value=1,
                        step=1,
                        value=1,
                        key=f"reps_{etapa}_{tipo_etapa}_{i}_{j}"
                    )

                    modo = st.radio(
                        "¿Cómo desea ingresar el consumo por pasada?",
                        ["Litros de combustible por pasada", "Horas de uso por pasada"],
                        key=f"modo_lab_{etapa}_{tipo_etapa}_{i}_{j}"
                    )

                    if modo == "Horas de uso por pasada":
                        rendimiento = st.number_input(
                            "Ingrese el rendimiento real de su maquinaria (litros/hora)",
                            min_value=0.0,
                            value=rendimiento_recomendado,
                            step=0.1,
                            format="%.10g",
                            key=f"rendimiento_{etapa}_{tipo_etapa}_{i}_{j}"
                        )
                        horas = st.number_input(
                            "Horas de uso por pasada (h/ha·pasada)",
                            min_value=0.0,
                            value=0.0,
                            step=0.1,
                            format="%.10g",
                            key=f"horas_{etapa}_{tipo_etapa}_{i}_{j}"
                        )
                        litros_por_pasada = horas * rendimiento
                    else:
                        litros_por_pasada = st.number_input(
                            "Litros de combustible por pasada (L/ha·pasada)",
                            min_value=0.0,
                            value=0.0,
                            step=0.1,
                            format="%.10g",
                            key=f"litros_{etapa}_{tipo_etapa}_{i}_{j}"
                        )

                    # Permitir FE personalizado para el combustible
                    usar_fe_personalizado = st.checkbox(
                        "¿Desea ingresar un factor de emisión personalizado para el tipo de combustible?",
                        key=f"usar_fe_maq_{etapa}_{tipo_etapa}_{i}_{j}"
                    )
                    if usar_fe_personalizado:
                        fe_comb = st.number_input(
                            "Factor de emisión personalizado (kg CO₂e/litro)",
                            min_value=0.0,
                            step=0.000001,
                            format="%.10g",
                            key=f"fe_personalizado_maq_{etapa}_{tipo_etapa}_{i}_{j}"
                        )
                    else:
                        fe_comb = fe_comb_default

                    litros_totales = litros_por_pasada * repeticiones
                    emisiones = litros_totales * fe_comb

                    labores.append({
                        "nombre_labor": nombre_labor,
                        "tipo_maquinaria": nombre_maq,
                        "tipo_combustible": tipo_comb,
                        "litros": litros_totales,
                        "emisiones": emisiones,
                        "fe_personalizado": fe_comb if usar_fe_personalizado else None
                    })
    return labores

def calcular_emisiones_maquinaria(labores, duracion):
    """
    Calcula las emisiones de maquinaria usando el FE personalizado si existe,
    o el de la base de datos si no.
    """
    total = 0
    for labor in labores:
        litros = labor.get("litros", 0)
        fe = labor.get("fe_personalizado", None)
        if fe is not None and fe > 0:
            fe_utilizado = fe
        else:
            tipo_comb = labor.get("tipo_combustible")
            fe_utilizado = factores_combustible.get(tipo_comb, 0)
        total += litros * fe_utilizado
    return total * duracion

# =============================================================================
# GESTION DE RESIDUOS
# =============================================================================

def ingresar_gestion_residuos(etapa):
    """
    Ingresa o muestra gestión de residuos.
    VERSIÓN CON GUARDADO CONFIABLE.
    """
    # =====================================================================
    # INICIALIZACIÓN OBLIGATORIA
    # =====================================================================
    if 'datos_confirmados' not in st.session_state:
        st.session_state.datos_confirmados = {}
    
    if 'residuos' not in st.session_state.datos_confirmados:
        st.session_state.datos_confirmados['residuos'] = {}
    
    # =====================================================================
    # VERIFICAR MODO VISUALIZACIÓN
    # =====================================================================
    modo_vis = st.session_state.get('modo_visualizacion', False)
    
    if modo_vis:
        # MODO VISUALIZACIÓN
        st.markdown(f"##### Gestión de residuos - {etapa}")
        st.info("🔒 **MODO VISUALIZACIÓN** - Los datos no se pueden modificar")
        
        datos_residuos_completos = obtener_datos_confirmados('residuos', etapa)
        
        # Verificar estructura
        if not datos_residuos_completos or not isinstance(datos_residuos_completos, dict):
            st.info("📝 No hay gestión de residuos confirmada para esta etapa")
            
            # GUARDAR ESTRUCTURA VACÍA
            st.session_state.datos_confirmados['residuos'][etapa] = {
                "detalle": {},
                "em_residuos": 0,
                "detalle_emisiones": {},
                "total_biomasa": 0
            }
            return 0, {}
        
        detalle = datos_residuos_completos.get('detalle', {})
        
        if not detalle:
            st.info("No se ingresó gestión de residuos")
            return 0, {}
        
        # Mostrar tabla
        total_biomasa = datos_residuos_completos.get('total_biomasa', 0)
        datos_tabla = []
        
        for metodo, datos_metodo in detalle.items():
            if isinstance(datos_metodo, dict):
                biomasa = datos_metodo.get("biomasa", 0)
                porcentaje = (biomasa / total_biomasa * 100) if total_biomasa > 0 else 0
                
                datos_tabla.append({
                    "Método": metodo,
                    "Biomasa (kg)": format_num(biomasa),
                    "Porcentaje": format_num(porcentaje, 1) + "%"
                })
        
        if datos_tabla:
            df = pd.DataFrame(datos_tabla)
            st.dataframe(df, use_container_width=True, hide_index=True)
        
        em_residuos = datos_residuos_completos.get('em_residuos', 0)
        detalle_emisiones = datos_residuos_completos.get('detalle_emisiones', {})
        
        st.info(f"**Total gestión de residuos:** {format_num(em_residuos)} kg CO₂e/ha")
        
        return em_residuos, detalle_emisiones
    
    # =========================================================================
    # MODO EDICIÓN: Mostrar formularios completos
    # =========================================================================
    # Detectar si es modo anual o perenne
    modo_perenne = "Implantacion" in etapa or "Crecimiento" in etapa or "Producción" in etapa or "produccion" in etapa.lower() or "perenne" in etapa.lower()
    if modo_perenne:
        st.subheader("Gestión de residuos vegetales")
    else:
        st.markdown("---")
        st.subheader("Gestión de residuos vegetales")
    st.markdown("""
    <div style="background-color:#e3f2fd; padding:0.7em; border-radius:6px;">
    <b>¿Qué son los residuos vegetales del huerto?</b><br>
    Son todos los restos de plantas generados en su predio durante el cultivo y cosecha:<br>
    • Ramas y hojas de poda • Frutos descartados o dañados • Restos de cosecha<br>
    • Raíces y tallos • Material vegetal no comercializable<br><br>
    <b>¿Cómo puede gestionarlos?</b><br>
    • <b>Quema:</b> Genera emisiones directas de CH₄ y N₂O por combustión.<br>
    • <b>Compostaje en el predio:</b> Proceso de descomposición controlada que genera emisiones según metodología IPCC.<br>
    • <b>Incorporación al suelo:</b> Enterrar o mezclar con tierra (no genera emisiones netas).<br>
    • <b>Retiro del campo:</b> Sacar del predio para gestión externa (sin emisiones en su huerto).<br>
    </div>
    """, unsafe_allow_html=True)

    activar = st.radio(
        "¿Desea ingresar la gestión de residuos vegetales para este ciclo?",
        ["No", "Sí"],
        key=f"activar_residuos_{etapa}"
    )
    detalle = {}

    if activar == "Sí":
        biomasa = st.number_input(
            "¿Cuántos kilogramos de residuos vegetales genera en total en este ciclo? (kg/hectárea, peso tal como salen del huerto)",
            min_value=0.0,
            format="%.10g",
            key=f"biomasa_total_{etapa}",
            help="Incluya todos los residuos: ramas de poda, hojas, frutos descartados, etc. Ingrese el peso tal como los recolecta, sin secar."
        )
        modo = st.radio(
            "¿Cómo desea ingresar la gestión de residuos?",
            ["Porcentaje (%)", "Kilogramos (kg)"],
            key=f"modo_residuos_{etapa}"
        )
        opciones = st.multiselect(
            "¿Cómo se gestionan los residuos? (puede seleccionar más de una opción)",
            ["Quema", "Compostaje", "Incorporación al suelo", "Retiro del campo"],
            key=f"opciones_residuos_{etapa}"
        )
        cantidades = {}
        suma = 0

        # --- Ajustes y opciones avanzadas por método ---
        ajustes = {}
        for op in opciones:
            with st.expander(f"Gestión: {op}"):
                if modo == "Porcentaje (%)":
                    valor = st.number_input(
                        f"¿Qué porcentaje de la biomasa va a '{op}'?",
                        min_value=0.0, max_value=100.0,
                        format="%.10g",
                        key=f"porc_{op}_{etapa}"
                    )
                    cantidad = biomasa * (valor / 100)
                else:
                    valor = st.number_input(
                        f"¿Cuántos kg de biomasa van a '{op}'?",
                        min_value=0.0, max_value=biomasa,
                        format="%.10g",
                        key=f"kg_{op}_{etapa}"
                    )
                    cantidad = valor
                cantidades[op] = cantidad
                suma += valor if modo == "Porcentaje (%)" else cantidad

                # --- Ajustes específicos por método ---
                if op == "Quema":
                    st.caption("Se aplicará fracción seca y fracción quemada según IPCC 2006 para el cálculo de huella de carbono.")
                    fraccion_seca = st.number_input(
                        "Fracción seca de la biomasa (valor recomendado IPCC: 0,8)",
                        min_value=0.0, max_value=1.0, value=factores_residuos["fraccion_seca"],
                        format="%.10g",
                        key=f"fraccion_seca_quema_{etapa}"
                    )
                    fraccion_quemada = st.number_input(
                        "Fracción de biomasa efectivamente quemada (valor recomendado IPCC: 0,85)",
                        min_value=0.0, max_value=1.0, value=FRACCION_QUEMADA,
                        format="%.10g",
                        key=f"fraccion_quemada_{etapa}"
                    )
                    ajustes[op] = {
                        "fraccion_seca": fraccion_seca,
                        "fraccion_quemada": fraccion_quemada,
                    }
                    st.info("Si no conoce estos valores, utilice los recomendados por el IPCC.")
                elif op == "Compostaje":
                    st.warning("⚠️ **Importante**: Solo considere el compostaje si se realiza dentro de su predio/huerto. Si los residuos se envían fuera para compostar, seleccione 'Retiro del campo'.")
                    
                    st.caption("Cálculo de huella de carbono según metodología IPCC 2006 para compostaje aeróbico de residuos vegetales generados en el huerto.")
                    
                    estado_residuos = st.radio(
                        "¿En qué estado están los residuos vegetales al momento de hacer el compost?",
                        [
                            "Frescos/húmedos (recién cosechados, podados o recolectados)",
                            "Secos (han perdido humedad, estuvieron al sol varios días)"
                        ],
                        key=f"estado_residuos_{etapa}",
                        help="Esta información determina qué factores de emisión IPCC aplicar. Los residuos frescos tienen más humedad, los secos han perdido agua naturalmente."
                    )
                    
                    base_calculo_key = "base_humeda" if estado_residuos.startswith("Frescos") else "base_seca"
                    ajustes_compost = {"base_calculo": base_calculo_key}
                    
                    if base_calculo_key == "base_seca":
                        fraccion_seca = st.number_input(
                            "¿Qué porcentaje de los residuos es materia seca? (típicamente 80% para residuos secos)",
                            min_value=0.0, max_value=100.0, value=factores_residuos["fraccion_seca"]*100,
                            format="%.1f",
                            key=f"fraccion_seca_compost_{etapa}"
                        ) / 100.0
                        ajustes_compost["fraccion_seca"] = fraccion_seca
                    
                    ajustes[op] = ajustes_compost
                elif op == "Incorporación al suelo":
                    st.caption("No se considera huella de carbono directa según IPCC 2006. (Modo avanzado para secuestro de carbono no implementado).")
                elif op == "Retiro del campo":
                    destino = st.text_input("Destino o nota sobre el retiro del residuo (opcional)", key=f"destino_retiro_{etapa}")
                    ajustes[op] = {"destino": destino}

        # Advertencias de suma
        if modo == "Porcentaje (%)":
            faltante = 100.0 - suma
            if faltante > 0:
                st.warning(f"Falta ingresar {format_num(faltante, decimales=1)}% para completar el 100% de los residuos.")
            elif faltante < 0:
                st.error(f"Ha ingresado más del 100% ({format_num(-faltante, decimales=1)}% excedente).")
        else:
            faltante = biomasa - suma
            if faltante > 0:
                st.warning(f"Falta ingresar {format_num(faltante, decimales=1)} kg para completar el total de residuos.")
            elif faltante < 0:
                st.error(f"Ha ingresado más residuos de los existentes ({format_num(-faltante, decimales=1)} kg excedente).")

        # Guardar detalle para cálculo posterior (NO mostrar tabla aquí)
        for op in opciones:
            detalle[op] = {"biomasa": cantidades[op], "ajustes": ajustes.get(op, {})}

        # Si hay faltante, agregar "Sin gestión"
        if faltante > 0 and len(opciones) > 0:
            if modo == "Porcentaje (%)":
                sin_gestion = biomasa * (faltante / 100)
            else:
                sin_gestion = faltante
            detalle["Sin gestion"] = {"biomasa": sin_gestion, "ajustes": {}}

    # Calcular emisiones y agregar al detalle
    em_residuos, detalle_emisiones = calcular_emisiones_residuos(detalle)
    
    # =====================================================================
    # GUARDADO OBLIGATORIO - PERO SOLO SI HAY DATOS REALES
    # =====================================================================
    total_biomasa = sum(d.get("biomasa", 0) for d in detalle.values() if isinstance(d, dict))
    
    # SOLO guardar si hay biomasa real
    if total_biomasa > 0 or activar == "Sí":
        datos_residuos_completos = {
            "detalle": detalle,
            "em_residuos": em_residuos,
            "detalle_emisiones": detalle_emisiones,
            "total_biomasa": total_biomasa
        }
        
        # Guardar SI hay datos reales
        st.session_state.datos_confirmados['residuos'][etapa] = datos_residuos_completos
        
        # Marcar cambios
        st.session_state.guardado_pendiente = True
        st.session_state.ultimo_cambio = datetime.now().isoformat()
        
        if total_biomasa > 0:
            st.success(f"✅ Gestión de residuos ingresada ({format_num(total_biomasa)} kg)")
    else:
        # NO guardar si no hay datos reales
        # Pero asegurar que existe la estructura en datos_confirmados (vacía)
        if 'residuos' not in st.session_state.datos_confirmados:
            st.session_state.datos_confirmados['residuos'] = {}
        if etapa not in st.session_state.datos_confirmados['residuos']:
            st.session_state.datos_confirmados['residuos'][etapa] = {
                "detalle": {},
                "em_residuos": 0,
                "detalle_emisiones": {},
                "total_biomasa": 0
            }
    
    return em_residuos, detalle_emisiones

def calcular_emisiones_residuos(detalle):
    """
    Calcula las emisiones de GEI por gestión de residuos vegetales según IPCC 2006.
    - detalle: dict con {"vía": {"biomasa": ..., "ajustes": {...}}}
    Devuelve: total_emisiones, detalle_emisiones (dict con emisiones por vía)
    """
    total_emisiones = 0
    detalle_emisiones = {}
    for via, datos in detalle.items():
        biomasa = datos.get("biomasa", 0)
        ajustes = datos.get("ajustes", {})
        emisiones = 0
        if via == "Quema":
            em_ch4, em_n2o = calcular_emisiones_quema_residuos(
                biomasa,
                fraccion_seca=ajustes.get("fraccion_seca"),
                fraccion_quemada=ajustes.get("fraccion_quemada"),
                ef_ch4=ajustes.get("ef_ch4"),
                ef_n2o=ajustes.get("ef_n2o")
            )
            emisiones = em_ch4 + em_n2o
        elif via == "Compostaje":
            em_ch4, em_n2o = calcular_emisiones_compostaje(
                biomasa,
                base_calculo=ajustes.get("base_calculo", "base_humeda"),
                fraccion_seca=ajustes.get("fraccion_seca")
            )
            emisiones = em_ch4 + em_n2o
        elif via == "Incorporación al suelo":
            emisiones = 0  # No se consideran emisiones directas según IPCC
        elif via == "Retiro del campo":
            emisiones = 0  # No se consideran emisiones dentro del predio
        elif via == "Sin gestión":
            emisiones = 0
        detalle_emisiones[via] = {"biomasa": biomasa, "emisiones": emisiones}
        total_emisiones += emisiones
    return total_emisiones, detalle_emisiones

def calcular_emisiones_quema_residuos(
    biomasa,
    fraccion_seca=None,
    fraccion_quemada=None,
    ef_ch4=None,
    ef_n2o=None
):
    if fraccion_seca is None:
        fraccion_seca = factores_residuos["fraccion_seca"]
    if fraccion_quemada is None:
        fraccion_quemada = FRACCION_QUEMADA
    if ef_ch4 is None:
        ef_ch4 = EF_CH4_QUEMA
    if ef_n2o is None:
        ef_n2o = EF_N2O_QUEMA
    biomasa_seca_quemada = biomasa * fraccion_seca * fraccion_quemada
    emision_CH4 = biomasa_seca_quemada * ef_ch4
    emision_N2O = biomasa_seca_quemada * ef_n2o
    emision_CH4_CO2e = emision_CH4 * GWP["CH4"]
    emision_N2O_CO2e = emision_N2O * GWP["N2O"]
    return emision_CH4_CO2e, emision_N2O_CO2e

def calcular_emisiones_compostaje(
    biomasa,
    base_calculo="base_humeda",
    fraccion_seca=None
):
    """
    Calcula emisiones de CH4 y N2O por compostaje aeróbico según IPCC 2006 Vol.5 Cap.3 Tabla 3.4.
    
    Args:
        biomasa: cantidad de biomasa compostada (kg, húmeda)
        base_calculo: "base_seca" o "base_humeda" según factores IPCC
        fraccion_seca: fracción seca de la biomasa (solo para base_seca)
    
    Returns:
        tuple: (emision_CH4_CO2e, emision_N2O_CO2e) en kg CO2e
    """
    if fraccion_seca is None:
        fraccion_seca = factores_residuos["fraccion_seca"]
    
    ef = factores_residuos["compostaje"][base_calculo]
    
    if base_calculo == "base_seca":
        # Aplicar factores a materia seca
        ms = biomasa * fraccion_seca
        em_ch4 = ms * ef["EF_CH4"]
        em_n2o = ms * ef["EF_N2O"]
    else:  # base_humeda
        # Aplicar factores directamente a materia húmeda
        em_ch4 = biomasa * ef["EF_CH4"]
        em_n2o = biomasa * ef["EF_N2O"]
    
    em_ch4_co2e = em_ch4 * GWP["CH4"]
    em_n2o_co2e = em_n2o * GWP["N2O"]
    return em_ch4_co2e, em_n2o_co2e

def calcular_emisiones_incorporacion(biomasa, fraccion_seca=None, modo="simple"):
    """
    Calcula emisiones por incorporación de residuos vegetales al suelo.
    - biomasa: cantidad de biomasa incorporada (kg/ha, húmeda)
    - fraccion_seca: fracción seca de la biomasa (por defecto, valor recomendado)
    - modo: "simple" (emisión nula) o "avanzado" (secuestro de carbono, pendiente)
    """
    if fraccion_seca is None:
        fraccion_seca = factores_residuos["fraccion_seca"]
    if modo == "simple":
        return 0
    elif modo == "avanzado":
        return 0

# =============================================================================
# RIEGO Y ENERGÍA
# =============================================================================

def ingresar_riego_ciclo(etapa):
    """
    Ingresa o muestra datos de riego.
    VERSIÓN CON GUARDADO CONFIABLE.
    """
    # =====================================================================
    # INICIALIZACIÓN OBLIGATORIA
    # =====================================================================
    if 'datos_confirmados' not in st.session_state:
        st.session_state.datos_confirmados = {}
    
    if 'riego' not in st.session_state.datos_confirmados:
        st.session_state.datos_confirmados['riego'] = {}
    
    # =====================================================================
    # VERIFICAR MODO VISUALIZACIÓN
    # =====================================================================
    modo_vis = st.session_state.get('modo_visualizacion', False)
    
    if modo_vis:
        # MODO VISUALIZACIÓN
        st.markdown(f"##### Riego y energía - {etapa}")
        st.info("🔒 **MODO VISUALIZACIÓN** - Los datos no se pueden modificar")
        
        datos_riego_completos = obtener_datos_confirmados('riego', etapa)
        
        # Verificar estructura
        if not datos_riego_completos or not isinstance(datos_riego_completos, dict):
            st.info("📝 No hay datos de riego y energía confirmados para esta etapa")
            
            # GUARDAR ESTRUCTURA VACÍA
            st.session_state.datos_confirmados['riego'][etapa] = {
                "em_agua_total": 0,
                "em_energia_total": 0,
                "energia_actividades": []
            }
            return 0, 0, []
        
        energia_actividades = datos_riego_completos.get('energia_actividades', [])
        
        if not energia_actividades:
            st.info("📝 No hay actividades de riego registradas")
            return 0, 0, []
        
        # Mostrar tabla
        datos_tabla = []
        for act in energia_actividades:
            datos_tabla.append({
                "Actividad": act.get("actividad", ""),
                "Tipo": act.get("tipo_actividad", ""),
                "Agua (m³)": format_num(act.get("agua_total_m3", 0)),
                "Consumo energía": format_num(act.get("consumo_energia", 0)),
                "Tipo energía": act.get("tipo_energia", ""),
                "Emis. agua (kg CO₂e)": format_num(act.get("emisiones_agua", 0)),
                "Emis. energía (kg CO₂e)": format_num(act.get("emisiones_energia", 0))
            })
        
        if datos_tabla:
            df = pd.DataFrame(datos_tabla)
            st.dataframe(df, use_container_width=True, hide_index=True)
        
        em_agua_total = datos_riego_completos.get('em_agua_total', 0)
        em_energia_total = datos_riego_completos.get('em_energia_total', 0)
        
        st.info(
            f"**Resumen riego:**\n"
            f"- Agua: {format_num(em_agua_total)} kg CO₂e/ha\n"
            f"- Energía: {format_num(em_energia_total)} kg CO₂e/ha\n"
            f"- **Total:** {format_num(em_agua_total + em_energia_total)} kg CO₂e/ha"
        )
        
        return em_agua_total, em_energia_total, energia_actividades
    
    # =========================================================================
    # MODO EDICIÓN: Mostrar formularios completos
    # =========================================================================
    st.markdown("### Riego y energía")
    st.caption("Agregue todas las actividades de riego y energía relevantes. Para cada actividad, ingrese el consumo de agua y energía si corresponde (puede dejar en 0 si no aplica).")

    actividades_base = ["Goteo", "Aspersión", "Surco", "Fertirriego", "Otro"]
    n_actividades = st.number_input(
        "¿Cuántas actividades de riego y energía desea agregar en este ciclo?",
        min_value=0, step=1, format="%.10g", key=f"num_actividades_riego_{etapa}"
    )
    energia_actividades = []
    em_agua_total = 0
    em_energia_total = 0

    for i in range(int(n_actividades)):
        with st.expander(f"Actividad #{i+1}"):
            actividad = st.selectbox(
                "Tipo de actividad",
                actividades_base,
                key=f"actividad_riego_{etapa}_{i}"
            )
            if actividad == "Otro":
                nombre_actividad = st.text_input(
                    "Ingrese el nombre de la actividad",
                    key=f"nombre_actividad_otro_{etapa}_{i}"
                )
            else:
                nombre_actividad = actividad

            # Agua (SIEMPRE)
            agua_total = st.number_input(
                "Cantidad total de agua aplicada (m³/ha·ciclo, puede ser 0 si no corresponde)",
                min_value=0.0,
                format="%.10g",
                key=f"agua_total_{etapa}_{i}"
            )

            st.markdown("---")  # Línea divisoria entre agua y energía

            # Energía (SIEMPRE)
            tipo_energia = st.selectbox(
                "Tipo de energía utilizada (puede dejar en 'Otro' y consumo 0 si no corresponde)",
                list(factores_combustible.keys()),
                key=f"tipo_energia_{etapa}_{i}"
            )
            modo_energia = st.radio(
                "¿Cómo desea ingresar el consumo de energía?",
                ["Consumo total (kWh/litros)", "Potencia × horas de uso"],
                key=f"modo_energia_{etapa}_{i}"
            )
            if tipo_energia == "Eléctrico":
                if modo_energia == "Consumo total (kWh/litros)":
                    consumo = st.number_input(
                        "Consumo total de electricidad (kWh/ha·ciclo)",
                        min_value=0.0,
                        format="%.10g",
                        key=f"consumo_elec_{etapa}_{i}"
                    )
                else:
                    potencia = st.number_input(
                        "Potencia del equipo (kW)",
                        min_value=0.0,
                        format="%.10g",
                        key=f"potencia_elec_{etapa}_{i}"
                    )
                    horas = st.number_input(
                        "Horas de uso (h/ha·ciclo)",
                        min_value=0.0,
                        format="%.10g",
                        key=f"horas_elec_{etapa}_{i}"
                    )
                    consumo = potencia * horas
            else:
                if modo_energia == "Consumo total (kWh/litros)":
                    consumo = st.number_input(
                        f"Consumo total de {tipo_energia} (litros/ha·ciclo)",
                        min_value=0.0,
                        format="%.10g",
                        key=f"consumo_comb_{etapa}_{i}"
                    )
                else:
                    potencia = st.number_input(
                        "Potencia del motor (kW)",
                        min_value=0.0,
                        format="%.10g",
                        key=f"potencia_comb_{etapa}_{i}"
                    )
                    horas = st.number_input(
                        "Horas de uso (h/ha·ciclo)",
                        min_value=0.0,
                        format="%.10g",
                        key=f"horas_comb_{etapa}_{i}"
                    )
                    rendimiento = st.number_input(
                        "Rendimiento del motor (litros/kWh)",
                        min_value=0.0,
                        value=valores_defecto["rendimiento_motor"],
                        format="%.10g",
                        key=f"rendimiento_comb_{etapa}_{i}"
                    )
                    consumo = potencia * horas * rendimiento

            # Factor de emisión (por defecto del diccionario, pero permitir personalizado)
            fe_energia = factores_combustible.get(tipo_energia, valores_defecto["fe_combustible_generico"])
            usar_fe_personalizado = st.checkbox(
                "¿Desea ingresar un factor de emisión personalizado para este tipo de energía?",
                key=f"usar_fe_energia_{etapa}_{i}"
            )
            if usar_fe_personalizado:
                fe_energia = st.number_input(
                    "Factor de emisión personalizado (kg CO₂e/kWh o kg CO₂e/litro)",
                    min_value=0.0,
                    step=0.000001,
                    format="%.10g",
                    key=f"fe_personalizado_energia_{etapa}_{i}"
                )

            emisiones_energia = consumo * fe_energia

            energia_actividades.append({
                "actividad": nombre_actividad,
                "tipo_actividad": actividad,
                "agua_total_m3": agua_total,
                "emisiones_agua": agua_total * 1000 * valores_defecto["fe_agua"],
                "consumo_energia": consumo,
                "tipo_energia": tipo_energia,
                "fe_energia": fe_energia,
                "emisiones_energia": emisiones_energia
            })
            em_agua_total += agua_total * 1000 * valores_defecto["fe_agua"]
            em_energia_total += emisiones_energia

    # Mostrar resultados globales de riego y energía
    st.info(
        f"**Riego y energía del ciclo:**\n"
        f"- Emisiones por agua de riego: {format_num(em_agua_total)} kg CO₂e/ha·ciclo\n"
        f"- Emisiones por energía: {format_num(em_energia_total)} kg CO₂e/ha·ciclo\n"
        f"- **Total riego y energía:** {format_num(em_agua_total + em_energia_total)} kg CO₂e/ha·ciclo"
    )

    # =====================================================================
    # GUARDADO OBLIGATORIO - PERO SOLO SI HAY DATOS REALES
    # =====================================================================
    datos_riego_completos = {
        "em_agua_total": em_agua_total,
        "em_energia_total": em_energia_total,
        "energia_actividades": energia_actividades
    }
    
    # SOLO guardar si hay actividades reales o consumo de agua/energía
    tiene_datos_reales = False
    
    if energia_actividades:
        # Verificar si hay consumo real (no solo estructuras vacías)
        for actividad in energia_actividades:
            if actividad.get("agua_total_m3", 0) > 0 or actividad.get("consumo_energia", 0) > 0:
                tiene_datos_reales = True
                break
    
    # También considerar si hay emisiones calculadas
    if em_agua_total > 0 or em_energia_total > 0:
        tiene_datos_reales = True
    
    if tiene_datos_reales:
        # Guardar SI hay datos reales
        st.session_state.datos_confirmados['riego'][etapa] = datos_riego_completos
        
        # Marcar cambios
        st.session_state.guardado_pendiente = True
        st.session_state.ultimo_cambio = datetime.now().isoformat()
        
        st.success(f"✅ {len(energia_actividades)} actividad(es) de riego ingresada(s)")
    else:
        # NO guardar si no hay datos reales
        # Pero asegurar que existe la estructura en datos_confirmados (vacía)
        if 'riego' not in st.session_state.datos_confirmados:
            st.session_state.datos_confirmados['riego'] = {}
        if etapa not in st.session_state.datos_confirmados['riego']:
            st.session_state.datos_confirmados['riego'][etapa] = {
                "em_agua_total": 0,
                "em_energia_total": 0,
                "energia_actividades": []
            }
    
    return em_agua_total, em_energia_total, energia_actividades

def ingresar_riego_implantacion(etapa):
    st.markdown("### Riego y energía")
    st.caption("Agregue todas las actividades de riego y energía relevantes. Para cada actividad, ingrese el consumo de agua y energía si corresponde (puede dejar en 0 si no aplica).")

    actividades_base = ["Goteo", "Aspersión", "Surco", "Fertirriego", "Otro"]
    n_actividades = st.number_input(
        "¿Cuántas actividades de riego y energía desea agregar en implantación?",
        min_value=0, step=1, format="%.10g", key=f"num_actividades_riego_implantacion_{etapa}"
    )
    energia_actividades = []
    em_agua_total = 0
    em_energia_total = 0

    for i in range(int(n_actividades)):
        with st.expander(f"Actividad #{i+1}"):
            actividad = st.selectbox(
                "Tipo de actividad",
                actividades_base,
                key=f"actividad_riego_implantacion_{etapa}_{i}"
            )
            if actividad == "Otro":
                nombre_actividad = st.text_input(
                    "Ingrese el nombre de la actividad",
                    key=f"nombre_actividad_otro_implantacion_{etapa}_{i}"
                )
            else:
                nombre_actividad = actividad

            # Agua (SIEMPRE)
            agua_total = st.number_input(
                "Cantidad total de agua aplicada (m³/ha, puede ser 0 si no corresponde)",
                min_value=0.0,
                format="%.10g",
                key=f"agua_total_implantacion_{etapa}_{i}"
            )

            st.markdown("---")  # Línea divisoria entre agua y energía

            # Energía (SIEMPRE)
            tipo_energia = st.selectbox(
                "Tipo de energía utilizada (puede dejar en 'Otro' y consumo 0 si no corresponde)",
                list(factores_combustible.keys()),
                key=f"tipo_energia_implantacion_{etapa}_{i}"
            )
            modo_energia = st.radio(
                "¿Cómo desea ingresar el consumo de energía?",
                ["Consumo total (kWh/litros)", "Potencia × horas de uso"],
                key=f"modo_energia_implantacion_{etapa}_{i}"
            )
            if tipo_energia == "Eléctrico":
                if modo_energia == "Consumo total (kWh/litros)":
                    consumo = st.number_input(
                        "Consumo total de electricidad (kWh/ha)",
                        min_value=0.0,
                        format="%.10g",
                        key=f"consumo_elec_implantacion_{etapa}_{i}"
                    )
                else:
                    potencia = st.number_input(
                        "Potencia del equipo (kW)",
                        min_value=0.0,
                        format="%.10g",
                        key=f"potencia_elec_implantacion_{etapa}_{i}"
                    )
                    horas = st.number_input(
                        "Horas de uso (h/ha)",
                        min_value=0.0,
                        format="%.10g",
                        key=f"horas_elec_implantacion_{etapa}_{i}"
                    )
                    consumo = potencia * horas
            else:
                if modo_energia == "Consumo total (kWh/litros)":
                    consumo = st.number_input(
                        f"Consumo total de {tipo_energia} (litros/ha)",
                        min_value=0.0,
                        format="%.10g",
                        key=f"consumo_comb_implantacion_{etapa}_{i}"
                    )
                else:
                    potencia = st.number_input(
                        "Potencia del motor (kW)",
                        min_value=0.0,
                        format="%.10g",
                        key=f"potencia_comb_implantacion_{etapa}_{i}"
                    )
                    horas = st.number_input(
                        "Horas de uso (h/ha)",
                        min_value=0.0,
                        format="%.10g",
                        key=f"horas_comb_implantacion_{etapa}_{i}"
                    )
                    rendimiento = st.number_input(
                        "Rendimiento del motor (litros/kWh)",
                        min_value=0.0,
                        value=valores_defecto["rendimiento_motor"],
                        format="%.10g",
                        key=f"rendimiento_comb_implantacion_{etapa}_{i}"
                    )
                    consumo = potencia * horas * rendimiento

            fe_energia = factores_combustible.get(tipo_energia, valores_defecto["fe_combustible_generico"])
            usar_fe_personalizado = st.checkbox(
                "¿Desea ingresar un factor de emisión personalizado para este tipo de energía?",
                key=f"usar_fe_energia_implantacion_{etapa}_{i}"
            )
            if usar_fe_personalizado:
                fe_energia = st.number_input(
                    "Factor de emisión personalizado (kg CO₂e/kWh o kg CO₂e/litro)",
                    min_value=0.0,
                    step=0.000001,
                    format="%.10g",
                    key=f"fe_personalizado_energia_implantacion_{etapa}_{i}"
                )

            emisiones_energia = consumo * fe_energia

            energia_actividades.append({
                "actividad": nombre_actividad,
                "tipo_actividad": actividad,
                "agua_total_m3": agua_total,
                "emisiones_agua": agua_total * 1000 * valores_defecto["fe_agua"],
                "consumo_energia": consumo,
                "tipo_energia": tipo_energia,
                "fe_energia": fe_energia,
                "emisiones_energia": emisiones_energia
            })
            em_agua_total += agua_total * 1000 * valores_defecto["fe_agua"]
            em_energia_total += emisiones_energia

    # Mostrar resultados globales de riego y energía
    st.info(
        f"**Riego y energía (Implantación):**\n"
        f"- Emisiones por agua de riego: {format_num(em_agua_total)} kg CO₂e\n"
        f"- Emisiones por energía: {format_num(em_energia_total)} kg CO₂e\n"
        f"- **Total riego y energía:** {format_num(em_agua_total + em_energia_total)} kg CO₂e"
    )

    return em_agua_total, em_energia_total, energia_actividades

def ingresar_riego_operacion_perenne(etapa, anios, sistema_riego_inicial):
    st.markdown("### Riego y energía")
    st.caption("Agregue todas las actividades de riego y energía relevantes. Para cada actividad, ingrese el consumo de agua y energía si corresponde (puede dejar en 0 si no aplica).")

    actividades_base = ["Goteo", "Aspersión", "Surco", "Fertirriego", "Otro"]
    emisiones_totales_agua = 0
    emisiones_totales_energia = 0
    emisiones_por_anio = []
    sistema_riego_actual = sistema_riego_inicial

    for anio in range(1, anios + 1):
        st.markdown(f"###### Año {anio}")
        cambiar = st.radio(
            "¿Desea cambiar el sistema de riego este año?",
            ["No", "Sí"],
            key=f"cambiar_riego_{etapa}_{anio}"
        )
        if cambiar == "Sí":
            sistema_riego_actual = st.selectbox("Nuevo tipo de riego", actividades_base, key=f"tipo_riego_{etapa}_{anio}")
        else:
            st.write(f"Tipo de riego: {sistema_riego_actual}")

        n_actividades = st.number_input(
            f"¿Cuántas actividades de riego y energía desea agregar en el año {anio}?",
            min_value=0, step=1, format="%.10g", key=f"num_actividades_riego_operacion_{etapa}_{anio}"
        )
        energia_actividades = []
        em_agua_total = 0
        em_energia_total = 0

        for i in range(int(n_actividades)):
            with st.expander(f"Actividad año {anio} #{i+1}"):
                actividad = st.selectbox(
                    "Tipo de actividad",
                    actividades_base,
                    key=f"actividad_riego_operacion_{etapa}_{anio}_{i}"
                )
                if actividad == "Otro":
                    nombre_actividad = st.text_input(
                        "Ingrese el nombre de la actividad",
                        key=f"nombre_actividad_otro_operacion_{etapa}_{anio}_{i}"
                    )
                else:
                    nombre_actividad = actividad

                # Agua (SIEMPRE)
                agua_total = st.number_input(
                    "Cantidad total de agua aplicada (m³/ha·año, puede ser 0 si no corresponde)",
                    min_value=0.0,
                    format="%.10g",
                    key=f"agua_total_operacion_{etapa}_{anio}_{i}"
                )

                st.markdown("---")  # Línea divisoria entre agua y energía

                # Energía (SIEMPRE)
                tipo_energia = st.selectbox(
                    "Tipo de energía utilizada (puede dejar en 'Otro' y consumo 0 si no corresponde)",
                    list(factores_combustible.keys()),
                    key=f"tipo_energia_operacion_{etapa}_{anio}_{i}"
                )
                modo_energia = st.radio(
                    "¿Cómo desea ingresar el consumo de energía?",
                    ["Consumo total (kWh/litros)", "Potencia × horas de uso"],
                    key=f"modo_energia_operacion_{etapa}_{anio}_{i}"
                )
                if tipo_energia == "Eléctrico":
                    if modo_energia == "Consumo total (kWh/litros)":
                        consumo = st.number_input(
                            "Consumo total de electricidad (kWh/ha·año)",
                            min_value=0.0,
                            format="%.10g",
                            key=f"consumo_elec_operacion_{etapa}_{anio}_{i}"
                        )
                    else:
                        potencia = st.number_input(
                            "Potencia del equipo (kW)",
                            min_value=0.0,
                            format="%.10g",
                            key=f"potencia_elec_operacion_{etapa}_{anio}_{i}"
                        )
                        horas = st.number_input(
                            "Horas de uso (h/ha·año)",
                            min_value=0.0,
                            format="%.10g",
                            key=f"horas_elec_operacion_{etapa}_{anio}_{i}"
                        )
                        consumo = potencia * horas
                else:
                    if modo_energia == "Consumo total (kWh/litros)":
                        consumo = st.number_input(
                            f"Consumo total de {tipo_energia} (litros/ha·año)",
                            min_value=0.0,
                            format="%.10g",
                            key=f"consumo_comb_operacion_{etapa}_{anio}_{i}"
                        )
                    else:
                        potencia = st.number_input(
                            "Potencia del motor (kW)",
                            min_value=0.0,
                            format="%.10g",
                            key=f"potencia_comb_operacion_{etapa}_{anio}_{i}"
                        )
                        horas = st.number_input(
                            "Horas de uso (h/ha·año)",
                            min_value=0.0,
                            format="%.10g",
                            key=f"horas_comb_operacion_{etapa}_{anio}_{i}"
                        )
                        rendimiento = st.number_input(
                            "Rendimiento del motor (litros/kWh)",
                            min_value=0.0,
                            value=valores_defecto["rendimiento_motor"],
                            format="%.10g",
                            key=f"rendimiento_comb_operacion_{etapa}_{anio}_{i}"
                        )
                        consumo = potencia * horas * rendimiento

                fe_energia = factores_combustible.get(tipo_energia, valores_defecto["fe_combustible_generico"])
                usar_fe_personalizado = st.checkbox(
                    "¿Desea ingresar un factor de emisión personalizado para este tipo de energía?",
                    key=f"usar_fe_energia_operacion_{etapa}_{anio}_{i}"
                )
                if usar_fe_personalizado:
                    fe_energia = st.number_input(
                        "Factor de emisión personalizado (kg CO₂e/kWh o kg CO₂e/litro)",
                        min_value=0.0,
                        step=0.000001,
                        format="%.10g",
                        key=f"fe_personalizado_energia_operacion_{etapa}_{anio}_{i}"
                    )

                emisiones_energia = consumo * fe_energia

                energia_actividades.append({
                    "actividad": nombre_actividad,
                    "tipo_actividad": actividad,
                    "agua_total_m3": agua_total,
                    "emisiones_agua": agua_total * 1000 * valores_defecto["fe_agua"],
                    "consumo_energia": consumo,
                    "tipo_energia": tipo_energia,
                    "fe_energia": fe_energia,
                    "emisiones_energia": emisiones_energia
                })
                em_agua_total += agua_total * 1000 * valores_defecto["fe_agua"]
                em_energia_total += emisiones_energia

        # Mostrar resultados del año
        st.info(
            f"**Año {anio} - Riego y energía:**\n"
            f"- Emisiones por agua de riego: {format_num(em_agua_total)} kg CO₂e/ha\n"
            f"- Emisiones por energía: {format_num(em_energia_total)} kg CO₂e/ha\n"
            f"- **Total riego y energía año {anio}:** {format_num(em_agua_total + em_energia_total)} kg CO₂e/ha"
        )

        emisiones_totales_agua += em_agua_total
        emisiones_totales_energia += em_energia_total
        emisiones_por_anio.append({
            "anio": anio,
            "em_agua": em_agua_total,
            "em_energia": em_energia_total,
            "tipo_riego": sistema_riego_actual,
            "energia_actividades": energia_actividades
        })

    # Mostrar resumen total de la etapa
    st.info(
        f"**Resumen total riego y energía etapa {etapa}:**\n"
        f"- Emisiones totales por agua de riego: {format_num(emisiones_totales_agua)} kg CO₂e/ha\n"
        f"- Emisiones totales por energía: {format_num(emisiones_totales_energia)} kg CO₂e/ha\n"
        f"- **Total de la etapa:** {format_num(emisiones_totales_agua + emisiones_totales_energia)} kg CO₂e/ha"
    )

    return emisiones_totales_agua, emisiones_totales_energia, emisiones_por_anio

def ingresar_riego_crecimiento(etapa, duracion, permitir_cambio_sistema=False):
    st.markdown("### Riego y energía")
    st.caption("Agregue todas las actividades de riego y energía relevantes. Para cada actividad, ingrese el consumo de agua y energía si corresponde (puede dejar en 0 si no aplica).")

    actividades_base = ["Goteo", "Aspersión", "Surco", "Fertirriego", "Otro"]
    n_actividades = st.number_input(
        "¿Cuántas actividades de riego y energía desea agregar?",
        min_value=0, step=1, format="%.10g", key=f"num_actividades_riego_crecimiento_{etapa}"
    )
    energia_actividades = []
    em_agua_total = 0
    em_energia_total = 0
    
    for i in range(int(n_actividades)):
        with st.expander(f"Actividad #{i+1}"):
            actividad = st.selectbox(
                "Tipo de actividad",
                actividades_base,
                key=f"actividad_riego_crecimiento_{etapa}_{i}"
            )
            if actividad == "Otro":
                nombre_actividad = st.text_input(
                    "Ingrese el nombre de la actividad",
                    key=f"nombre_actividad_otro_crecimiento_{etapa}_{i}"
                )
            else:
                nombre_actividad = actividad

            # Agua (SIEMPRE)
            agua_total = st.number_input(
                "Cantidad total de agua aplicada (m³/ha, puede ser 0 si no corresponde)",
                min_value=0.0,
                format="%.10g",
                key=f"agua_total_crecimiento_{etapa}_{i}"
            )

            st.markdown("---")  # Línea divisoria entre agua y energía

            # Energía (SIEMPRE)
            tipo_energia = st.selectbox(
                "Tipo de energía utilizada (puede dejar en 'Otro' y consumo 0 si no corresponde)",
                list(factores_combustible.keys()),
                key=f"tipo_energia_crecimiento_{etapa}_{i}"
            )
            modo_energia = st.radio(
                "¿Cómo desea ingresar el consumo de energía?",
                ["Consumo total (kWh/litros)", "Potencia × horas de uso"],
                key=f"modo_energia_crecimiento_{etapa}_{i}"
            )
            if tipo_energia == "Eléctrico":
                if modo_energia == "Consumo total (kWh/litros)":
                    consumo = st.number_input(
                        "Consumo total de electricidad (kWh/ha)",
                        min_value=0.0,
                        format="%.10g",
                        key=f"consumo_elec_crecimiento_{etapa}_{i}"
                    )
                else:
                    potencia = st.number_input(
                        "Potencia del equipo (kW)",
                        min_value=0.0,
                        format="%.10g",
                        key=f"potencia_elec_crecimiento_{etapa}_{i}"
                    )
                    horas = st.number_input(
                        "Horas de uso (h/ha)",
                        min_value=0.0,
                        format="%.10g",
                        key=f"horas_elec_crecimiento_{etapa}_{i}"
                    )
                    consumo = potencia * horas
            else:
                if modo_energia == "Consumo total (kWh/litros)":
                    consumo = st.number_input(
                        f"Consumo total de {tipo_energia} (litros/ha)",
                        min_value=0.0,
                        format="%.10g",
                        key=f"consumo_comb_crecimiento_{etapa}_{i}"
                    )
                else:
                    potencia = st.number_input(
                        "Potencia del motor (kW)",
                        min_value=0.0,
                        format="%.10g",
                        key=f"potencia_comb_crecimiento_{etapa}_{i}"
                    )
                    horas = st.number_input(
                        "Horas de uso (h/ha)",
                        min_value=0.0,
                        format="%.10g",
                        key=f"horas_comb_crecimiento_{etapa}_{i}"
                    )
                    rendimiento = st.number_input(
                        "Rendimiento del motor (litros/kWh)",
                        min_value=0.0,
                        value=valores_defecto["rendimiento_motor"],
                        format="%.10g",
                        key=f"rendimiento_comb_crecimiento_{etapa}_{i}"
                    )
                    consumo = potencia * horas * rendimiento

            fe_energia = factores_combustible.get(tipo_energia, valores_defecto["fe_combustible_generico"])
            usar_fe_personalizado = st.checkbox(
                "¿Desea ingresar un factor de emisión personalizado para este tipo de energía?",
                key=f"usar_fe_energia_crecimiento_{etapa}_{i}"
            )
            if usar_fe_personalizado:
                fe_energia = st.number_input(
                    "Factor de emisión personalizado (kg CO₂e/kWh o kg CO₂e/litro)",
                    min_value=0.0,
                    step=0.000001,
                    format="%.10g",
                    key=f"fe_personalizado_energia_crecimiento_{etapa}_{i}"
                )

            emisiones_energia = consumo * fe_energia

            energia_actividades.append({
                "actividad": nombre_actividad,
                "tipo_actividad": actividad,
                "agua_total_m3": agua_total,
                "emisiones_agua": agua_total * 1000 * valores_defecto["fe_agua"],
                "consumo_energia": consumo,
                "tipo_energia": tipo_energia,
                "fe_energia": fe_energia,
                "emisiones_energia": emisiones_energia
            })
            em_agua_total += agua_total * 1000 * valores_defecto["fe_agua"]
            em_energia_total += emisiones_energia

    # Mostrar resultados globales de riego y energía (POR AÑO, antes de multiplicar por duración)
    st.info(
        f"**Riego y energía (por año):**\n"
        f"- Emisiones por agua de riego: {format_num(em_agua_total)} kg CO₂e/ha·año\n"
        f"- Emisiones por energía: {format_num(em_energia_total)} kg CO₂e/ha·año\n"
        f"- **Total riego y energía:** {format_num(em_agua_total + em_energia_total)} kg CO₂e/ha·año"
    )

    st.session_state[f"energia_actividades_crecimiento_{etapa}"] = energia_actividades

    # Retornar valores ya multiplicados por la duración para mantener compatibilidad
    return em_agua_total * duracion, em_energia_total * duracion, energia_actividades

# =============================================================================
# MODO DE ANÁLISIS ANUAL
# =============================================================================

def etapa_anual():
    # =========================================================================
    # VERIFICAR MODO VISUALIZACIÓN
    # =========================================================================
    if st.session_state.get('modo_visualizacion', False):
        # MODO VISUALIZACIÓN: Recrear la interfaz de ingreso COMPLETA en modo solo lectura.
        st.header("📋 Ingreso de Datos - Ciclo Anual (Guardado)")
        st.success("✅ **PROYECTO GUARDADO** - Solo modo visualización")
        st.info("Estos son los datos que ingresaste. Para modificarlos, crea una nueva versión desde el sidebar.")
        st.markdown("---")

        # Intentar recuperar la configuración de ciclos desde los datos guardados.
        # Si no existen, mostrar valores por defecto o campos vacíos en solo lectura.
        # NOTA: Esta información debe estar guardada en session_state o en los datos confirmados para reconstruirse.
        # Se asume que 'n_ciclos' y 'ciclos_diferentes' se guardaron previamente.
        n_ciclos_guardado = st.session_state.get('n_ciclos', 1)
        ciclos_diferentes_guardado = st.session_state.get('ciclos_diferentes', 'No, todos los ciclos son iguales')

        # Mostrar la configuración de ciclos en solo lectura.
        st.markdown("#### Configuración de Ciclos")
        col1, col2 = st.columns(2)
        with col1:
            st.text_input("¿Cuántos ciclos realiza por año?", value=str(n_ciclos_guardado), disabled=True, key="info_n_ciclos")
        with col2:
            opciones = ["No, todos los ciclos son iguales", "Sí, cada ciclo es diferente"]
            indice = opciones.index(ciclos_diferentes_guardado) if ciclos_diferentes_guardado in opciones else 0
            st.selectbox("¿Los ciclos son diferentes entre sí?", options=opciones, index=indice, disabled=True, key="info_ciclos_diferentes")

        if ciclos_diferentes_guardado == "No, todos los ciclos son iguales":
            st.info(f"Se aplicarán los mismos datos del 'ciclo_tipico' a los {n_ciclos_guardado} ciclo(s).")
        else:
            st.info(f"Ingresó datos específicos para cada uno de los {n_ciclos_guardado} ciclo(s).")

        em_total_viz = 0
        prod_total_viz = 0

        # --- LÓGICA PRINCIPAL PARA MOSTRAR FORMULARIOS BLOQUEADOS ---
        if ciclos_diferentes_guardado == "No, todos los ciclos son iguales":
            st.markdown("### Datos del Ciclo Típico")
            
            # 1. Producción (si existe en datos guardados)
            prod_guardada = st.session_state.get('prod_total_por_ciclo_tipico', 0) # Necesitarías guardar este dato
            if prod_guardada and n_ciclos_guardado > 1:
                prod_guardada = prod_guardada / n_ciclos_guardado # Ajustar para mostrar por ciclo
            produccion_viz = st.number_input("Producción de fruta en el ciclo (kg/ha·ciclo)", min_value=0.0, value=float(prod_guardada), disabled=True, key="viz_prod_ciclo_tipico")
            
            st.markdown("---")
            st.subheader("Fertilizantes")
            # Esta función, al estar en modo visualización, mostrará los datos confirmados de 'ciclo_tipico' en una tabla.
            ingresar_fertilizantes("ciclo_tipico", unidad_cantidad="ciclo")
            
            st.markdown("---")
            st.subheader("Agroquímicos y pesticidas")
            ingresar_agroquimicos("ciclo_tipico")
            
            st.markdown("---")
            st.subheader("Riego")
            # Estas funciones retornan valores, pero en modo visualización también mostrarán tablas.
            ingresar_riego_ciclo("ciclo_tipico")
            
            st.markdown("---")
            st.subheader("Labores y maquinaria")
            ingresar_maquinaria_ciclo("ciclo_tipico")
            
            st.markdown("---")
            st.subheader("Gestión de residuos")
            ingresar_gestion_residuos("ciclo_tipico")

            # Calcular totales para retornar (usando datos de session_state)
            em_total_viz = st.session_state.get('em_total', 0)
            prod_total_viz = st.session_state.get('prod_total', 0)

        else:
            # Lógica para ciclos diferentes en modo visualización
            total_fert_viz = 0
            total_agroq_viz = 0
            total_riego_viz = 0
            total_maq_viz = 0
            total_res_viz = 0
            
            # Se asume que 'n_ciclos_guardado' es correcto.
            for i in range(int(n_ciclos_guardado)):
                ciclo_num = i + 1
                st.markdown(f"### Ciclo {ciclo_num}")
                
                # Producción por ciclo (si está guardada)
                prod_key = f'prod_ciclo_{ciclo_num}'
                prod_ciclo_guardada = st.session_state.get(prod_key, 0)
                st.number_input(f"Producción de fruta en el ciclo {ciclo_num} (kg/ha·ciclo)", min_value=0.0, value=float(prod_ciclo_guardada), disabled=True, key=f"viz_prod_ciclo_{ciclo_num}")

                st.subheader("Fertilizantes")
                ingresar_fertilizantes(f"ciclo_{ciclo_num}", unidad_cantidad="ciclo")
                
                st.subheader("Agroquímicos y pesticidas")
                ingresar_agroquimicos(f"ciclo_{ciclo_num}")
                
                st.subheader("Riego")
                ingresar_riego_ciclo(f"ciclo_{ciclo_num}")
                
                st.subheader("Labores y maquinaria")
                ingresar_maquinaria_ciclo(f"ciclo_{ciclo_num}")
                
                st.subheader("Gestión de residuos")
                ingresar_gestion_residuos(f"ciclo_{ciclo_num}")
                
                if i < int(n_ciclos_guardado) - 1: # No poner línea después del último ciclo
                    st.markdown("---")

            # Los totales se obtienen del session_state, que debería estar actualizado si los datos se cargaron correctamente.
            em_total_viz = st.session_state.get('em_total', 0)
            prod_total_viz = st.session_state.get('prod_total', 0)

        st.markdown("---")
        st.info("📊 **Los resultados calculados están en la pestaña 'Resultados'**")
        
        # Retornar los valores guardados (para que la pestaña Resultados funcione)
        return em_total_viz, prod_total_viz
    
    # =========================================================================
    # MODO EDICIÓN: Solo formularios editables (sin cálculos intermedios)
    # =========================================================================
    st.header("📝 Ingreso de Datos - Ciclo Anual")
    
    # === VERIFICAR SI HAY DATOS GUARDADOS Y CARGARLOS ===
    if 'resultados_globales' in st.session_state:
        # Si ya hay resultados calculados, usarlos directamente
        resultados = st.session_state.resultados_globales
        em_total = resultados.get('em_total', 0)
        prod_total = resultados.get('prod_total', 0)
        
        # Mostrar que los datos están cargados
        st.success("✅ Datos del proyecto cargados desde la memoria")
    
    # === MOSTRAR RESUMEN DE DATOS CONFIRMADOS ===
    mostrar_resumen_datos_confirmados()
    st.markdown("---")
    
    n_ciclos = st.number_input("¿Cuántos ciclos realiza por año?", min_value=1, step=1, key="n_ciclos")
    ciclos_diferentes = st.radio(
        "¿Los ciclos son diferentes entre sí?",
        ["No, todos los ciclos son iguales", "Sí, cada ciclo es diferente"],
        key="ciclos_diferentes"
    )
    if ciclos_diferentes == "No, todos los ciclos son iguales":
        st.info(
            f"""
            Todos los datos que ingrese a continuación se **asumirán iguales para cada ciclo** y se multiplicarán por {n_ciclos} ciclos.
            Es decir, el sistema considerará que en todos los ciclos usted mantiene los mismos consumos, actividades y hábitos de manejo.
            Si existen diferencias importantes entre ciclos, le recomendamos ingresar el detalle ciclo por ciclo.
            """
        )
    else:
        st.info(
            "Ingrese los datos correspondientes a cada ciclo. El sistema sumará los valores de todos los ciclos, permitiendo reflejar cambios o variaciones entre ciclos."
        )

    em_total = 0
    prod_total = 0
    emisiones_ciclos = []
    desglose_fuentes_ciclos = []

    if ciclos_diferentes == "No, todos los ciclos son iguales":
        st.markdown("### Datos para un ciclo típico (se multiplicará por el número de ciclos)")
        produccion = st.number_input("Producción de fruta en el ciclo (kg/ha·ciclo)", min_value=0.0, key="prod_ciclo_tipico")
        
        st.markdown("---")
        st.subheader("Fertilizantes")
        # Ingresar fertilizantes (los cálculos se hacen internamente)
        fert = ingresar_fertilizantes("ciclo_tipico", unidad_cantidad="ciclo")
        
        st.markdown("---")
        st.subheader("Agroquímicos y pesticidas")
        # Ingresar agroquímicos (los cálculos se hacen internamente)
        agroq = ingresar_agroquimicos("ciclo_tipico")
        
        st.markdown("---")
        st.subheader("Riego")
        # Ingresar riego (los cálculos se hacen internamente)
        em_agua, em_energia, energia_actividades = ingresar_riego_ciclo("ciclo_tipico")
        tipo_riego = st.session_state.get("tipo_riego_ciclo_tipico", "")
        
        st.markdown("---")
        st.subheader("Labores y maquinaria")
        # Ingresar maquinaria (los cálculos se hacen internamente)
        labores = ingresar_maquinaria_ciclo("ciclo_tipico")
        
        st.markdown("---")
        st.subheader("Gestión de residuos")
        # Ingresar residuos (los cálculos se hacen internamente)
        em_residuos, detalle_residuos = ingresar_gestion_residuos("ciclo_tipico")
        
        # OBTENER DATOS CONFIRMADOS para cálculos finales
        fertilizantes_confirmados = obtener_datos_confirmados('fertilizantes', 'ciclo_tipico')
        agroquimicos_confirmados = obtener_datos_confirmados('agroquimicos', 'ciclo_tipico')
        
        # Calcular emisiones (solo para guardar, NO mostrar)
        if fertilizantes_confirmados:
            em_fert_prod, em_fert_co2_urea, em_fert_n2o_dir, em_fert_n2o_ind, desglose_fert = calcular_emisiones_fertilizantes(
                {"fertilizantes": fertilizantes_confirmados}, 1
            )
            em_fert_total = em_fert_prod + em_fert_co2_urea + em_fert_n2o_dir + em_fert_n2o_ind
        else:
            em_fert_prod = em_fert_co2_urea = em_fert_n2o_dir = em_fert_n2o_ind = 0
            em_fert_total = 0
            desglose_fert = []
        
        if agroquimicos_confirmados:
            em_agroq = calcular_emisiones_agroquimicos(agroquimicos_confirmados, 1)
        else:
            em_agroq = 0
        
        if labores:
            em_maq = calcular_emisiones_maquinaria(labores, 1)
        else:
            em_maq = 0
        
        # Solo calcular si hay datos confirmados
        if fertilizantes_confirmados or agroquimicos_confirmados or labores:
            em_ciclo = em_fert_total + em_agroq + em_agua + em_energia + em_maq + em_residuos
            em_total = em_ciclo * n_ciclos
            prod_total = produccion * n_ciclos
            
            for ciclo in range(1, int(n_ciclos) + 1):
                desglose_fuentes_ciclos.append({
                    "Ciclo": ciclo,
                    "Fertilizantes": em_fert_total,
                    "Agroquímicos": em_agroq,
                    "Riego": em_agua + em_energia,
                    "Maquinaria": em_maq,
                    "Residuos": em_residuos,
                    "desglose_fertilizantes": desglose_fert,
                    "desglose_agroquimicos": agroquimicos_confirmados if agroquimicos_confirmados else [],
                    "desglose_maquinaria": labores,
                    "desglose_riego": {
                        "tipo_riego": tipo_riego,
                        "emisiones_agua": em_agua,
                        "emisiones_energia": em_energia,
                        "energia_actividades": energia_actividades
                    },
                    "desglose_residuos": detalle_residuos
                })
                emisiones_ciclos.append((ciclo, em_ciclo, produccion))

            # Guardar en variables globales (session_state)
            st.session_state.emisiones_fuentes["Fertilizantes"] = em_fert_total * n_ciclos
            st.session_state.emisiones_fuentes["Agroquímicos"] = em_agroq * n_ciclos
            st.session_state.emisiones_fuentes["Riego"] = (em_agua + em_energia) * n_ciclos
            st.session_state.emisiones_fuentes["Maquinaria"] = em_maq * n_ciclos
            st.session_state.emisiones_fuentes["Residuos"] = em_residuos * n_ciclos

            # Guardar resultados en session_state
            st.session_state.emisiones_etapas["Anual"] = em_total
            st.session_state.produccion_etapas["Anual"] = prod_total
            st.session_state.emisiones_fuente_etapa["Anual"] = {
                "Fertilizantes": st.session_state.emisiones_fuentes["Fertilizantes"],
                "Agroquímicos": st.session_state.emisiones_fuentes["Agroquímicos"],
                "Riego": st.session_state.emisiones_fuentes["Riego"],
                "Maquinaria": st.session_state.emisiones_fuentes["Maquinaria"],
                "Residuos": st.session_state.emisiones_fuentes["Residuos"]
            }
        
        # Mensaje informativo (sin cálculos detallados)
        if fertilizantes_confirmados or agroquimicos_confirmados or labores:
            st.info("✅ Datos ingresados correctamente. Ve a la pestaña 'Resultados' para ver los cálculos completos.")
        else:
            st.info("📝 Ingresa y confirma los datos en cada sección para poder calcular la huella de carbono.")

    else:
        total_fert = 0
        total_agroq = 0
        total_riego = 0
        total_maq = 0
        total_res = 0
        
        for i in range(int(n_ciclos)):
            st.markdown(f"### Ciclo {i+1}")
            produccion = st.number_input(f"Producción de fruta en el ciclo {i+1} (kg/ha·ciclo)", min_value=0.0, key=f"prod_ciclo_{i+1}")

            st.subheader("Fertilizantes")
            fert = ingresar_fertilizantes(f"ciclo_{i+1}", unidad_cantidad="ciclo")
            
            st.subheader("Agroquímicos y pesticidas")
            agroq = ingresar_agroquimicos(f"ciclo_{i+1}")
            
            st.subheader("Riego")
            em_agua, em_energia, energia_actividades = ingresar_riego_ciclo(f"ciclo_{i+1}")
            tipo_riego = st.session_state.get(f"tipo_riego_ciclo_{i+1}", "")
            
            st.subheader("Labores y maquinaria")
            labores = ingresar_maquinaria_ciclo(f"ciclo_{i+1}")
            
            st.subheader("Gestión de residuos")
            em_residuos, detalle_residuos = ingresar_gestion_residuos(f"ciclo_{i+1}")
            
            # OBTENER DATOS CONFIRMADOS para cálculos finales
            fertilizantes_confirmados = obtener_datos_confirmados('fertilizantes', f'ciclo_{i+1}')
            agroquimicos_confirmados = obtener_datos_confirmados('agroquimicos', f'ciclo_{i+1}')
            
            # Calcular emisiones (solo para guardar, NO mostrar)
            if fertilizantes_confirmados:
                em_fert_prod, em_fert_co2_urea, em_fert_n2o_dir, em_fert_n2o_ind, desglose_fert = calcular_emisiones_fertilizantes(
                    {"fertilizantes": fertilizantes_confirmados}, 1
                )
                em_fert_total = em_fert_prod + em_fert_co2_urea + em_fert_n2o_dir + em_fert_n2o_ind
            else:
                em_fert_prod = em_fert_co2_urea = em_fert_n2o_dir = em_fert_n2o_ind = 0
                em_fert_total = 0
                desglose_fert = []
            
            if agroquimicos_confirmados:
                em_agroq = calcular_emisiones_agroquimicos(agroquimicos_confirmados, 1)
            else:
                em_agroq = 0
            
            if labores:
                em_maq = calcular_emisiones_maquinaria(labores, 1)
            else:
                em_maq = 0
            
            # Solo sumar si hay datos confirmados
            if fertilizantes_confirmados or agroquimicos_confirmados or labores:
                em_ciclo = em_fert_total + em_agroq + em_agua + em_energia + em_maq + em_residuos
                em_total += em_ciclo
                prod_total += produccion
                
                desglose_fuentes_ciclos.append({
                    "Ciclo": i+1,
                    "Fertilizantes": em_fert_total,
                    "Agroquímicos": em_agroq,
                    "Riego": em_agua + em_energia,
                    "Maquinaria": em_maq,
                    "Residuos": em_residuos,
                    "desglose_fertilizantes": desglose_fert,
                    "desglose_agroquimicos": agroquimicos_confirmados if agroquimicos_confirmados else [],
                    "desglose_maquinaria": labores,
                    "desglose_riego": {
                        "tipo_riego": tipo_riego,
                        "emisiones_agua": em_agua,
                        "emisiones_energia": em_energia,
                        "energia_actividades": energia_actividades
                    },
                    "desglose_residuos": detalle_residuos
                })
                emisiones_ciclos.append((i+1, em_ciclo, produccion))

                total_fert += em_fert_total
                total_agroq += em_agroq
                total_riego += em_agua + em_energia
                total_maq += em_maq
                total_res += em_residuos
            
            # Mensaje informativo por ciclo
            if fertilizantes_confirmados or agroquimicos_confirmados or labores:
                st.info(f"✅ Datos del ciclo {i+1} ingresados correctamente.")
            else:
                st.info(f"📝 Ingresa y confirma los datos para el ciclo {i+1}.")

        # Solo guardar si hay datos
        if total_fert > 0 or total_agroq > 0 or total_riego > 0 or total_maq > 0 or total_res > 0:
            st.session_state.emisiones_fuentes["Fertilizantes"] = total_fert
            st.session_state.emisiones_fuentes["Agroquímicos"] = total_agroq
            st.session_state.emisiones_fuentes["Riego"] = total_riego
            st.session_state.emisiones_fuentes["Maquinaria"] = total_maq
            st.session_state.emisiones_fuentes["Residuos"] = total_res

            st.session_state.emisiones_etapas["Anual"] = em_total
            st.session_state.produccion_etapas["Anual"] = prod_total
            st.session_state.emisiones_fuente_etapa["Anual"] = {
                "Fertilizantes": st.session_state.emisiones_fuentes["Fertilizantes"],
                "Agroquímicos": st.session_state.emisiones_fuentes["Agroquímicos"],
                "Riego": st.session_state.emisiones_fuentes["Riego"],
                "Maquinaria": st.session_state.emisiones_fuentes["Maquinaria"],
                "Residuos": st.session_state.emisiones_fuentes["Residuos"]
            }
        
        # Mensaje final informativo
        if em_total > 0:
            st.success("✅ Todos los ciclos han sido ingresados. Ve a la pestaña 'Resultados' para ver los cálculos completos.")

    # Guardar siempre (incluso si está vacío) para mantener la estructura
    st.session_state["emisiones_ciclos"] = emisiones_ciclos
    st.session_state["desglose_fuentes_ciclos"] = desglose_fuentes_ciclos
    
    # Actualizar em_total y prod_total en session_state
    st.session_state.em_total = em_total
    st.session_state.prod_total = prod_total
    
    return em_total, prod_total
    
    # =========================================================================
    # MODO EDICIÓN: Flujo completo con formularios
    # =========================================================================
    st.header("Ciclo anual")
    
    # === VERIFICAR SI HAY DATOS GUARDADOS Y CARGARLOS ===
    if 'resultados_globales' in st.session_state:
        # Si ya hay resultados calculados, usarlos directamente
        resultados = st.session_state.resultados_globales
        em_total = resultados.get('em_total', 0)
        prod_total = resultados.get('prod_total', 0)
        
        # Mostrar que los datos están cargados
        st.success("✅ Datos del proyecto cargados desde la memoria")
        
        # Continuar con la lógica normal, pero sin recalcular desde cero
        # (mostrar los resultados existentes)
    
    # === MOSTRAR RESUMEN DE DATOS CONFIRMADOS ===
    mostrar_resumen_datos_confirmados()
    st.markdown("---")
    
    n_ciclos = st.number_input("¿Cuántos ciclos realiza por año?", min_value=1, step=1, key="n_ciclos")
    ciclos_diferentes = st.radio(
        "¿Los ciclos son diferentes entre sí?",
        ["No, todos los ciclos son iguales", "Sí, cada ciclo es diferente"],
        key="ciclos_diferentes"
    )
    if ciclos_diferentes == "No, todos los ciclos son iguales":
        st.info(
            f"""
            Todos los datos que ingrese a continuación se **asumirán iguales para cada ciclo** y se multiplicarán por {n_ciclos} ciclos.
            Es decir, el sistema considerará que en todos los ciclos usted mantiene los mismos consumos, actividades y hábitos de manejo.
            Si existen diferencias importantes entre ciclos, le recomendamos ingresar el detalle ciclo por ciclo.
            """
        )
    else:
        st.info(
            "Ingrese los datos correspondientes a cada ciclo. El sistema sumará los valores de todos los ciclos, permitiendo reflejar cambios o variaciones entre ciclos."
        )

    em_total = 0
    prod_total = 0
    emisiones_ciclos = []
    desglose_fuentes_ciclos = []

    if ciclos_diferentes == "No, todos los ciclos son iguales":
        st.markdown("### Datos para un ciclo típico (se multiplicará por el número de ciclos)")
        produccion = st.number_input("Producción de fruta en el ciclo (kg/ha·ciclo)", min_value=0.0, key="prod_ciclo_tipico")
        
        st.markdown("---")
        st.subheader("Fertilizantes")
        fert = ingresar_fertilizantes("ciclo_tipico", unidad_cantidad="ciclo")
        
        # OBTENER DATOS CONFIRMADOS para cálculos
        fertilizantes_confirmados = obtener_datos_confirmados('fertilizantes', 'ciclo_tipico')
        
        if fertilizantes_confirmados:
            em_fert_prod, em_fert_co2_urea, em_fert_n2o_dir, em_fert_n2o_ind, desglose_fert = calcular_emisiones_fertilizantes(
                {"fertilizantes": fertilizantes_confirmados}, 1
            )
            em_fert_total = em_fert_prod + em_fert_co2_urea + em_fert_n2o_dir + em_fert_n2o_ind
            st.info(
                f"**Fertilizantes (por ciclo):**\n"
                f"- Producción de fertilizantes: {format_num(em_fert_prod)} kg CO₂e/ha·ciclo\n"
                f"- Emisiones CO₂ por hidrólisis de urea: {format_num(em_fert_co2_urea)} kg CO₂e/ha·ciclo\n"
                f"- Emisiones directas N₂O: {format_num(em_fert_n2o_dir)} kg CO₂e/ha·ciclo\n"
                f"- Emisiones indirectas N₂O: {format_num(em_fert_n2o_ind)} kg CO₂e/ha·ciclo\n"
                f"- **Total fertilizantes:** {format_num(em_fert_total)} kg CO₂e/ha·ciclo"
            )
        else:
            st.info("ℹ️ Ingrese y confirme los fertilizantes para ver los cálculos")
            em_fert_prod = em_fert_co2_urea = em_fert_n2o_dir = em_fert_n2o_ind = 0
            em_fert_total = 0
            desglose_fert = []

        st.markdown("---")
        st.subheader("Agroquímicos y pesticidas")
        agroq = ingresar_agroquimicos("ciclo_tipico")
        
        # OBTENER DATOS CONFIRMADOS para cálculos
        agroquimicos_confirmados = obtener_datos_confirmados('agroquimicos', 'ciclo_tipico')
        
        if agroquimicos_confirmados:
            em_agroq = calcular_emisiones_agroquimicos(agroquimicos_confirmados, 1)
            st.info(
                f"**Agroquímicos (por ciclo):**\n"
                f"- **Total agroquímicos:** {format_num(em_agroq)} kg CO₂e/ha·ciclo"
            )
        else:
            st.info("ℹ️ Ingrese y confirme los agroquímicos para ver los cálculos")
            em_agroq = 0
            agroq = []

        st.markdown("---")
        st.subheader("Riego")
        em_agua, em_energia, energia_actividades = ingresar_riego_ciclo("ciclo_tipico")
        tipo_riego = st.session_state.get("tipo_riego_ciclo_tipico", "")

        st.markdown("---")
        st.subheader("Labores y maquinaria")
        labores = ingresar_maquinaria_ciclo("ciclo_tipico")
        
        # OBTENER DATOS CONFIRMADOS para cálculos (si aplica)
        # Nota: ingresar_maquinaria_ciclo ya retorna los datos, así que usamos directamente
        if labores:
            em_maq = calcular_emisiones_maquinaria(labores, 1)
            st.info(
                f"**Maquinaria (por ciclo):**\n"
                f"- **Total maquinaria:** {format_num(em_maq)} kg CO₂e/ha·ciclo"
            )
        else:
            st.info("ℹ️ Ingrese labores y maquinaria para ver los cálculos")
            em_maq = 0

        em_residuos, detalle_residuos = ingresar_gestion_residuos("ciclo_tipico")
        st.info(
            f"**Gestión de residuos (por ciclo):**\n"
            f"- **Total gestión de residuos:** {format_num(em_residuos)} kg CO₂e/ha·ciclo"
        )

        # Solo calcular si hay datos confirmados
        if fertilizantes_confirmados or agroquimicos_confirmados or labores:
            em_ciclo = em_fert_total + em_agroq + em_agua + em_energia + em_maq + em_residuos
            em_total = em_ciclo * n_ciclos
            prod_total = produccion * n_ciclos
            
            for ciclo in range(1, int(n_ciclos) + 1):
                desglose_fuentes_ciclos.append({
                    "Ciclo": ciclo,
                    "Fertilizantes": em_fert_total,
                    "Agroquímicos": em_agroq,
                    "Riego": em_agua + em_energia,
                    "Maquinaria": em_maq,
                    "Residuos": em_residuos,
                    "desglose_fertilizantes": desglose_fert,
                    "desglose_agroquimicos": agroquimicos_confirmados if agroquimicos_confirmados else [],
                    "desglose_maquinaria": labores,
                    "desglose_riego": {
                        "tipo_riego": tipo_riego,
                        "emisiones_agua": em_agua,
                        "emisiones_energia": em_energia,
                        "energia_actividades": energia_actividades
                    },
                    "desglose_residuos": detalle_residuos
                })
                emisiones_ciclos.append((ciclo, em_ciclo, produccion))

            # Guardar en variables globales (session_state)
            st.session_state.emisiones_fuentes["Fertilizantes"] = em_fert_total * n_ciclos
            st.session_state.emisiones_fuentes["Agroquímicos"] = em_agroq * n_ciclos
            st.session_state.emisiones_fuentes["Riego"] = (em_agua + em_energia) * n_ciclos
            st.session_state.emisiones_fuentes["Maquinaria"] = em_maq * n_ciclos
            st.session_state.emisiones_fuentes["Residuos"] = em_residuos * n_ciclos

            st.info(f"Huella de carbono por ciclo típico: {format_num(em_ciclo)} kg CO₂e/ha·ciclo")
            st.info(f"Huella de carbono anual (todos los ciclos): {format_num(em_total)} kg CO₂e/ha·año")

            # Guardar resultados en session_state
            st.session_state.emisiones_etapas["Anual"] = em_total
            st.session_state.produccion_etapas["Anual"] = prod_total
            st.session_state.emisiones_fuente_etapa["Anual"] = {
                "Fertilizantes": st.session_state.emisiones_fuentes["Fertilizantes"],
                "Agroquímicos": st.session_state.emisiones_fuentes["Agroquímicos"],
                "Riego": st.session_state.emisiones_fuentes["Riego"],
                "Maquinaria": st.session_state.emisiones_fuentes["Maquinaria"],
                "Residuos": st.session_state.emisiones_fuentes["Residuos"]
            }
        else:
            st.warning("⚠️ Confirma los datos en cada sección para ver los cálculos completos")

    else:
        total_fert = 0
        total_agroq = 0
        total_riego = 0
        total_maq = 0
        total_res = 0
        
        for i in range(int(n_ciclos)):
            st.markdown(f"### Ciclo {i+1}")
            produccion = st.number_input(f"Producción de fruta en el ciclo {i+1} (kg/ha·ciclo)", min_value=0.0, key=f"prod_ciclo_{i+1}")

            st.subheader("Fertilizantes")
            fert = ingresar_fertilizantes(f"ciclo_{i+1}", unidad_cantidad="ciclo")
            
            # OBTENER DATOS CONFIRMADOS para cálculos
            fertilizantes_confirmados = obtener_datos_confirmados('fertilizantes', f'ciclo_{i+1}')
            
            if fertilizantes_confirmados:
                em_fert_prod, em_fert_co2_urea, em_fert_n2o_dir, em_fert_n2o_ind, desglose_fert = calcular_emisiones_fertilizantes(
                    {"fertilizantes": fertilizantes_confirmados}, 1
                )
                em_fert_total = em_fert_prod + em_fert_co2_urea + em_fert_n2o_dir + em_fert_n2o_ind
                st.info(
                    f"**Fertilizantes (Ciclo {i+1}):**\n"
                    f"- Producción de fertilizantes: {format_num(em_fert_prod)} kg CO₂e/ha\n"
                    f"- Emisiones CO₂ por hidrólisis de urea: {format_num(em_fert_co2_urea)} kg CO₂e/ha\n"
                    f"- Emisiones directas N₂O: {format_num(em_fert_n2o_dir)} kg CO₂e/ha\n"
                    f"- Emisiones indirectas N₂O: {format_num(em_fert_n2o_ind)} kg CO₂e/ha\n"
                    f"- **Total fertilizantes:** {format_num(em_fert_total)} kg CO₂e/ha"
                )
            else:
                st.info(f"ℹ️ Ingrese y confirme los fertilizantes para el ciclo {i+1}")
                em_fert_prod = em_fert_co2_urea = em_fert_n2o_dir = em_fert_n2o_ind = 0
                em_fert_total = 0
                desglose_fert = []

            st.subheader("Agroquímicos y pesticidas")
            agroq = ingresar_agroquimicos(f"ciclo_{i+1}")
            
            # OBTENER DATOS CONFIRMADOS para cálculos
            agroquimicos_confirmados = obtener_datos_confirmados('agroquimicos', f'ciclo_{i+1}')
            
            if agroquimicos_confirmados:
                em_agroq = calcular_emisiones_agroquimicos(agroquimicos_confirmados, 1)
                st.info(
                    f"**Agroquímicos (Ciclo {i+1}):**\n"
                    f"- **Total agroquímicos:** {format_num(em_agroq)} kg CO₂e/ha"
                )
            else:
                st.info(f"ℹ️ Ingrese y confirme los agroquímicos para el ciclo {i+1}")
                em_agroq = 0
                agroq = []

            st.subheader("Riego")
            em_agua, em_energia, energia_actividades = ingresar_riego_ciclo(f"ciclo_{i+1}")
            tipo_riego = st.session_state.get(f"tipo_riego_ciclo_{i+1}", "")

            st.subheader("Labores y maquinaria")
            labores = ingresar_maquinaria_ciclo(f"ciclo_{i+1}")
            
            if labores:
                em_maq = calcular_emisiones_maquinaria(labores, 1)
                st.info(
                    f"**Maquinaria (Ciclo {i+1}):**\n"
                    f"- **Total maquinaria:** {format_num(em_maq)} kg CO₂e/ha"
                )
            else:
                st.info(f"ℹ️ Ingrese labores y maquinaria para el ciclo {i+1}")
                em_maq = 0

            em_residuos, detalle_residuos = ingresar_gestion_residuos(f"ciclo_{i+1}")
            st.info(
                f"**Gestión de residuos (Ciclo {i+1}):**\n"
                f"- **Total gestión de residuos:** {format_num(em_residuos)} kg CO₂e/ha"
            )

            # Solo sumar si hay datos confirmados
            if fertilizantes_confirmados or agroquimicos_confirmados or labores:
                em_ciclo = em_fert_total + em_agroq + em_agua + em_energia + em_maq + em_residuos
                em_total += em_ciclo
                prod_total += produccion
                
                desglose_fuentes_ciclos.append({
                    "Ciclo": i+1,
                    "Fertilizantes": em_fert_total,
                    "Agroquímicos": em_agroq,
                    "Riego": em_agua + em_energia,
                    "Maquinaria": em_maq,
                    "Residuos": em_residuos,
                    "desglose_fertilizantes": desglose_fert,
                    "desglose_agroquimicos": agroquimicos_confirmados if agroquimicos_confirmados else [],
                    "desglose_maquinaria": labores,
                    "desglose_riego": {
                        "tipo_riego": tipo_riego,
                        "emisiones_agua": em_agua,
                        "emisiones_energia": em_energia,
                        "energia_actividades": energia_actividades
                    },
                    "desglose_residuos": detalle_residuos
                })
                emisiones_ciclos.append((i+1, em_ciclo, produccion))

                total_fert += em_fert_total
                total_agroq += em_agroq
                total_riego += em_agua + em_energia
                total_maq += em_maq
                total_res += em_residuos

                st.info(f"Huella de carbono en ciclo {i+1}: {format_num(em_ciclo)} kg CO₂e/ha·ciclo")
            else:
                st.warning(f"⚠️ Confirma los datos en cada sección para el ciclo {i+1}")

        if n_ciclos > 1 and emisiones_ciclos:
            st.markdown("### Comparación de emisiones entre ciclos")
            for ciclo, em, prod in emisiones_ciclos:
                st.write(f"Ciclo {ciclo}: {format_num(em)} kg CO₂e/ha·ciclo, Producción: {format_num(prod)} kg/ha·ciclo")

        # Solo guardar si hay datos
        if total_fert > 0 or total_agroq > 0 or total_riego > 0 or total_maq > 0 or total_res > 0:
            st.session_state.emisiones_fuentes["Fertilizantes"] = total_fert
            st.session_state.emisiones_fuentes["Agroquímicos"] = total_agroq
            st.session_state.emisiones_fuentes["Riego"] = total_riego
            st.session_state.emisiones_fuentes["Maquinaria"] = total_maq
            st.session_state.emisiones_fuentes["Residuos"] = total_res

            st.session_state.emisiones_etapas["Anual"] = em_total
            st.session_state.produccion_etapas["Anual"] = prod_total
            st.session_state.emisiones_fuente_etapa["Anual"] = {
                "Fertilizantes": st.session_state.emisiones_fuentes["Fertilizantes"],
                "Agroquímicos": st.session_state.emisiones_fuentes["Agroquímicos"],
                "Riego": st.session_state.emisiones_fuentes["Riego"],
                "Maquinaria": st.session_state.emisiones_fuentes["Maquinaria"],
                "Residuos": st.session_state.emisiones_fuentes["Residuos"]
            }

    # Guardar siempre (incluso si está vacío) para mantener la estructura
    st.session_state["emisiones_ciclos"] = emisiones_ciclos
    st.session_state["desglose_fuentes_ciclos"] = desglose_fuentes_ciclos
    
    # Actualizar em_total y prod_total en session_state
    st.session_state.em_total = em_total
    st.session_state.prod_total = prod_total
    
    return em_total, prod_total

# =============================================================================
# ETAPAS MODO DE ANÁLISIS PERENNE
# =============================================================================

def etapa_implantacion():
    st.header("Implantación")
    duracion = st.number_input("Años de duración de la etapa de implantación", min_value=1, step=1, key="duracion_Implantacion")

    # 1. Fertilizantes
    st.markdown("---")
    st.subheader("Fertilizantes utilizados en implantación")
    st.info("Ingrese la cantidad de fertilizantes aplicados por año. El sistema multiplicará por la duración de la etapa.")
    fert = ingresar_fertilizantes("Implantacion", unidad_cantidad="año")
    em_fert_prod, em_fert_co2_urea, em_fert_n2o_dir, em_fert_n2o_ind, desglose_fert = calcular_emisiones_fertilizantes(fert, duracion)
    em_fert_total = em_fert_prod + em_fert_co2_urea + em_fert_n2o_dir + em_fert_n2o_ind
    st.info(
        f"**Fertilizantes (Implantación):**\n"
        f"- Producción de fertilizantes: {format_num(em_fert_prod)} kg CO₂e\n"
        f"- Emisiones CO₂ por hidrólisis de urea: {format_num(em_fert_co2_urea)} kg CO₂e\n"
        f"- Emisiones directas N₂O: {format_num(em_fert_n2o_dir)} kg CO₂e\n"
        f"- Emisiones indirectas N₂O: {format_num(em_fert_n2o_ind)} kg CO₂e\n"
        f"- **Total fertilizantes:** {format_num(em_fert_total)} kg CO₂e"
    )

    # 2. Agroquímicos
    st.markdown("---")
    st.subheader("Agroquímicos y pesticidas")
    st.info("Ingrese la cantidad de agroquímicos aplicados por año. El sistema multiplicará por la duración de la etapa.")
    agroq = ingresar_agroquimicos("Implantacion")
    em_agroq = calcular_emisiones_agroquimicos(agroq, duracion)
    st.info(
        f"**Agroquímicos (Implantación):**\n"
        f"- **Total agroquímicos:** {format_num(em_agroq)} kg CO₂e"
    )

    # 3. Riego (operación y energía para riego)
    st.markdown("---")
    st.subheader("Sistema de riego")
    em_agua, em_energia, energia_actividades = ingresar_riego_implantacion("Implantacion")
    tipo_riego = st.session_state.get("tipo_riego_Implantacion", None)

    # 4. Labores y maquinaria
    st.markdown("---")
    st.subheader("Labores y maquinaria")
    labores = ingresar_maquinaria_perenne("Implantacion", "Implantación")
    em_maq = calcular_emisiones_maquinaria(labores, duracion)
    st.info(
        f"**Maquinaria (Implantación):**\n"
        f"- **Total maquinaria:** {format_num(em_maq)} kg CO₂e"
    )

    # 5. Gestión de residuos vegetales
    st.markdown("---")
    st.subheader("Gestión de residuos vegetales")
    em_residuos, detalle_residuos = ingresar_gestion_residuos("Implantacion")
    st.info(
        f"**Gestión de residuos (Implantación):**\n"
        f"- **Total residuos:** {format_num(em_residuos)} kg CO₂e"
    )

    total = em_maq + em_agua + em_energia + em_fert_total + em_agroq + em_residuos

    # Guardar resultados por etapa y fuente
    emisiones_etapas["Implantación"] = total
    produccion_etapas["Implantación"] = 0  # No hay producción en implantación

    # ASIGNACIÓN DIRECTA (NO +=)
    emisiones_fuentes["Maquinaria"] = em_maq
    emisiones_fuentes["Riego"] = em_agua + em_energia
    emisiones_fuentes["Fertilizantes"] = em_fert_total
    emisiones_fuentes["Agroquímicos"] = em_agroq
    emisiones_fuentes["Residuos"] = em_residuos

    emisiones_fuente_etapa["Implantación"] = {
        "Fertilizantes": em_fert_total,
        "Agroquímicos": em_agroq,
        "Riego": em_agua + em_energia,
        "Maquinaria": em_maq,
        "Residuos": em_residuos,
        "desglose_fertilizantes": desglose_fert,
        "desglose_agroquimicos": agroq,
        "desglose_maquinaria": labores,
        "desglose_riego": {
            "tipo_riego": tipo_riego,
            "emisiones_agua": em_agua,
            "emisiones_energia": em_energia,
            "energia_actividades": energia_actividades
        },
        "desglose_residuos": detalle_residuos
    }

    st.success(f"Emisiones totales en etapa 'Implantación': {format_num(total)} kg CO₂e/ha para {duracion} años")
    return total, 0

def etapa_crecimiento(nombre_etapa, produccion_pregunta=True):
    st.header(nombre_etapa)
    duracion = st.number_input(f"Años de duración de la etapa {nombre_etapa}", min_value=1, step=1, key=f"duracion_{nombre_etapa}")
    segmentar = st.radio(
        "¿Desea ingresar información diferenciada para cada año de la etapa?",
        ["No, ingresaré datos generales para toda la etapa", "Sí, ingresaré datos año por año"],
        key=f"segmentar_{nombre_etapa}"
    )
    if segmentar == "No, ingresaré datos generales para toda la etapa":
        st.info(
            f"""
            Todos los datos que ingrese a continuación se **asumirán iguales para cada año** de la etapa y se multiplicarán por {duracion} años.
            Es decir, el sistema considerará que durante todos los años de esta etapa usted mantiene los mismos consumos, actividades y hábitos de manejo.
            Si existen diferencias importantes entre años (por ejemplo, cambios en fertilización, riego, labores, etc.), le recomendamos ingresar el detalle año por año.
            """
        )
    else:
        st.info(
            "Ingrese los datos correspondientes a cada año de la etapa. El sistema sumará los valores de todos los años."
        )

    produccion_total = 0
    em_total = 0
    resultados_anuales = []

    if segmentar == "Sí, ingresaré datos año por año":
        total_fert = 0
        total_agroq = 0
        total_riego = 0
        total_maq = 0
        total_res = 0
        for anio in range(1, int(duracion) + 1):
            em_anio = 0
            st.markdown(f"#### Año {anio}")
            if produccion_pregunta:
                produccion = st.number_input(f"Producción de fruta en el año {anio} (kg/ha)", min_value=0.0, key=f"prod_{nombre_etapa}_{anio}")
            else:
                produccion = 0

            st.markdown("---")
            st.subheader("Fertilizantes")
            fert = ingresar_fertilizantes(f"{nombre_etapa}_anio{anio}", unidad_cantidad="año")
            em_fert_prod, em_fert_co2_urea, em_fert_n2o_dir, em_fert_n2o_ind, desglose_fert = calcular_emisiones_fertilizantes(fert, 1)
            em_fert_total = em_fert_prod + em_fert_co2_urea + em_fert_n2o_dir + em_fert_n2o_ind
            st.info(
                f"**Fertilizantes (Año {anio}):**\n"
                f"- Producción de fertilizantes: {format_num(em_fert_prod)} kg CO₂e\n"
                f"- Emisiones CO₂ por hidrólisis de urea: {format_num(em_fert_co2_urea)} kg CO₂e\n"
                f"- Emisiones directas N₂O: {format_num(em_fert_n2o_dir)} kg CO₂e\n"
                f"- Emisiones indirectas N₂O: {format_num(em_fert_n2o_ind)} kg CO₂e\n"
                f"- **Total fertilizantes:** {format_num(em_fert_total)} kg CO₂e"
            )

            st.markdown("---")
            st.subheader("Agroquímicos y pesticidas")
            agroq = ingresar_agroquimicos(f"{nombre_etapa}_anio{anio}")
            em_agroq = calcular_emisiones_agroquimicos(agroq, 1)
            st.info(
                f"**Agroquímicos (Año {anio}):**\n"
                f"- **Total agroquímicos:** {format_num(em_agroq)} kg CO₂e"
            )

            st.markdown("---")
            st.subheader("Riego (operación)")
            em_agua, em_energia, energia_actividades = ingresar_riego_crecimiento(f"{nombre_etapa}_anio{anio}", 1, permitir_cambio_sistema=True)
            tipo_riego = st.session_state.get(f"tipo_riego_{nombre_etapa}_anio{anio}", None)

            st.markdown("---")
            st.subheader("Labores y maquinaria")
            labores = ingresar_maquinaria_perenne(f"{nombre_etapa}_anio{anio}", nombre_etapa)
            em_maq = calcular_emisiones_maquinaria(labores, 1)
            st.info(
                f"**Maquinaria (Año {anio}):**\n"
                f"- **Total maquinaria:** {format_num(em_maq)} kg CO₂e"
            )

            em_residuos, detalle_residuos = ingresar_gestion_residuos(f"{nombre_etapa}_anio{anio}")
            st.info(
                f"**Gestión de residuos (Año {anio}):**\n"
                f"- **Total residuos:** {format_num(em_residuos)} kg CO₂e"
            )

            em_anio = em_fert_total + em_agroq + em_agua + em_energia + em_maq + em_residuos
            em_total += em_anio
            produccion_total += produccion

            total_fert += em_fert_total
            total_agroq += em_agroq
            total_riego += em_agua + em_energia
            total_maq += em_maq
            total_res += em_residuos

            resultados_anuales.append({
                "Año": anio,
                "Huella de carbono (kg CO₂e/ha·año)": em_anio,
                "Producción (kg/ha·año)": produccion,
                "Fertilizantes": em_fert_total,
                "Agroquímicos": em_agroq,
                "Riego": em_agua + em_energia,
                "Maquinaria": em_maq,
                "Residuos": em_residuos
            })

            emisiones_fuente_etapa[f"{nombre_etapa} - Año {anio}"] = {
                "Fertilizantes": em_fert_total,
                "Agroquímicos": em_agroq,
                "Riego": em_agua + em_energia,
                "Maquinaria": em_maq,
                "Residuos": em_residuos,
                "desglose_fertilizantes": desglose_fert,
                "desglose_agroquimicos": agroq,
                "desglose_maquinaria": labores,
                "desglose_riego": {
                    "tipo_riego": tipo_riego,
                    "emisiones_agua": em_agua,
                    "emisiones_energia": em_energia,
                    "energia_actividades": energia_actividades
                },
                "desglose_residuos": detalle_residuos
            }

            st.info(f"Huella de carbono en año {anio}: {format_num(em_anio)} kg CO₂e/ha")

        emisiones_fuentes["Fertilizantes"] = total_fert
        emisiones_fuentes["Agroquímicos"] = total_agroq
        emisiones_fuentes["Riego"] = total_riego
        emisiones_fuentes["Maquinaria"] = total_maq
        emisiones_fuentes["Residuos"] = total_res

        if resultados_anuales:
            st.markdown("### Huella de carbono por año en esta etapa")
            df_anual = pd.DataFrame(resultados_anuales)
            df_anual["Huella de carbono (kg CO₂e/kg fruta·año)"] = df_anual.apply(
                lambda row: row["Huella de carbono (kg CO₂e/ha·año)"] / row["Producción (kg/ha·año)"] if row["Producción (kg/ha·año)"] > 0 else None,
                axis=1
            )
            st.dataframe(df_anual, hide_index=True)
            st.info(
                "🔎 Las emisiones por año corresponden a cada año de la etapa. "
                "Las emisiones totales de la etapa son la suma de todos los años."
            )

    else:
        if produccion_pregunta:
            produccion = st.number_input(f"Producción de fruta por año en esta etapa (kg/ha·año)", min_value=0.0, key=f"prod_{nombre_etapa}")
        else:
            produccion = 0
        
        st.markdown("---")
        st.subheader("Fertilizantes")
        fert = ingresar_fertilizantes(nombre_etapa, unidad_cantidad="año")
        em_fert_prod, em_fert_co2_urea, em_fert_n2o_dir, em_fert_n2o_ind, desglose_fert = calcular_emisiones_fertilizantes(fert, duracion)
        em_fert_total = em_fert_prod + em_fert_co2_urea + em_fert_n2o_dir + em_fert_n2o_ind
        st.info(
            f"**Fertilizantes (Etapa completa):**\n"
            f"- Producción de fertilizantes: {format_num(em_fert_prod)} kg CO₂e\n"
            f"- Emisiones CO₂ por hidrólisis de urea: {format_num(em_fert_co2_urea)} kg CO₂e\n"
            f"- Emisiones directas N₂O: {format_num(em_fert_n2o_dir)} kg CO₂e\n"
            f"- Emisiones indirectas N₂O: {format_num(em_fert_n2o_ind)} kg CO₂e\n"
            f"- **Total fertilizantes:** {format_num(em_fert_total)} kg CO₂e"
        )

        st.markdown("---")
        st.subheader("Agroquímicos y pesticidas")
        agroq = ingresar_agroquimicos(nombre_etapa)
        em_agroq = calcular_emisiones_agroquimicos(agroq, duracion)
        st.info(
            f"**Agroquímicos (Etapa completa):**\n"
            f"- **Total agroquímicos:** {format_num(em_agroq)} kg CO₂e"
        )

        st.markdown("---")
        st.subheader("Riego (operación)")
        em_agua, em_energia, energia_actividades = ingresar_riego_crecimiento(nombre_etapa, duracion, permitir_cambio_sistema=True)
        tipo_riego = st.session_state.get(f"tipo_riego_{nombre_etapa}", None)

        st.markdown("---")
        st.subheader("Labores y maquinaria")
        labores = ingresar_maquinaria_perenne(nombre_etapa, nombre_etapa)
        em_maq = calcular_emisiones_maquinaria(labores, duracion)
        st.info(
            f"**Maquinaria (Etapa completa):**\n"
            f"- **Total maquinaria:** {format_num(em_maq)} kg CO₂e"
        )

        em_residuos, detalle_residuos = ingresar_gestion_residuos(nombre_etapa)
        st.info(
            f"**Gestión de residuos (Etapa completa):**\n"
            f"- **Total residuos:** {format_num(em_residuos)} kg CO₂e"
        )

        em_total = em_fert_total + em_agroq + em_agua + em_energia + em_maq + em_residuos
        produccion_total = produccion * duracion

        emisiones_fuentes["Fertilizantes"] = em_fert_total
        emisiones_fuentes["Agroquímicos"] = em_agroq
        emisiones_fuentes["Riego"] = em_agua + em_energia
        emisiones_fuentes["Maquinaria"] = em_maq
        emisiones_fuentes["Residuos"] = em_residuos

        emisiones_fuente_etapa[nombre_etapa] = {
            "Fertilizantes": em_fert_total,
            "Agroquímicos": em_agroq,
            "Riego": em_agua + em_energia,
            "Maquinaria": em_maq,
            "Residuos": em_residuos,
            "desglose_fertilizantes": desglose_fert,
            "desglose_agroquimicos": agroq,
            "desglose_maquinaria": labores,
            "desglose_riego": {
                "tipo_riego": tipo_riego,
                "emisiones_agua": em_agua,
                "emisiones_energia": em_energia,
                "energia_actividades": energia_actividades
            },
            "desglose_residuos": detalle_residuos
        }

        st.info(f"Huella de carbono total en la etapa: {format_num(em_total)} kg CO₂e/ha para {duracion} años")
        st.info(f"Producción total en la etapa: {format_num(produccion_total)} kg/ha")

    emisiones_etapas[nombre_etapa] = em_total
    produccion_etapas[nombre_etapa] = produccion_total

    st.success(f"Emisiones totales en etapa '{nombre_etapa}': {format_num(em_total)} kg CO₂e/ha para {duracion} años")
    return em_total, produccion_total

def etapa_produccion_segmentada():
    st.header("Crecimiento con producción")
    st.warning(
        "Puede segmentar esta etapa en sub-etapas (por ejemplo, baja y alta producción). "
        "Si segmenta, para cada sub-etapa se preguntará la producción esperada y duración.\n\n"
        "🔎 **Sugerencia profesional:** Si desea considerar las emisiones asociadas al último año productivo del cultivo (por ejemplo, insumos, riego, energía, labores y actividades relacionadas con el fin de vida del huerto), "
        "le recomendamos crear una sub-etapa llamada **'Fin de vida'** dentro de esta etapa de producción. "
        "En esa sub-etapa podrá ingresar todos los insumos y actividades relevantes para el último año del cultivo, incluyendo la gestión de residuos vegetales generados por la remoción de plantas (árboles, arbustos, etc.).\n\n"
        "**Nota:** Si aún no ha llegado al fin de vida de su huerto, puede estimar estos valores según su experiencia o dejar la sub-etapa vacía. "
        "No cree una sub-etapa de fin de vida si ya incluyó todos los residuos y actividades en las sub-etapas anteriores."
    )
    segmentar = st.radio(
        "¿Desea segmentar esta etapa en sub-etapas?",
        ["No, usar una sola etapa", "Sí, segmentar en sub-etapas"],
        key="segmentar_produccion"
    )
    em_total = 0
    prod_total = 0
    emisiones_anuales = []  # [(año, emisiones, producción, nombre_subetapa)]
    if segmentar == "Sí, segmentar en sub-etapas":
        n_sub = st.number_input("¿Cuántas sub-etapas desea ingresar?", min_value=1, step=1, key="n_subetapas")
        anio_global = 1
        total_fert = 0
        total_agroq = 0
        total_riego = 0
        total_maq = 0
        total_res = 0
        for i in range(int(n_sub)):
            st.markdown(f"### Sub-etapa {i+1}")
            nombre = st.text_input(f"Nombre de la sub-etapa {i+1} (ej: baja producción, alta producción, fin de vida)", key=f"nombre_sub_{i}")
            prod = st.number_input(f"Producción esperada anual en esta sub-etapa (kg/ha/año)", min_value=0.0, key=f"prod_sub_{i}")
            dur = st.number_input(f"Años de duración de la sub-etapa", min_value=1, step=1, key=f"dur_sub_{i}")

            st.markdown(f"#### Datos para sub-etapa {i+1}: {nombre}")
            segmentar_anios = st.radio(
                f"¿Desea ingresar información diferenciada para cada año de la sub-etapa '{nombre}'?",
                ["No, ingresaré datos generales para toda la sub-etapa", "Sí, ingresaré datos año por año"],
                key=f"segmentar_anios_sub_{i}"
            )
            em_sub = 0
            prod_sub_total = 0
            if segmentar_anios == "Sí, ingresaré datos año por año":
                for anio in range(1, int(dur) + 1):
                    st.markdown(f"##### Año {anio}")
                    produccion = st.number_input(f"Producción de fruta en el año {anio} (kg/ha)", min_value=0.0, key=f"prod_{nombre}_{anio}_{i}")
                    
                    st.markdown("---")
                    st.subheader("Fertilizantes")
                    fert = ingresar_fertilizantes(f"{nombre}_anio{anio}_{i}", unidad_cantidad="año")
                    em_fert_prod, em_fert_co2_urea, em_fert_n2o_dir, em_fert_n2o_ind, desglose_fert = calcular_emisiones_fertilizantes(fert, 1)
                    em_fert_total = em_fert_prod + em_fert_co2_urea + em_fert_n2o_dir + em_fert_n2o_ind
                    # Mostrar resumen de fertilizantes
                    st.info(f"**Fertilizantes (año {anio}):** {format_num(em_fert_total)} kg CO₂e/ha")

                    st.markdown("---")
                    st.subheader("Agroquímicos y pesticidas")
                    agroq = ingresar_agroquimicos(f"{nombre}_anio{anio}_{i}")
                    em_agroq = calcular_emisiones_agroquimicos(agroq, 1)
                    # Mostrar resumen de agroquímicos
                    st.info(f"**Agroquímicos (año {anio}):** {format_num(em_agroq)} kg CO₂e/ha")

                    st.markdown("---")
                    st.subheader("Riego (operación)")
                    em_agua, em_energia, energia_actividades = ingresar_riego_crecimiento(f"{nombre}_anio{anio}_{i}", 1, permitir_cambio_sistema=True)
                    tipo_riego = st.session_state.get(f"tipo_riego_{nombre}_anio{anio}_{i}", None)

                    st.markdown("---")
                    st.subheader("Labores y maquinaria")
                    labores = ingresar_maquinaria_perenne(f"{nombre}_anio{anio}_{i}", nombre)
                    em_maq = calcular_emisiones_maquinaria(labores, 1)  # Solo por año
                    # Mostrar resumen de maquinaria
                    st.info(f"**Maquinaria (año {anio}):** {format_num(em_maq)} kg CO₂e/ha")

                    em_residuos, detalle_residuos = ingresar_gestion_residuos(f"{nombre}_anio{anio}_{i}")
                    # Mostrar resumen de residuos
                    st.info(f"**Gestión de residuos (año {anio}):** {format_num(em_residuos)} kg CO₂e/ha")

                    em_anio = em_fert_total + em_agroq + em_agua + em_energia + em_maq + em_residuos
                    em_sub += em_anio
                    prod_sub_total += produccion

                    total_fert += em_fert_total
                    total_agroq += em_agroq
                    total_riego += em_agua + em_energia
                    total_maq += em_maq
                    total_res += em_residuos

                    # Guardar emisiones y producción por año y sub-etapa
                    nombre_etapa = f"{nombre} - Año {anio_global}"
                    emisiones_etapas[nombre_etapa] = em_anio
                    produccion_etapas[nombre_etapa] = produccion
                    emisiones_anuales.append((anio_global, em_anio, produccion, nombre))
                    emisiones_fuente_etapa[nombre_etapa] = {
                        "Fertilizantes": em_fert_total,
                        "Agroquímicos": em_agroq,
                        "Riego": em_agua + em_energia,
                        "Maquinaria": em_maq,
                        "Residuos": em_residuos,
                        "desglose_fertilizantes": desglose_fert,
                        "desglose_agroquimicos": agroq,
                        "desglose_maquinaria": labores,
                        "desglose_riego": {
                            "tipo_riego": tipo_riego,
                            "emisiones_agua": em_agua,
                            "emisiones_energia": em_energia,
                            "energia_actividades": energia_actividades
                        },
                        "desglose_residuos": detalle_residuos
                    }
                    anio_global += 1

            else:
                st.markdown("---")
                st.subheader("Fertilizantes")
                fert = ingresar_fertilizantes(f"{nombre}_general_{i}", unidad_cantidad="año")
                em_fert_prod, em_fert_co2_urea, em_fert_n2o_dir, em_fert_n2o_ind, desglose_fert = calcular_emisiones_fertilizantes(fert, dur)
                em_fert_total = em_fert_prod + em_fert_co2_urea + em_fert_n2o_dir + em_fert_n2o_ind
                # Mostrar resumen de fertilizantes (por año)
                st.info(f"**Fertilizantes (por año):** {format_num(em_fert_total/dur)} kg CO₂e/ha·año → **Total sub-etapa:** {format_num(em_fert_total)} kg CO₂e/ha")

                st.markdown("---")
                st.subheader("Agroquímicos y pesticidas")
                agroq = ingresar_agroquimicos(f"{nombre}_general_{i}")
                em_agroq = calcular_emisiones_agroquimicos(agroq, dur)
                # Mostrar resumen de agroquímicos (por año)
                st.info(f"**Agroquímicos (por año):** {format_num(em_agroq/dur)} kg CO₂e/ha·año → **Total sub-etapa:** {format_num(em_agroq)} kg CO₂e/ha")

                st.markdown("---")
                st.subheader("Riego (operación)")
                em_agua, em_energia, energia_actividades = ingresar_riego_crecimiento(f"{nombre}_general_{i}", dur, permitir_cambio_sistema=True)
                tipo_riego = st.session_state.get(f"tipo_riego_{nombre}_general_{i}", None)

                st.markdown("---")
                st.subheader("Labores y maquinaria")
                labores = ingresar_maquinaria_perenne(f"{nombre}_general_{i}", nombre)
                em_maq = calcular_emisiones_maquinaria(labores, dur)  # Multiplica por duración
                # Mostrar resumen de maquinaria (por año)
                st.info(f"**Maquinaria (por año):** {format_num(em_maq/dur)} kg CO₂e/ha·año → **Total sub-etapa:** {format_num(em_maq)} kg CO₂e/ha")

                em_residuos, detalle_residuos = ingresar_gestion_residuos(f"{nombre}_general_{i}")
                # Mostrar resumen de residuos (por año)
                st.info(f"**Gestión de residuos (por año):** {format_num(em_residuos/dur)} kg CO₂e/ha·año → **Total sub-etapa:** {format_num(em_residuos)} kg CO₂e/ha")

                em_sub = em_fert_total + em_agroq + em_agua + em_energia + em_maq + em_residuos
                prod_sub_total = prod * dur

                total_fert += em_fert_total
                total_agroq += em_agroq
                total_riego += em_agua + em_energia
                total_maq += em_maq
                total_res += em_residuos

                nombre_etapa = f"{nombre}"
                emisiones_etapas[nombre_etapa] = em_sub
                produccion_etapas[nombre_etapa] = prod_sub_total
                emisiones_fuente_etapa[nombre_etapa] = {
                    "Fertilizantes": em_fert_total,
                    "Agroquímicos": em_agroq,
                    "Riego": em_agua + em_energia,
                    "Maquinaria": em_maq,
                    "Residuos": em_residuos,
                    "desglose_fertilizantes": desglose_fert,
                    "desglose_agroquimicos": agroq,
                    "desglose_maquinaria": labores,
                    "desglose_riego": {
                        "tipo_riego": tipo_riego,
                        "emisiones_agua": em_agua,
                        "emisiones_energia": em_energia,
                        "energia_actividades": energia_actividades
                    },
                    "desglose_residuos": detalle_residuos
                }
                for k in range(int(dur)):
                    emisiones_anuales.append((anio_global, em_sub/dur, prod, nombre))
                    anio_global += 1

            em_total += em_sub
            prod_total += prod_sub_total
            st.success(f"Emisiones totales en sub-etapa '{nombre}': {format_num(em_sub)} kg CO₂e/ha para {dur} años")

        emisiones_fuentes["Fertilizantes"] = total_fert
        emisiones_fuentes["Agroquímicos"] = total_agroq
        emisiones_fuentes["Riego"] = total_riego
        emisiones_fuentes["Maquinaria"] = total_maq
        emisiones_fuentes["Residuos"] = total_res

    else:
        nombre_etapa = st.text_input("Nombre para la etapa de producción (ej: Producción, Producción plena, etc.)", value="Producción", key="nombre_etapa_produccion_unica")
        em, prod = etapa_crecimiento(nombre_etapa, produccion_pregunta=True)
        em_total += em
        prod_total += prod

    st.session_state["emisiones_anuales"] = emisiones_anuales

    return em_total, prod_total

import locale
# Establecer el locale a español para los formatos numéricos
try:
    locale.setlocale(locale.LC_ALL, 'es_ES.UTF-8')
except:
    try:
        locale.setlocale(locale.LC_ALL, 'es_ES')
    except:
        try:
            locale.setlocale(locale.LC_ALL, 'Spanish_Spain.1252')
        except:
            locale.setlocale(locale.LC_ALL, '')

# Configurar Plotly para formato español
import plotly.io as pio
try:
    if pio.kaleido and pio.kaleido.scope:
        pio.kaleido.scope.default_format = "png"
except (AttributeError, TypeError):
    pass  # Kaleido no está disponible o no configurado
px.defaults.template = "plotly_white"

# Configuración global para separadores en Plotly
def configure_plotly_locale():
    """Configura Plotly para usar formato español"""
    return {
        'separators': ',.',  # Coma para decimales, punto para miles
        'locale': 'es'
    }

def apply_spanish_format_to_fig(fig):
    """
    Aplica formato español a cualquier gráfico de plotly
    """
    fig.update_layout(separators=',.')
    return fig

def format_num(x, decimales=None):
    """
    Formatea números con coma como separador decimal y punto como separador de miles
    Reglas de decimales automáticas según magnitud:
    - >= 1000: sin decimales
    - >= 10: 2 decimales máximo
    - >= 1: 2-3 decimales
    - < 1: 3-4 decimales (eliminando ceros innecesarios)
    """
    try:
        if pd.isnull(x) or x is None:
            return ""
        if isinstance(x, (float, int)):
            abs_x = abs(x)
            
            # Determinar número de decimales según magnitud si no se especifica
            if decimales is None:
                if abs_x >= 1000:
                    decimales = 0
                elif abs_x >= 10:
                    decimales = 2
                elif abs_x >= 1:
                    decimales = 2
                else:
                    decimales = 4
            
            # Formatear manualmente en formato español
            # Primero formatear con el número de decimales deseado
            if decimales == 0:
                formatted = f"{x:.0f}"
            else:
                formatted = f"{x:.{decimales}f}"
            
            # Separar parte entera y decimal
            if '.' in formatted:
                parte_entera, parte_decimal = formatted.split('.')
            else:
                parte_entera = formatted
                parte_decimal = ""
            
            # Agregar separadores de miles (puntos) a la parte entera
            if len(parte_entera) > 3:
                # Convertir a positivo para agregar separadores, luego restaurar signo
                es_negativo = parte_entera.startswith('-')
                if es_negativo:
                    parte_entera = parte_entera[1:]
                
                # Agregar puntos cada 3 dígitos de derecha a izquierda
                parte_entera_formateada = ""
                for i, digito in enumerate(reversed(parte_entera)):
                    if i > 0 and i % 3 == 0:
                        parte_entera_formateada = "." + parte_entera_formateada
                    parte_entera_formateada = digito + parte_entera_formateada
                
                if es_negativo:
                    parte_entera_formateada = "-" + parte_entera_formateada
                parte_entera = parte_entera_formateada
            
            # Eliminar ceros innecesarios al final de la parte decimal
            if parte_decimal:
                parte_decimal = parte_decimal.rstrip('0')
                if parte_decimal:
                    formatted = parte_entera + "," + parte_decimal
                else:
                    formatted = parte_entera
            else:
                formatted = parte_entera
            
            return formatted
        return str(x)
    except Exception:
        return str(x) if x is not None else ""

def format_percent(x, decimales=1):
    """
    Formatea porcentajes con coma como separador decimal
    Asume que x ya está en formato de porcentaje (0-100)
    """
    try:
        if pd.isnull(x) or x is None:
            return ""
        # NO multiplicar por 100 porque ya viene en formato de porcentaje
        # Usar format_num sin el símbolo de porcentaje
        formatted = format_num(x, decimales)
        return formatted + "%"
    except Exception:
        return str(x) + "%" if x is not None else ""

def format_fraction_as_percent(x, decimales=1):
    """
    Formatea fracciones (0.0-1.0) como porcentajes con coma como separador decimal
    Multiplica por 100 para convertir fracción a porcentaje
    """
    try:
        if pd.isnull(x) or x is None:
            return ""
        # Multiplicar por 100 para convertir fracción a porcentaje
        percentage = x * 100
        formatted = locale.format_string(f"%.{decimales}f", percentage, grouping=True)
        
        # Asegurar formato español para porcentajes
        if '.' in formatted and ',' not in formatted:
            parts = formatted.split('.')
            if len(parts) == 2 and len(parts[0]) <= 3:
                formatted = parts[0] + ',' + parts[1]
        
        return formatted + "%"
    except Exception:
        return str(x * 100) + "%" if x is not None else ""

def format_plotly_pie_percent(percent_value):
    """
    Formatea porcentajes específicamente para gráficos de torta de Plotly
    Convierte del formato inglés (12.3) al formato español (12,3%)
    """
    try:
        if isinstance(percent_value, (int, float)):
            formatted = locale.format_string("%.1f", percent_value, grouping=True)
            # Asegurar formato español
            if '.' in formatted and ',' not in formatted:
                parts = formatted.split('.')
                if len(parts) == 2:
                    formatted = parts[0] + ',' + parts[1]
            return formatted + "%"
        return str(percent_value) + "%"
    except Exception:
        return str(percent_value) + "%"

# -----------------------------
# Resultados Finales
# -----------------------------

def explicacion_fuente(fuente):
    if fuente == "Fertilizantes":
        return "Incluye la producción del fertilizante, emisiones directas de N₂O (por aplicación) y emisiones indirectas de N₂O (por volatilización y lixiviación)."
    elif fuente == "Riego":
        return "Corresponde al uso de agua (energía para extracción y distribución) y al tipo de energía utilizada (diésel, electricidad, etc.)."
    elif fuente == "Maquinaria":
        return "Proviene del consumo de combustibles fósiles (diésel, gasolina, etc.) en las labores agrícolas mecanizadas."
    elif fuente == "Agroquímicos":
        return "Incluye la producción y aplicación de pesticidas, fungicidas y herbicidas."
    elif fuente == "Residuos":
        return "Emisiones por gestión de residuos vegetales: quema, compostaje, incorporación al suelo, etc."
    else:
        return "Desglose no disponible para esta fuente."

import numpy as np

###################################################
# RESULTADOS PARA CULTIVO ANUAL
###################################################

def mostrar_resultados_anual(em_total, prod_total):
    # =========================================================================
    # VERIFICAR MODO VISUALIZACIÓN - Mensaje diferente
    # =========================================================================
    if st.session_state.get('modo_visualizacion', False):
        st.header("📊 Resultados Finales del Proyecto")
        st.success("✅ **PROYECTO GUARDADO** - Solo modo visualización")
        
        # Información específica para modo visualización
        st.info(
            "Estos son los resultados finales del proyecto guardado. "
            "Los datos ya no se pueden modificar. "
            "Para crear una versión editable, usa la opción 'Crear nueva versión' en el sidebar."
        )
    else:
        st.header("📈 Resultados Preliminares")
        st.warning("⚠️ **PROYECTO EN EDICIÓN** - Estos resultados son preliminares")
        
        # Información específica para modo edición
        st.info(
            "En esta sección se presentan los resultados globales y desglosados del cálculo de huella de carbono para el cultivo anual. "
            "Se muestran los resultados globales del sistema productivo, el detalle por ciclo productivo y por fuente de emisión, "
            "y finalmente el desglose interno de cada fuente. Todas las tablas muestran emisiones en kg CO₂e/ha·año y kg CO₂e/kg fruta·año. "
            "Todos los gráficos muestran emisiones en kg CO₂e/ha·año."
        )
        
        # Recordatorio para guardar
        st.warning("""
        **⚠️ IMPORTANTE:** 
        Estos resultados son PRELIMINARES. 
        Para guardarlos permanentemente, usa el botón **'💾 Guardar Proyecto'** en el sidebar.
        Una vez guardado, no podrás modificar los datos.
        """)

    # --- USAR VARIABLES DE SESSION_STATE - VERSIÓN CORREGIDA ---
    # Eliminar la línea 'global' y usar directamente session_state
    emisiones_fuentes = st.session_state.emisiones_fuentes
    emisiones_etapas = st.session_state.emisiones_etapas
    produccion_etapas = st.session_state.produccion_etapas
    emisiones_fuente_etapa = st.session_state.emisiones_fuente_etapa

    # --- RECONSTRUCCIÓN CORRECTA DE TOTALES GLOBALES DESDE EL DESGLOSE ---
    fuentes = ["Fertilizantes", "Agroquímicos", "Riego", "Maquinaria", "Residuos"]
    desglose_fuentes_ciclos = st.session_state.get("desglose_fuentes_ciclos", [])
    emisiones_fuentes_reales = {f: 0 for f in fuentes}
    
    for ciclo in desglose_fuentes_ciclos:
        for f in fuentes:
            emisiones_fuentes_reales[f] += ciclo.get(f, 0)
    
    # Actualiza los acumuladores globales
    for f in fuentes:
        emisiones_fuentes[f] = emisiones_fuentes_reales[f]
    
    em_total = sum(emisiones_fuentes_reales.values())
    
    # Si hay producción total, recalcúlala desde los ciclos
    emisiones_ciclos = st.session_state.get("emisiones_ciclos", [])
    prod_total = sum([c[2] for c in emisiones_ciclos]) if emisiones_ciclos else prod_total

    # --- Resultados globales ---
    st.markdown("#### Resultados globales")
    
    # Mensaje diferente según el modo
    if st.session_state.get('modo_visualizacion', False):
        st.success("✅ **Resultados finales calculados**")
    else:
        st.warning("📊 **Resultados preliminares calculados**")
    
    st.metric("Huella de carbono por hectárea", format_num(em_total, 2) + " kg CO₂e/ha·año")
    
    if prod_total > 0:
        st.metric("Huella de carbono por kg de fruta", format_num(em_total / prod_total, 3) + " kg CO₂e/kg fruta")
    else:
        st.warning("No se ha ingresado producción total. No es posible calcular emisiones por kg de fruta.")

    # --- Gráficos globales de fuentes ---
    valores_fuentes = [emisiones_fuentes.get(f, 0) for f in fuentes]
    total_fuentes = sum(valores_fuentes)
    
    st.markdown("#### % de contribución de cada fuente (global, kg CO₂e/ha·año)")
    col1, col2 = st.columns(2)
    
    with col1:
        fig_bar = px.bar(
            x=fuentes,
            y=valores_fuentes,
            labels={"x": "Fuente", "y": "Huella de carbono (kg CO₂e/ha·año)"},
            color=fuentes,
            color_discrete_sequence=px.colors.qualitative.Set2,
            title="Huella de carbono por fuente en el año",
        )
        y_max = max(valores_fuentes) if valores_fuentes else 1
        textos = [format_num(v) for v in valores_fuentes]
        fig_bar.add_trace(go.Scatter(
            x=fuentes,
            y=valores_fuentes,
            text=textos,
            mode="text",
            textposition="top center",
            showlegend=False
        ))
        fig_bar.update_layout(showlegend=False, height=400, separators=',.')
        fig_bar.update_yaxes(range=[0, y_max * 1.15])
        st.plotly_chart(fig_bar, use_container_width=True, key=get_unique_key())
    
    with col2:
        if total_fuentes > 0:
            # Calcular porcentajes con formato español
            porcentajes = [(v/total_fuentes)*100 for v in valores_fuentes]
            # Crear textos personalizados con formato español
            textos_personalizados = [
                f"{fuente}<br>{format_plotly_pie_percent(pct)}" 
                for fuente, pct in zip(fuentes, porcentajes)
            ]
            
            fig_pie = px.pie(
                names=fuentes,
                values=valores_fuentes,
                title="% de contribución de cada fuente",
                color=fuentes,
                color_discrete_sequence=px.colors.qualitative.Set2,
                hole=0.3
            )
            # Actualizar para mostrar nombres y porcentajes con formato español
            fig_pie.update_traces(
                textinfo='label+percent',
                texttemplate='%{label}<br>%{percent}',
                hovertemplate='<b>%{label}</b><br>Huella de carbono: %{value:.2f} kg CO₂e/ha·año<br>Porcentaje: %{percent}<extra></extra>'
            )
            # Configurar formato de números para el hover y texto
            fig_pie.update_layout(
                separators=',.'  # Formato español: coma decimal, punto miles
            )
        else:
            fig_pie = px.pie(names=["Sin datos"], values=[1], color_discrete_sequence=["#cccccc"])
        fig_pie.update_layout(showlegend=False, height=400, separators=',.')
        st.plotly_chart(fig_pie, use_container_width=True, key=get_unique_key())

    st.markdown("---")

    # --- Resultados por ciclo ---
    if emisiones_ciclos:
        st.markdown("#### Huella de carbono por ciclo productivo")
        df_ciclos = pd.DataFrame(emisiones_ciclos, columns=[
            "Ciclo",
            "Huella de carbono (kg CO₂e/ha·ciclo)",
            "Producción (kg/ha·ciclo)"
        ])
        df_ciclos["Nombre ciclo"] = ["Ciclo " + str(c) for c in df_ciclos["Ciclo"]]
        df_ciclos["Huella de carbono (kg CO₂e/kg fruta·ciclo)"] = df_ciclos.apply(
            lambda row: row["Huella de carbono (kg CO₂e/ha·ciclo)"] / row["Producción (kg/ha·ciclo)"] if row["Producción (kg/ha·ciclo)"] > 0 else None,
            axis=1
        )
        total_emisiones_ciclos = df_ciclos["Huella de carbono (kg CO₂e/ha·ciclo)"].sum()
        if total_emisiones_ciclos > 0:
            df_ciclos["% contribución"] = df_ciclos["Huella de carbono (kg CO₂e/ha·ciclo)"] / total_emisiones_ciclos * 100
        else:
            df_ciclos["% contribución"] = 0

        st.markdown("**Tabla: Huella de carbono y producción por ciclo**")
        st.dataframe(
            df_ciclos[[
                "Nombre ciclo",
                "Huella de carbono (kg CO₂e/ha·ciclo)",
                "Producción (kg/ha·ciclo)",
                "Huella de carbono (kg CO₂e/kg fruta·ciclo)",
                "% contribución"
            ]].style.format({
                "Huella de carbono (kg CO₂e/ha·ciclo)": format_num,
                "Producción (kg/ha·ciclo)": format_num,
                "Huella de carbono (kg CO₂e/kg fruta·ciclo)": lambda x: format_num(x, 3),
                "% contribución": format_percent
            }),
            hide_index=True
        )
        st.caption("Unidades: kg CO₂e/ha·ciclo, kg/ha·ciclo, kg CO₂e/kg fruta·ciclo, % sobre el total anual.")

        # Gráfico de barras por ciclo (kg CO₂e/ha)
        st.markdown("##### Gráfico: Huella de carbono por ciclo (kg CO₂e/ha·ciclo)")
        y_max_ciclo = df_ciclos["Huella de carbono (kg CO₂e/ha·ciclo)"].max() if not df_ciclos.empty else 1
        textos_ciclo = [format_num(v) for v in df_ciclos["Huella de carbono (kg CO₂e/ha·ciclo)"]]
        fig_ciclo = px.bar(
            df_ciclos,
            x="Nombre ciclo",
            y="Huella de carbono (kg CO₂e/ha·ciclo)",
            color="Nombre ciclo",
            color_discrete_sequence=px.colors.qualitative.Pastel,
            labels={"Huella de carbono (kg CO₂e/ha·ciclo)": "Huella de carbono (kg CO₂e/ha·ciclo)"},
            title="Huella de carbono por ciclo"
        )
        fig_ciclo.add_trace(go.Scatter(
            x=df_ciclos["Nombre ciclo"],
            y=df_ciclos["Huella de carbono (kg CO₂e/ha·ciclo)"],
            text=textos_ciclo,
            mode="text",
            textposition="top center",
            showlegend=False
        ))
        fig_ciclo.update_layout(showlegend=False, height=400, separators=',.')
        fig_ciclo.update_yaxes(range=[0, y_max_ciclo * 1.15])
        st.plotly_chart(fig_ciclo, use_container_width=True, key=get_unique_key())

    st.markdown("---")

    # --- Resultados por fuente en cada ciclo ---
    desglose_fuentes_ciclos = st.session_state.get("desglose_fuentes_ciclos", [])
    if desglose_fuentes_ciclos:
        st.markdown("#### Huella de carbono por fuente en cada ciclo")
        fuentes = ["Fertilizantes", "Agroquímicos", "Riego", "Maquinaria", "Residuos"]
        for idx, ciclo in enumerate(desglose_fuentes_ciclos):
            st.markdown(f"##### {'Ciclo ' + str(ciclo['Ciclo']) if 'Ciclo' in ciclo else 'Ciclo típico'}")
            prod = ciclo.get("Producción", None)
            if prod is None:
                prod = None
                for c in emisiones_ciclos:
                    if c[0] == ciclo.get("Ciclo"):
                        prod = c[2]
                        break
            total_fuente = sum([ciclo[f] for f in fuentes])
            df_fuentes_ciclo = pd.DataFrame({
                "Fuente": fuentes,
                "Huella de carbono (kg CO₂e/ha·ciclo)": [ciclo[f] for f in fuentes]
            })
            if prod and prod > 0:
                df_fuentes_ciclo["Huella de carbono (kg CO₂e/kg fruta·ciclo)"] = df_fuentes_ciclo["Huella de carbono (kg CO₂e/ha·ciclo)"] / prod
            else:
                df_fuentes_ciclo["Huella de carbono (kg CO₂e/kg fruta·ciclo)"] = None
            if total_fuente > 0:
                df_fuentes_ciclo["% contribución"] = df_fuentes_ciclo["Huella de carbono (kg CO₂e/ha·ciclo)"] / total_fuente * 100
            else:
                df_fuentes_ciclo["% contribución"] = 0

            st.dataframe(df_fuentes_ciclo.style.format({
                "Huella de carbono (kg CO₂e/ha·ciclo)": format_num,
                "Huella de carbono (kg CO₂e/kg fruta·ciclo)": lambda x: format_num(x, 3),
                "% contribución": format_percent
            }), hide_index=True)
            st.caption("Unidades: kg CO₂e/ha·ciclo, kg CO₂e/kg fruta·ciclo, % sobre el total del ciclo.")

            # Gráfico de barras por fuente en el ciclo (kg CO₂e/ha)
            st.markdown("##### Gráfico: Huella de carbono por fuente en el ciclo (kg CO₂e/ha·ciclo)")
            y_max_fuente = df_fuentes_ciclo["Huella de carbono (kg CO₂e/ha·ciclo)"].max() if not df_fuentes_ciclo.empty else 1
            textos_fuente = [format_num(v) for v in df_fuentes_ciclo["Huella de carbono (kg CO₂e/ha·ciclo)"]]
            fig_fuente = px.bar(
                df_fuentes_ciclo,
                x="Fuente",
                y="Huella de carbono (kg CO₂e/ha·ciclo)",
                color="Fuente",
                color_discrete_sequence=px.colors.qualitative.Set2,
                title="Huella de carbono por fuente en el ciclo"
            )
            fig_fuente.add_trace(go.Scatter(
                x=df_fuentes_ciclo["Fuente"],
                y=df_fuentes_ciclo["Huella de carbono (kg CO₂e/ha·ciclo)"],
                text=textos_fuente,
                mode="text",
                textposition="top center",
                showlegend=False
            ))
            fig_fuente.update_layout(showlegend=False, height=400, separators=',.')
            fig_fuente.update_yaxes(range=[0, y_max_fuente * 1.15])
            st.plotly_chart(fig_fuente, use_container_width=True, key=get_unique_key())

            # --- Desglose interno de cada fuente ---
            st.markdown("###### Desglose interno de cada fuente")
            fuentes_ordenadas = sorted(
                df_fuentes_ciclo["Fuente"],
                key=lambda f: ciclo.get(f, 0),
                reverse=True
            )
            for fuente in fuentes_ordenadas:
                valor = ciclo[fuente]
                if valor > 0:
                    st.markdown(f"**{fuente}**")
                    st.info(f"Explicación: {explicacion_fuente(fuente)}")
                    # --- FERTILIZANTES ---
                    if fuente == "Fertilizantes" and ciclo.get("desglose_fertilizantes"):
                        df_fert = pd.DataFrame(ciclo["desglose_fertilizantes"])
                        if not df_fert.empty:
                            df_fert["Tipo fertilizante"] = df_fert["tipo"].apply(
                                lambda x: "Orgánico" if "org" in str(x).lower() or "estiércol" in str(x).lower() or "guano" in str(x).lower() else "Inorgánico"
                            )
                            total_fert = df_fert["total"].sum()
                            df_fert["% contribución"] = df_fert["total"] / total_fert * 100
                            if prod and prod > 0:
                                df_fert["Huella de carbono total (kg CO₂e/kg fruta·ciclo)"] = df_fert["total"] / prod
                            else:
                                df_fert["Huella de carbono total (kg CO₂e/kg fruta·ciclo)"] = None
                            st.markdown("**Tabla: Desglose de fertilizantes (orgánicos e inorgánicos)**")
                            df_fert_display = df_fert.rename(columns={
                                "emision_produccion": "Huella de carbono producción (kg CO₂e/ha·ciclo)",
                                "emision_co2_urea": "Huella de carbono CO₂ urea (kg CO₂e/ha·ciclo)",
                                "emision_n2o_directa": "Huella de carbono N₂O directa (kg CO₂e/ha·ciclo)",
                                "emision_n2o_ind_volatilizacion": "Huella de carbono N₂O ind. volatilización (kg CO₂e/ha·ciclo)",
                                "emision_n2o_ind_lixiviacion": "Huella de carbono N₂O ind. lixiviación (kg CO₂e/ha·ciclo)",
                                "emision_n2o_indirecta": "Huella de carbono N₂O indirecta (kg CO₂e/ha·ciclo)",
                                "total": "Huella de carbono total (kg CO₂e/ha·ciclo)"
                            })
                            st.dataframe(
                                df_fert_display[[
                                    "Tipo fertilizante", "tipo", "cantidad", "Huella de carbono producción (kg CO₂e/ha·ciclo)", "Huella de carbono CO₂ urea (kg CO₂e/ha·ciclo)",
                                    "Huella de carbono N₂O directa (kg CO₂e/ha·ciclo)", "Huella de carbono N₂O ind. volatilización (kg CO₂e/ha·ciclo)", "Huella de carbono N₂O ind. lixiviación (kg CO₂e/ha·ciclo)",
                                    "Huella de carbono N₂O indirecta (kg CO₂e/ha·ciclo)", "Huella de carbono total (kg CO₂e/ha·ciclo)", "Huella de carbono total (kg CO₂e/kg fruta·ciclo)", "% contribución"
                                ]].style.format({
                                    "cantidad": format_num,
                                    "Huella de carbono producción (kg CO₂e/ha·ciclo)": format_num,
                                    "Huella de carbono CO₂ urea (kg CO₂e/ha·ciclo)": format_num,
                                    "Huella de carbono N₂O directa (kg CO₂e/ha·ciclo)": format_num,
                                    "Huella de carbono N₂O ind. volatilización (kg CO₂e/ha·ciclo)": format_num,
                                    "Huella de carbono N₂O ind. lixiviación (kg CO₂e/ha·ciclo)": format_num,
                                    "Huella de carbono N₂O indirecta (kg CO₂e/ha·ciclo)": format_num,
                                    "Huella de carbono total (kg CO₂e/ha·ciclo)": format_num,
                                    "Huella de carbono total (kg CO₂e/kg fruta·ciclo)": lambda x: format_num(x, 3),
                                    "% contribución": format_percent
                                }),
                                hide_index=True
                            )
                            st.caption("Unidades: cantidad (kg/ha·ciclo), huella de carbono (kg CO₂e/ha·ciclo), % sobre el total de fertilizantes. N₂O indirecta se desglosa en volatilización y lixiviación. CO₂ urea incluye hidrólisis según IPCC 2006.")
                            
                            # --- NUEVO: Gráfico de torta Orgánicos vs Inorgánicos ---
                            st.markdown("**Gráfico: Contribución orgánicos vs inorgánicos (torta)**")
                            df_resumen_tipo = df_fert.groupby("Tipo fertilizante")["total"].sum().reset_index()
                            if len(df_resumen_tipo) > 0:
                                fig_pie_tipo = px.pie(
                                    values=df_resumen_tipo["total"],
                                    names=df_resumen_tipo["Tipo fertilizante"],
                                    title="Contribución orgánicos vs inorgánicos",
                                    color_discrete_sequence=["#66c2a5", "#fc8d62"],
                                    hole=0.3
                                )
                                # Configurar formato español para nombres y porcentajes
                                fig_pie_tipo.update_traces(
                                    textinfo='label+percent',
                                    texttemplate='%{label}<br>%{percent}',
                                    hovertemplate='<b>%{label}</b><br>Huella de carbono: %{value:.2f} kg CO₂e/ha·ciclo<br>Porcentaje: %{percent}<extra></extra>'
                                )
                                fig_pie_tipo.update_layout(
                                    showlegend=True, 
                                    height=400,
                                    separators=',.'  # Formato español
                                )
                                st.plotly_chart(fig_pie_tipo, use_container_width=True, key=get_unique_key())
                            
                            # --- NUEVO: Gráficos de torta por cada tipo de fertilizante ---
                            for tipo_cat in ["Orgánico", "Inorgánico"]:
                                df_tipo_pie = df_fert[df_fert["Tipo fertilizante"] == tipo_cat]
                                if not df_tipo_pie.empty and len(df_tipo_pie) > 1:  # Solo si hay más de un fertilizante del tipo
                                    st.markdown(f"**Gráfico: Contribución de cada fertilizante {tipo_cat.lower()} (torta)**")
                                    # Crear etiquetas únicas para fertilizantes duplicados
                                    tipo_counts = {}
                                    etiquetas_unicas = []
                                    for _, row in df_tipo_pie.iterrows():
                                        tipo_base = row["tipo"]
                                        if tipo_base in tipo_counts:
                                            tipo_counts[tipo_base] += 1
                                            etiquetas_unicas.append(f"{tipo_base} ({tipo_counts[tipo_base]})")
                                        else:
                                            tipo_counts[tipo_base] = 1
                                            etiquetas_unicas.append(tipo_base)
                                    
                                    fig_pie_individual = px.pie(
                                        values=df_tipo_pie["total"],
                                        names=etiquetas_unicas,
                                        title=f"Contribución de cada fertilizante {tipo_cat.lower()}",
                                        hole=0.3
                                    )
                                    # Configurar formato español para nombres y porcentajes
                                    fig_pie_individual.update_traces(
                                        textinfo='label+percent',
                                        texttemplate='%{label}<br>%{percent}',
                                        hovertemplate='<b>%{label}</b><br>Huella de carbono: %{value:.2f} kg CO₂e/ha·ciclo<br>Porcentaje: %{percent}<extra></extra>'
                                    )
                                    fig_pie_individual.update_layout(
                                        showlegend=True, 
                                        height=400,
                                        separators=',.'  # Formato español
                                    )
                                    st.plotly_chart(fig_pie_individual, use_container_width=True, key=get_unique_key())
                            
                            # --- Gráficos de barras apiladas por tipo de emisión (orgánico e inorgánico por separado) ---
                            for tipo_cat in ["Orgánico", "Inorgánico"]:
                                df_tipo = df_fert[df_fert["Tipo fertilizante"] == tipo_cat]
                                if not df_tipo.empty:
                                    st.markdown(f"**Gráfico: Emisiones por fertilizante {tipo_cat.lower()} y tipo de emisión (kg CO₂e/ha·ciclo)**")
                                    
                                    # Crear etiquetas únicas para fertilizantes duplicados en gráficos de barras
                                    tipo_counts = {}
                                    etiquetas_unicas = []
                                    for _, row in df_tipo.iterrows():
                                        tipo_base = row["tipo"]
                                        if tipo_base in tipo_counts:
                                            tipo_counts[tipo_base] += 1
                                            etiquetas_unicas.append(f"{tipo_base} ({tipo_counts[tipo_base]})")
                                        else:
                                            tipo_counts[tipo_base] = 1
                                            etiquetas_unicas.append(tipo_base)
                                    
                                    labels = etiquetas_unicas
                                    em_prod = df_tipo["emision_produccion"].values
                                    em_co2_urea = df_tipo["emision_co2_urea"].values
                                    em_n2o_dir = df_tipo["emision_n2o_directa"].values
                                    em_n2o_ind_vol = df_tipo["emision_n2o_ind_volatilizacion"].values
                                    em_n2o_ind_lix = df_tipo["emision_n2o_ind_lixiviacion"].values
                                    fig_fert = go.Figure()
                                    fig_fert.add_bar(x=labels, y=em_prod, name="Producción")
                                    fig_fert.add_bar(x=labels, y=em_co2_urea, name="CO₂ hidrólisis urea")
                                    fig_fert.add_bar(x=labels, y=em_n2o_dir, name="N₂O directa")
                                    fig_fert.add_bar(x=labels, y=em_n2o_ind_vol, name="N₂O indirecta (volatilización)")
                                    fig_fert.add_bar(x=labels, y=em_n2o_ind_lix, name="N₂O indirecta (lixiviación)")
                                    totales = em_prod + em_co2_urea + em_n2o_dir + em_n2o_ind_vol + em_n2o_ind_lix
                                    textos_tot = [format_num(v) for v in totales]
                                    fig_fert.add_trace(go.Scatter(
                                        x=labels,
                                        y=totales,
                                        text=textos_tot,
                                        mode="text",
                                        textposition="top center",
                                        showlegend=False
                                    ))
                                    fig_fert.update_layout(
                                        barmode='stack',
                                        yaxis_title="Huella de carbono (kg CO₂e/ha·ciclo)",
                                        title=f"Huella de carbono por fertilizante {tipo_cat.lower()} y tipo de emisión",
                                        height=400,
                                        separators=',.'  # Formato español
                                    )
                                    fig_fert.update_yaxes(range=[0, max(totales) * 1.15 if len(totales) > 0 else 1])
                                    st.plotly_chart(fig_fert, use_container_width=True, key=get_unique_key())
                    # --- AGROQUÍMICOS ---
                    elif fuente == "Agroquímicos" and ciclo.get("desglose_agroquimicos"):
                        df_agro = pd.DataFrame(ciclo["desglose_agroquimicos"])
                        if not df_agro.empty:
                            total_agro = df_agro["emisiones"].sum()
                            df_agro["% contribución"] = df_agro["emisiones"] / total_agro * 100
                            if prod and prod > 0:
                                df_agro["Huella de carbono (kg CO₂e/kg fruta·ciclo)"] = df_agro["emisiones"] / prod
                            else:
                                df_agro["Huella de carbono (kg CO₂e/kg fruta·ciclo)"] = None
                            # Renombrar columna para mostrar en tabla
                            df_agro["Huella de carbono (kg CO₂e/ha·ciclo)"] = df_agro["emisiones"]
                            st.markdown("**Tabla: Desglose de agroquímicos**")
                            st.dataframe(df_agro[["nombre_comercial", "categoria", "tipo", "cantidad_ia", "Huella de carbono (kg CO₂e/ha·ciclo)", "Huella de carbono (kg CO₂e/kg fruta·ciclo)", "% contribución"]].style.format({
                                "cantidad_ia": format_num,
                                "Huella de carbono (kg CO₂e/ha·ciclo)": format_num,
                                "Huella de carbono (kg CO₂e/kg fruta·ciclo)": lambda x: format_num(x, 3),
                                "% contribución": format_percent
                            }), hide_index=True)
                            st.caption("Unidades: cantidad ingrediente activo (kg/ha·ciclo), huella de carbono (kg CO₂e/ha·ciclo y kg CO₂e/kg fruta·ciclo), % sobre el total de agroquímicos.")

                            # --- Gráfico de barras por nombre comercial (kg CO₂e/ha) ---
                            st.markdown("**Gráfico: Emisiones de agroquímicos por nombre comercial (kg CO₂e/ha·ciclo)**")
                            # Agrupar por categoría para crear las barras
                            categorias = df_agro["categoria"].unique()
                            fig_agro = go.Figure()
                            
                            for categoria in categorias:
                                df_cat = df_agro[df_agro["categoria"] == categoria]
                                fig_agro.add_bar(
                                    x=df_cat["nombre_comercial"], 
                                    y=df_cat["emisiones"], 
                                    name=categoria,
                                    text=[format_num(v) for v in df_cat["emisiones"]],
                                    textposition="outside"
                                )
                            
                            fig_agro.update_layout(
                                barmode='group',
                                yaxis_title="Huella de carbono (kg CO₂e/ha·ciclo)",
                                title="Huella de carbono de agroquímicos por nombre comercial",
                                height=400,
                                separators=',.',  # Formato español
                                xaxis_title="Nombre comercial"
                            )
                            y_max_agro = df_agro["emisiones"].max() if not df_agro.empty else 1
                            fig_agro.update_yaxes(range=[0, y_max_agro * 1.15])
                            st.plotly_chart(fig_agro, use_container_width=True, key=get_unique_key())

                            # --- Gráfico de torta por categoría (kg CO₂e/ha) ---
                            st.markdown("**Gráfico: % de contribución por categoría de agroquímico (kg CO₂e/ha·ciclo)**")
                            df_cat = df_agro.groupby("categoria").agg({"emisiones": "sum"}).reset_index()
                            fig_pie_cat = px.pie(
                                df_cat,
                                names="categoria",
                                values="emisiones",
                                title="Contribución por categoría de agroquímico",
                                color_discrete_sequence=px.colors.qualitative.Set1,
                                hole=0.3
                            )
                            # Configurar formato español para nombres y porcentajes
                            fig_pie_cat.update_traces(
                                textinfo='label+percent',
                                texttemplate='%{label}<br>%{percent}',
                                hovertemplate='<b>%{label}</b><br>Huella de carbono: %{value:.2f} kg CO₂e/ha·ciclo<br>Porcentaje: %{percent}<extra></extra>'
                            )
                            fig_pie_cat.update_layout(
                                showlegend=True, 
                                height=400,
                                separators=',.'  # Formato español
                            )
                            st.plotly_chart(fig_pie_cat, use_container_width=True, key=get_unique_key())

                            # --- Gráfico de torta por nombre comercial individual (kg CO₂e/ha) ---
                            st.markdown("**Gráfico: % de contribución de cada agroquímico individual (kg CO₂e/ha·ciclo)**")
                            fig_pie_agro = px.pie(
                                df_agro,
                                names="nombre_comercial",
                                values="emisiones",
                                title="Contribución individual de cada agroquímico",
                                color_discrete_sequence=px.colors.qualitative.Set2,
                                hole=0.3
                            )
                            # Configurar formato español para nombres y porcentajes
                            fig_pie_agro.update_traces(
                                textinfo='label+percent',
                                texttemplate='%{label}<br>%{percent}',
                                hovertemplate='<b>%{label}</b><br>Categoría: %{customdata}<br>Huella de carbono: %{value:.2f} kg CO₂e/ha·ciclo<br>Porcentaje: %{percent}<extra></extra>',
                                customdata=df_agro["categoria"]
                            )
                            fig_pie_agro.update_layout(
                                showlegend=True, 
                                height=400,
                                separators=',.'  # Formato español
                            )
                            st.plotly_chart(fig_pie_agro, use_container_width=True, key=get_unique_key())
                    # --- MAQUINARIA ---
                    elif fuente == "Maquinaria" and ciclo.get("desglose_maquinaria"):
                        df_maq = pd.DataFrame(ciclo["desglose_maquinaria"])
                        if not df_maq.empty:
                            total_maq = df_maq["emisiones"].sum()
                            df_maq["% contribución"] = df_maq["emisiones"] / total_maq * 100
                            if prod and prod > 0:
                                df_maq["Huella de carbono (kg CO₂e/kg fruta·ciclo)"] = df_maq["emisiones"] / prod
                            else:
                                df_maq["Huella de carbono (kg CO₂e/kg fruta·ciclo)"] = None
                            # Renombrar columna para mostrar en tabla
                            df_maq["Huella de carbono (kg CO₂e/ha·ciclo)"] = df_maq["emisiones"]
                            st.markdown("**Tabla: Desglose de maquinaria**")
                            st.dataframe(df_maq[["nombre_labor", "tipo_maquinaria", "tipo_combustible", "litros", "Huella de carbono (kg CO₂e/ha·ciclo)", "Huella de carbono (kg CO₂e/kg fruta·ciclo)", "% contribución"]].style.format({
                                "litros": format_num,
                                "Huella de carbono (kg CO₂e/ha·ciclo)": format_num,
                                "Huella de carbono (kg CO₂e/kg fruta·ciclo)": lambda x: format_num(x, 3),
                                "% contribución": format_percent
                            }), hide_index=True)
                            st.caption("Unidades: litros (L/ha·ciclo), huella de carbono (kg CO₂e/ha·ciclo y kg CO₂e/kg fruta·ciclo), % sobre el total de maquinaria.")

                            # --- Gráfico de torta: emisiones por labor (kg CO₂e/ha) ---
                            st.markdown("**Gráfico: % de contribución de cada labor (torta, kg CO₂e/ha·ciclo)**")
                            df_labor = df_maq.groupby("nombre_labor")["emisiones"].sum().reset_index()
                            fig_pie_labor = px.pie(
                                df_labor,
                                names="nombre_labor",
                                values="emisiones",
                                title="Contribución de cada labor al total de emisiones de maquinaria",
                                color_discrete_sequence=px.colors.qualitative.Set2,
                                hole=0.3
                            )
                            # Configurar formato español para nombres y porcentajes
                            fig_pie_labor.update_traces(
                                textinfo='label+percent',
                                texttemplate='%{label}<br>%{percent}',
                                hovertemplate='<b>%{label}</b><br>Huella de carbono: %{value:.2f} kg CO₂e/ha·ciclo<br>Porcentaje: %{percent}<extra></extra>'
                            )
                            fig_pie_labor.update_layout(
                                showlegend=True, 
                                height=400,
                                separators=',.'  # Formato español
                            )
                            st.plotly_chart(fig_pie_labor, use_container_width=True, key=get_unique_key())

                            # --- Gráfico de torta: emisiones por maquinaria dentro de cada labor (kg CO₂e/ha) ---
                            labores_unicas = df_maq["nombre_labor"].unique()
                            for labor in labores_unicas:
                                df_labor_maq = df_maq[df_maq["nombre_labor"] == labor]
                                if len(df_labor_maq) > 1:
                                    st.markdown(f"**Gráfico: % de contribución de cada maquinaria en la labor '{labor}' (torta, kg CO₂e/ha·ciclo)**")
                                    fig_pie_maq = px.pie(
                                        df_labor_maq,
                                        names="tipo_maquinaria",
                                        values="emisiones",
                                        title=f"Contribución de cada maquinaria en la labor '{labor}'",
                                        color_discrete_sequence=px.colors.qualitative.Pastel,
                                        hole=0.3
                                    )
                                    # Configurar formato español para nombres y porcentajes
                                    fig_pie_maq.update_traces(
                                        textinfo='label+percent',
                                        texttemplate='%{label}<br>%{percent}',
                                        hovertemplate='<b>%{label}</b><br>Huella de carbono: %{value:.2f} kg CO₂e/ha·ciclo<br>Porcentaje: %{percent}<extra></extra>'
                                    )
                                    fig_pie_maq.update_layout(
                                        showlegend=True, 
                                        height=400,
                                        separators=',.'  # Formato español
                                    )
                                    st.plotly_chart(fig_pie_maq, use_container_width=True, key=get_unique_key())

                            # --- Gráfico de barras apiladas: labor (X), emisiones (Y), apilado por maquinaria (kg CO₂e/ha) ---
                            st.markdown("**Gráfico: Emisiones por labor y tipo de maquinaria (barras apiladas, kg CO₂e/ha·ciclo)**")
                            df_maq_grouped = df_maq.groupby(["nombre_labor", "tipo_maquinaria"]).agg({"emisiones": "sum"}).reset_index()
                            labores = df_maq_grouped["nombre_labor"].unique()
                            tipos_maq = df_maq_grouped["tipo_maquinaria"].unique()
                            fig_maq = go.Figure()
                            for maq in tipos_maq:
                                vals = []
                                for l in labores:
                                    row = df_maq_grouped[(df_maq_grouped["nombre_labor"] == l) & (df_maq_grouped["tipo_maquinaria"] == maq)]
                                    vals.append(row["emisiones"].values[0] if not row.empty else 0)
                                fig_maq.add_bar(
                                    x=labores,
                                    y=vals,
                                    name=maq
                                )
                            totales = df_maq_grouped.groupby("nombre_labor")["emisiones"].sum().reindex(labores).values
                            textos_tot = [format_num(v) for v in totales]
                            fig_maq.add_trace(go.Scatter(
                                x=labores,
                                y=totales,
                                text=textos_tot,
                                mode="text",
                                textposition="top center",
                                showlegend=False
                            ))
                            y_max_maq = max(totales) if len(totales) > 0 else 1
                            fig_maq.update_layout(
                                barmode='stack',
                                yaxis_title="Huella de carbono (kg CO₂e/ha·ciclo)",
                                title="Huella de carbono por labor y tipo de maquinaria",
                                height=400,
                                separators=',.'  # Formato español
                            )
                            fig_maq.update_yaxes(range=[0, y_max_maq * 1.15])
                            st.plotly_chart(fig_maq, use_container_width=True, key=get_unique_key())
                    # --- RIEGO ---
                    elif fuente == "Riego" and ciclo.get("desglose_riego"):
                        dr = ciclo["desglose_riego"]
                        energia_actividades = dr.get("energia_actividades", [])
                        actividades = []
                        for ea in energia_actividades:
                            actividades.append({
                                "Actividad": ea.get("actividad", ""),
                                "Tipo actividad": ea.get("tipo_actividad", ""),
                                "Consumo agua (m³)": ea.get("agua_total_m3", 0),
                                "Huella de carbono agua (kg CO₂e/ha·ciclo)": ea.get("emisiones_agua", 0),
                                "Consumo energía": ea.get("consumo_energia", 0),
                                "Tipo energía": ea.get("tipo_energia", ""),
                                "Huella de carbono energía (kg CO₂e/ha·ciclo)": ea.get("emisiones_energia", 0),
                            })
                        if actividades:
                            df_riego = pd.DataFrame(actividades)
                            df_riego["Huella de carbono total (kg CO₂e/ha·ciclo)"] = df_riego["Huella de carbono agua (kg CO₂e/ha·ciclo)"] + df_riego["Huella de carbono energía (kg CO₂e/ha·ciclo)"]
                            if prod and prod > 0:
                                df_riego["Huella de carbono total (kg CO₂e/kg fruta·ciclo)"] = df_riego["Huella de carbono total (kg CO₂e/ha·ciclo)"] / prod
                            else:
                                df_riego["Huella de carbono total (kg CO₂e/kg fruta·ciclo)"] = None
                            total_riego = df_riego["Huella de carbono total (kg CO₂e/ha·ciclo)"].sum()
                            if total_riego > 0:
                                df_riego["% contribución"] = df_riego["Huella de carbono total (kg CO₂e/ha·ciclo)"] / total_riego * 100
                            else:
                                df_riego["% contribución"] = 0
                            st.markdown("**Tabla: Desglose de riego por actividad (agua y energía apilados)**")
                            st.dataframe(df_riego[[
                                "Actividad", "Tipo actividad", "Consumo agua (m³)", "Huella de carbono agua (kg CO₂e/ha·ciclo)",
                                "Consumo energía", "Tipo energía", "Huella de carbono energía (kg CO₂e/ha·ciclo)",
                                "Huella de carbono total (kg CO₂e/ha·ciclo)", "Huella de carbono total (kg CO₂e/kg fruta·ciclo)", "% contribución"
                            ]].style.format({
                                "Consumo agua (m³)": format_num,
                                "Huella de carbono agua (kg CO₂e/ha·ciclo)": format_num,
                                "Consumo energía": format_num,
                                "Huella de carbono energía (kg CO₂e/ha·ciclo)": format_num,
                                "Huella de carbono total (kg CO₂e/ha·ciclo)": format_num,
                                "Huella de carbono total (kg CO₂e/kg fruta·ciclo)": lambda x: format_num(x, 3),
                                "% contribución": format_percent
                            }), hide_index=True)
                            st.caption("Unidades: agua (m³/ha), energía (kWh o litros/ha), huella de carbono (kg CO₂e/ha y kg CO₂e/kg fruta), % sobre el total de riego.")
                            # Gráfico de barras apiladas por actividad (agua + energía)
                            st.markdown("**Gráfico: Huella de carbono de riego por actividad (barras apiladas agua + energía, kg CO₂e/ha·ciclo)**")
                            fig_riego = go.Figure()
                            fig_riego.add_bar(
                                x=df_riego["Actividad"],
                                y=df_riego["Huella de carbono agua (kg CO₂e/ha·ciclo)"],
                                name="Agua",
                                marker_color="#4fc3f7"
                            )
                            fig_riego.add_bar(
                                x=df_riego["Actividad"],
                                y=df_riego["Huella de carbono energía (kg CO₂e/ha·ciclo)"],
                                name="Energía",
                                marker_color="#0288d1"
                            )
                            totales = df_riego["Huella de carbono total (kg CO₂e/ha·ciclo)"].values
                            textos_tot = [format_num(v) for v in totales]
                            fig_riego.add_trace(go.Scatter(
                                x=df_riego["Actividad"],
                                y=totales,
                                text=textos_tot,
                                mode="text",
                                textposition="top center",
                                showlegend=False
                            ))
                            y_max_riego = max(totales) if len(totales) > 0 else 1
                            fig_riego.update_layout(
                                barmode='stack',
                                yaxis_title="Huella de carbono (kg CO₂e/ha)",
                                title="Huella de carbono de riego por actividad (agua + energía)",
                                height=400,
                                separators=',.'  # Formato español
                            )
                            fig_riego.update_yaxes(range=[0, y_max_riego * 1.15])
                            st.plotly_chart(fig_riego, use_container_width=True, key=get_unique_key())

                            # --- Gráficos de torta por actividad individual: contribución agua vs energía ---
                            actividades_unicas = df_riego["Actividad"].unique()
                            for actividad in actividades_unicas:
                                df_act = df_riego[df_riego["Actividad"] == actividad]
                                if len(df_act) == 1:  # Una sola fila por actividad
                                    row = df_act.iloc[0]
                                    em_agua = row["Huella de carbono agua (kg CO₂e/ha·ciclo)"]
                                    em_energia = row["Huella de carbono energía (kg CO₂e/ha·ciclo)"]
                                    total_act = em_agua + em_energia
                                    
                                    # Solo crear gráfico si hay emisiones totales > 0
                                    if total_act > 0:
                                        st.markdown(f"**Gráfico: Contribución agua vs energía en la actividad '{actividad}' (torta, kg CO₂e/ha·ciclo)**")
                                        
                                        # Datos para el gráfico de torta
                                        labels = []
                                        values = []
                                        if em_agua > 0:
                                            labels.append("Agua")
                                            values.append(em_agua)
                                        if em_energia > 0:
                                            labels.append("Energía")
                                            values.append(em_energia)
                                        
                                        if len(values) > 0:
                                            fig_pie_act = px.pie(
                                                values=values,
                                                names=labels,
                                                title=f"Contribución agua vs energía en '{actividad}'",
                                                color_discrete_sequence=["#4fc3f7", "#0288d1"],
                                                hole=0.3
                                            )
                                            # Configurar formato español para nombres y porcentajes
                                            fig_pie_act.update_traces(
                                                textinfo='label+percent',
                                                texttemplate='%{label}<br>%{percent}',
                                                hovertemplate='<b>%{label}</b><br>Huella de carbono: %{value:.2f} kg CO₂e/ha·ciclo<br>Porcentaje: %{percent}<extra></extra>'
                                            )
                                            fig_pie_act.update_layout(
                                                showlegend=True, 
                                                height=400,
                                                separators=',.'  # Formato español
                                            )
                                            st.plotly_chart(fig_pie_act, use_container_width=True, key=get_unique_key())
                                        else:
                                            st.info(f"La actividad '{actividad}' no tiene huella de carbono de agua ni energía.")
                                    else:
                                        st.info(f"La actividad '{actividad}' no tiene huella de carbono total.")
                        else:
                            st.info("No se ingresaron actividades de riego para este ciclo.")
                    # --- RESIDUOS ---
                    elif fuente == "Residuos" and ciclo.get("desglose_residuos"):
                        dr = ciclo["desglose_residuos"]
                        if isinstance(dr, dict) and dr:
                            df_res = pd.DataFrame([
                                {
                                    "Gestión": k,
                                    "Biomasa (kg/ha·ciclo)": v.get("biomasa", 0),
                                    "Huella de carbono (kg CO₂e/ha·ciclo)": v.get("emisiones", 0),
                                    "Huella de carbono (kg CO₂e/kg fruta·ciclo)": v.get("emisiones", 0) / prod if prod and prod > 0 else None
                                }
                                for k, v in dr.items()
                            ])
                            total_res = df_res["Huella de carbono (kg CO₂e/ha·ciclo)"].sum()
                            df_res["% contribución"] = df_res["Huella de carbono (kg CO₂e/ha·ciclo)"] / total_res * 100
                            st.markdown("**Tabla: Desglose de gestión de residuos vegetales**")
                            st.dataframe(df_res[[
                                "Gestión", "Biomasa (kg/ha·ciclo)", "Huella de carbono (kg CO₂e/ha·ciclo)", "Huella de carbono (kg CO₂e/kg fruta·ciclo)", "% contribución"
                            ]].style.format({
                                "Biomasa (kg/ha·ciclo)": format_num,
                                "Huella de carbono (kg CO₂e/ha·ciclo)": format_num,
                                "Huella de carbono (kg CO₂e/kg fruta·ciclo)": lambda x: format_num(x, 3),
                                "% contribución": format_percent
                            }), hide_index=True)
                            st.caption("Unidades: biomasa (kg/ha·ciclo), huella de carbono (kg CO₂e/ha·ciclo y kg CO₂e/kg fruta·ciclo), % sobre el total de residuos.")
                            textos_res = [format_num(v) for v in df_res["Huella de carbono (kg CO₂e/ha·ciclo)"]]
                            fig_res = px.bar(
                                df_res,
                                x="Gestión",
                                y="Huella de carbono (kg CO₂e/ha·ciclo)",
                                color="Gestión",
                                color_discrete_sequence=px.colors.qualitative.Pastel,
                                title="Huella de carbono por gestión de residuos"
                            )
                            fig_res.add_trace(go.Scatter(
                                x=df_res["Gestión"],
                                y=df_res["Huella de carbono (kg CO₂e/ha·ciclo)"],
                                text=textos_res,
                                mode="text",
                                textposition="top center",
                                showlegend=False
                            ))
                            fig_res.update_layout(showlegend=False, height=400, separators=',.')
                            fig_res.update_yaxes(range=[0, max(df_res["Huella de carbono (kg CO₂e/ha·ciclo)"]) * 1.15 if not df_res.empty else 1])
                            st.plotly_chart(fig_res, use_container_width=True, key=get_unique_key())
            st.markdown("---")

    # --- Resumen ejecutivo ---
    st.markdown("#### Resumen ejecutivo")
    st.success(
        "📝 **Resumen ejecutivo:**\n\n"
        "La huella de carbono total estimada para el sistema productivo corresponde a la suma de todas las fuentes de emisión y ciclos considerados, expresadas en **kg CO₂e/ha·año** y **kg CO₂e/kg fruta·año**. "
        "Este valor representa las emisiones acumuladas a lo largo de todos los ciclos productivos del año agrícola.\n\n"
        f"**Huella de carbono total estimada por hectárea:** {format_num(em_total, 2)} kg CO₂e/ha·año"
        + (
            f"\n\n**Huella de carbono por kg de fruta:** {format_num(em_total/prod_total, 3)} kg CO₂e/kg fruta.\n\n"
            "Este indicador permite comparar la huella de carbono entre diferentes sistemas o productos, ya que relaciona las emisiones totales con la producción obtenida en el año."
            if prod_total > 0 else "\n\nNo se ha ingresado producción total. No es posible calcular huella de carbono por kg de fruta."
        )
    )

    st.markdown("---")
    st.markdown("#### Parámetros de cálculo")
    st.write(f"Potenciales de calentamiento global (GWP) usados: {GWP}")
    st.write("Factores de emisión y fórmulas según IPCC 2006 y valores configurables al inicio del código.")

    # Guardar resultados globales y desgloses en session_state para exportación futura
    st.session_state["resultados_globales"] = {
        "tipo": "anual",
        "em_total": em_total,
        "prod_total": prod_total,
        "emisiones_ciclos": st.session_state.get("emisiones_ciclos", []),
        "desglose_fuentes_ciclos": st.session_state.get("desglose_fuentes_ciclos", []),
        "detalle_residuos": st.session_state.get("detalle_residuos", []),
        "emisiones_fuentes": emisiones_fuentes.copy(),
        "emisiones_etapas": emisiones_etapas.copy(),
        "produccion_etapas": produccion_etapas.copy(),
        "emisiones_fuente_etapa": emisiones_fuente_etapa.copy()
    }

# =============================================================================
# BOTÓN DE GUARDADO MANUAL EN RESULTADOS
# =============================================================================

def mostrar_boton_guardado_manual():
    """Muestra un botón para guardar manualmente los resultados"""
    
    # =========================================================================
    # VERIFICAR MODO VISUALIZACIÓN
    # =========================================================================
    if st.session_state.get('modo_visualizacion', False):
        # MODO VISUALIZACIÓN: NO mostrar botón de guardado
        # Solo mostrar información del proyecto guardado
        st.markdown("---")
        st.markdown("### ✅ Proyecto Guardado")
        
        with st.expander("📋 Información del proyecto", expanded=False):
            st.success("Este proyecto ya está guardado en la nube.")
            
            # Mostrar información básica
            st.write(f"**Nombre:** {st.session_state.get('current_project_name', 'Sin nombre')}")
            st.write(f"**Usuario:** {st.session_state.get('current_user_email', 'Desconocido')}")
            
            ultimo_guardado = st.session_state.get('ultimo_guardado')
            if ultimo_guardado:
                st.write(f"**Último guardado:** {ultimo_guardado}")
            
            # Botón para crear nueva versión
            if st.button("🆕 Crear nueva versión (copia editable)", use_container_width=True):
                # Crear copia como nuevo proyecto local
                st.session_state.current_project_id = f"local_{uuid.uuid4()}"
                st.session_state.current_project_name = f"{st.session_state.current_project_name} - Copia"
                st.session_state.modo_visualizacion = False  # Volver a modo edición
                st.session_state.guardado_pendiente = True
                
                # Limpiar resultados temporales
                st.session_state.em_total = 0
                st.session_state.prod_total = 0
                
                st.rerun()
        
        return  # Salir de la función
    
    # =========================================================================
    # MODO EDICIÓN: Mostrar botón de guardado normal
    # =========================================================================
    st.markdown("---")
    st.markdown("### 💾 Guardar Resultados")
    
    with st.expander("📤 Opciones de guardado", expanded=False):
        col1, col2, col3 = st.columns(3)  # Cambiar a 3 columnas
        
        with col1:
            # Botón para guardar en Supabase
            if st.session_state.get('supabase') and st.session_state.get('current_project_id'):
                if st.button("✅ Guardar en la nube", use_container_width=True, type="primary"):
                    if guardar_proyecto_completo():
                        st.success("✅ Resultados guardados exitosamente en Supabase")
                        # NO hacer st.rerun() aquí
                    else:
                        st.error("❌ Error al guardar en la nube")
            else:
                st.warning("⚠️ No hay conexión con la nube")
                if st.button("🔗 Reconectar", use_container_width=True, type="secondary"):
                    st.session_state.supabase = init_supabase_connection()
                    st.rerun()
        
        with col2:
            # Botón para exportar a Excel/PDF
            if st.button("📄 Exportar reporte", use_container_width=True, type="secondary"):
                st.info("Funcionalidad de exportación en desarrollo")
        
        with col3:
            # Botón para recargar manualmente (solo si es necesario)
            if st.session_state.get('necesita_recarga', False):
                if st.button("🔄 Recargar página", use_container_width=True, type="secondary"):
                    st.session_state.necesita_recarga = False
                    st.rerun()
        
        # Indicador de estado
        ultimo_guardado = st.session_state.get('ultimo_guardado')
        if ultimo_guardado:
            st.caption(f"Último guardado: {ultimo_guardado}")
        elif st.session_state.get('datos_pendientes_guardar', False):
            st.warning("⚠️ Hay cambios sin guardar")
        
        # Mostrar si hay resultados temporales guardados
        if 'resultados_temporales' in st.session_state:
            st.info("💡 Hay resultados temporales guardados. Los cálculos se mantendrán.")

###################################################
# RESULTADOS PARA CULTIVO PERENNE
###################################################

def mostrar_resultados_perenne(em_total, prod_total):
    st.header("Resultados Finales")
    st.info(
        "En esta sección se presentan los resultados globales y desglosados del cálculo de huella de carbono para el cultivo perenne. "
        "Se muestran los resultados globales del sistema productivo, el detalle por etapa y por fuente de emisión, "
        "y finalmente el desglose interno de cada fuente. Todas las tablas muestran emisiones en kg CO₂e/ha y kg CO₂e/kg fruta. "
        "Todos los gráficos muestran emisiones en kg CO₂e/ha."
    )

    def limpiar_nombre(etapa):
        return etapa.replace("3.1 ", "").replace("3.2 ", "").replace("3.3 ", "").replace("3. ", "").strip()

    # --- USAR VARIABLES DE SESSION_STATE - VERSIÓN CORREGIDA ---
    # Usar directamente las variables de session_state
    emisiones_fuentes = st.session_state.emisiones_fuentes
    emisiones_etapas = st.session_state.emisiones_etapas
    produccion_etapas = st.session_state.produccion_etapas
    emisiones_fuente_etapa = st.session_state.emisiones_fuente_etapa

    # --- RECONSTRUCCIÓN CORRECTA DE TOTALES GLOBALES DESDE EL DESGLOSE ---
    fuentes = ["Fertilizantes", "Agroquímicos", "Riego", "Maquinaria", "Residuos"]
    etapas_ordenadas = []
    
    # Reconstruir el orden de etapas
    for clave in emisiones_etapas:
        if clave.lower().startswith("implantación"):
            etapas_ordenadas.append(clave)
    for clave in emisiones_etapas:
        if "crecimiento sin producción" in clave.lower():
            etapas_ordenadas.append(clave)
    for clave in emisiones_etapas:
        if clave not in etapas_ordenadas:
            etapas_ordenadas.append(clave)

    # Sumar emisiones por fuente a partir de los desgloses de cada etapa
    emisiones_fuentes_reales = {f: 0 for f in fuentes}
    for etapa in etapas_ordenadas:
        fuente_etapa = emisiones_fuente_etapa.get(etapa, {})
        for f in fuentes:
            emisiones_fuentes_reales[f] += fuente_etapa.get(f, 0)
    
    # Actualiza los acumuladores globales
    for f in fuentes:
        emisiones_fuentes[f] = emisiones_fuentes_reales[f]
    
    em_total = sum(emisiones_fuentes_reales.values())
    prod_total = sum([produccion_etapas.get(et, 0) for et in etapas_ordenadas])

    # --- Resultados globales ---
    st.markdown("#### Resultados globales")
    st.metric("Total emisiones estimadas", format_num(em_total, 2) + " kg CO₂e/ha")
    if prod_total > 0:
        st.metric("Emisiones por kg de fruta", format_num(em_total / prod_total, 3) + " kg CO₂e/kg fruta")
    else:
        st.warning("No se ha ingresado producción total. No es posible calcular emisiones por kg de fruta.")
    
    st.markdown("---")

    # --- Gráfico de evolución temporal de emisiones año a año ---
    emisiones_anuales = st.session_state.get("emisiones_anuales", [])
    if emisiones_anuales:
        st.markdown("#### Evolución temporal de emisiones año a año")
        df_evol = pd.DataFrame(emisiones_anuales, columns=["Año", "Emisiones (kg CO₂e/ha)", "Producción (kg/ha)", "Etapa"])
        df_evol["Emisiones_texto"] = df_evol["Emisiones (kg CO₂e/ha)"].apply(format_num)
        
        fig_evol = px.bar(
            df_evol,
            x="Año",
            y="Emisiones (kg CO₂e/ha)",
            color="Etapa",
            color_discrete_sequence=px.colors.qualitative.Set2,
            title="Evolución de emisiones año a año",
            text="Emisiones_texto"
        )
        
        # Configurar posición del texto dentro de las barras
        fig_evol.update_traces(
            textposition='inside',
            textangle=0,
            textfont=dict(
                size=10,
                color='white'
            )
        )
        
        # Mejorar el layout para mejor visualización
        fig_evol.update_layout(
            showlegend=True, 
            height=500,
            xaxis_title="Año",
            yaxis_title="Huella de carbono (kg CO₂e/ha)",
            xaxis=dict(
                tickmode='linear',
                tick0=df_evol["Año"].min(),
                dtick=1
            ),
            separators=',.'
        )
        
        st.plotly_chart(fig_evol, use_container_width=True, key=get_unique_key())

    st.markdown("---")

    # --- Resultados por etapa ---
    if emisiones_etapas:
        st.markdown("#### Huella de carbono por etapa")
        df_etapas = pd.DataFrame({
            "Etapa": [limpiar_nombre(et) for et in etapas_ordenadas],
            "Clave": etapas_ordenadas,
            "Huella de carbono (kg CO₂e/ha)": [emisiones_etapas[et] for et in etapas_ordenadas],
            "Producción (kg/ha)": [produccion_etapas.get(et, 0) for et in etapas_ordenadas]
        })
        df_etapas["Huella de carbono (kg CO₂e/kg fruta)"] = df_etapas.apply(
            lambda row: row["Huella de carbono (kg CO₂e/ha)"] / row["Producción (kg/ha)"] if row["Producción (kg/ha)"] > 0 else None,
            axis=1
        )
        total_emisiones_etapas = df_etapas["Huella de carbono (kg CO₂e/ha)"].sum()
        if total_emisiones_etapas > 0:
            df_etapas["% contribución"] = df_etapas["Huella de carbono (kg CO₂e/ha)"] / total_emisiones_etapas * 100
        else:
            df_etapas["% contribución"] = 0

        st.markdown("**Tabla: Huella de carbono y producción por etapa**")
        st.dataframe(df_etapas[["Etapa", "Huella de carbono (kg CO₂e/ha)", "Producción (kg/ha)", "Huella de carbono (kg CO₂e/kg fruta)", "% contribución"]].style.format({
            "Huella de carbono (kg CO₂e/ha)": format_num,
            "Producción (kg/ha)": format_num,
            "Huella de carbono (kg CO₂e/kg fruta)": lambda x: format_num(x, 3),
            "% contribución": format_percent
        }), hide_index=True)

        # Gráfico de barras por etapa (texto sólo en el total)
        st.markdown("##### Gráfico: Huella de carbono por etapa (kg CO₂e/ha)")
        y_max_etapa = df_etapas["Huella de carbono (kg CO₂e/ha)"].max() if not df_etapas.empty else 1
        textos_etapa = [format_num(v) for v in df_etapas["Emisiones (kg CO₂e/ha)"]]
        fig_etapa = px.bar(
            df_etapas,
            x="Etapa",
            y="Huella de carbono (kg CO₂e/ha)",
            color="Etapa",
            color_discrete_sequence=px.colors.qualitative.Pastel,
            title="Huella de carbono por etapa"
        )
        fig_etapa.add_trace(go.Scatter(
            x=df_etapas["Etapa"],
            y=df_etapas["Huella de carbono (kg CO₂e/ha)"],
            text=textos_etapa,
            mode="text",
            textposition="top center",
            showlegend=False
        ))
        fig_etapa.update_layout(showlegend=False, height=400, separators=',.')
        fig_etapa.update_yaxes(range=[0, y_max_etapa * 1.15])
        st.plotly_chart(fig_etapa, use_container_width=True, key=get_unique_key())

    st.markdown("---")

    # --- Emisiones por fuente y etapa (tabla y barras apiladas) ---
    if emisiones_etapas and emisiones_fuentes and emisiones_fuente_etapa:
        st.markdown("#### Huella de carbono por fuente y etapa (tabla y barras apiladas)")
        fuentes = [f for f in emisiones_fuentes.keys() if f != "Transporte"]
        etapas = df_etapas["Clave"].tolist()
        data_fuente_etapa = {fuente: [emisiones_fuente_etapa.get(etapa, {}).get(fuente, 0) for etapa in etapas] for fuente in fuentes}
        df_fuente_etapa = pd.DataFrame(data_fuente_etapa, index=[limpiar_nombre(e) for e in etapas])
        df_fuente_etapa.insert(0, "Etapa", [limpiar_nombre(e) for e in etapas])
        df_fuente_etapa_kg = df_fuente_etapa.copy()
        for i, etapa in enumerate(etapas):
            prod = produccion_etapas.get(etapa, 0)
            if prod > 0:
                df_fuente_etapa_kg.iloc[i, 1:] = df_fuente_etapa.iloc[i, 1:] / prod
            else:
                df_fuente_etapa_kg.iloc[i, 1:] = None
        st.markdown("**Tabla: Emisiones por fuente y etapa (kg CO₂e/ha)**")
        st.dataframe(df_fuente_etapa.style.format(format_num), hide_index=True)
        st.markdown("**Tabla: Emisiones por fuente y etapa (kg CO₂e/kg fruta)**")
        st.dataframe(df_fuente_etapa_kg.style.format(lambda x: format_num(x, 3)), hide_index=True)

        # Gráfico de barras apiladas por fuente y etapa (kg CO₂e/ha) - texto sólo en el total
        st.markdown("##### Gráfico: Emisiones por fuente y etapa (barras apiladas, kg CO₂e/ha)")
        fig_fuente_etapa = go.Figure()
        for fuente in fuentes:
            fig_fuente_etapa.add_bar(
                x=df_fuente_etapa["Etapa"],
                y=df_fuente_etapa[fuente],
                name=fuente
            )
        totales = df_fuente_etapa.iloc[:, 1:].sum(axis=1).values
        textos_tot = [format_num(v) for v in totales]
        fig_fuente_etapa.add_trace(go.Scatter(
            x=df_fuente_etapa["Etapa"],
            y=totales,
            text=textos_tot,
            mode="text",
            textposition="top center",
            showlegend=False
        ))
        y_max_fte = max(totales) if len(totales) > 0 else 1
        fig_fuente_etapa.update_layout(
            barmode='stack',
            yaxis_title="Huella de carbono (kg CO₂e/ha)",
            title="Huella de carbono por fuente y etapa (barras apiladas)",
            height=400,
            separators=',.'  # Formato español
        )
        fig_fuente_etapa.update_yaxes(range=[0, y_max_fte * 1.15])
        st.plotly_chart(fig_fuente_etapa, use_container_width=True, key=get_unique_key())

    st.markdown("---")

    # --- Desglose interno de cada fuente por etapa ---
    st.markdown("#### Desglose interno de cada fuente por etapa")
    etapas = df_etapas["Clave"].tolist()
    orden_fuentes = [f for f in emisiones_fuentes.keys() if f != "Transporte"]
    for idx, etapa in enumerate(etapas):
        nombre_etapa_limpio = limpiar_nombre(etapa)
        st.markdown(f"### Etapa: {nombre_etapa_limpio}")
        prod = produccion_etapas.get(etapa, 0)
        # ORDENAR fuentes de mayor a menor emisión en esta etapa
        fuentes_etapa = [f for f in orden_fuentes if f in emisiones_fuente_etapa.get(etapa, {})]
        fuentes_ordenadas = sorted(
            fuentes_etapa,
            key=lambda f: emisiones_fuente_etapa.get(etapa, {}).get(f, 0),
            reverse=True
        )
        for fuente in fuentes_ordenadas:
            valor = emisiones_fuente_etapa.get(etapa, {}).get(fuente, 0)
            if valor > 0:
                st.markdown(f"**{fuente}**")
                st.info(f"Explicación: {explicacion_fuente(fuente)}")
                # --- FERTILIZANTES ---
                if fuente == "Fertilizantes" and emisiones_fuente_etapa[etapa].get("desglose_fertilizantes"):
                    df_fert = pd.DataFrame(emisiones_fuente_etapa[etapa]["desglose_fertilizantes"])
                    if not df_fert.empty:
                        df_fert["Tipo fertilizante"] = df_fert["tipo"].apply(
                            lambda x: "Orgánico" if "org" in str(x).lower() or "estiércol" in str(x).lower() or "guano" in str(x).lower() else "Inorgánico"
                        )
                        total_fert = df_fert["total"].sum()
                        df_fert["% contribución"] = df_fert["total"] / total_fert * 100
                        if prod and prod > 0:
                            df_fert["Huella de carbono total (kg CO₂e/kg fruta)"] = df_fert["total"] / prod
                        else:
                            df_fert["Huella de carbono total (kg CO₂e/kg fruta)"] = None
                        st.markdown("**Tabla: Desglose de fertilizantes (orgánicos e inorgánicos)**")
                        df_fert_display = df_fert.rename(columns={
                            "emision_produccion": "Huella de carbono producción (kg CO₂e/ha·ciclo)",
                            "emision_co2_urea": "Huella de carbono CO₂ urea (kg CO₂e/ha·ciclo)",
                            "emision_n2o_directa": "Huella de carbono N₂O directa (kg CO₂e/ha·ciclo)",
                            "emision_n2o_ind_volatilizacion": "Huella de carbono N₂O ind. volatilización (kg CO₂e/ha·ciclo)",
                            "emision_n2o_ind_lixiviacion": "Huella de carbono N₂O ind. lixiviación (kg CO₂e/ha·ciclo)",
                            "emision_n2o_indirecta": "Huella de carbono N₂O indirecta (kg CO₂e/ha·ciclo)",
                            "total": "Huella de carbono total (kg CO₂e/ha·ciclo)"
                        })
                        st.dataframe(
                            df_fert_display[[
                                "Tipo fertilizante", "tipo", "cantidad", "Huella de carbono producción (kg CO₂e/ha·ciclo)", "Huella de carbono CO₂ urea (kg CO₂e/ha·ciclo)",
                                "Huella de carbono N₂O directa (kg CO₂e/ha·ciclo)", "Huella de carbono N₂O ind. volatilización (kg CO₂e/ha·ciclo)", "Huella de carbono N₂O ind. lixiviación (kg CO₂e/ha·ciclo)",
                                "Huella de carbono N₂O indirecta (kg CO₂e/ha·ciclo)", "Huella de carbono total (kg CO₂e/ha·ciclo)", "Huella de carbono total (kg CO₂e/kg fruta)", "% contribución"
                            ]].style.format({
                                "cantidad": format_num,
                                "Huella de carbono producción (kg CO₂e/ha·ciclo)": format_num,
                                "Huella de carbono CO₂ urea (kg CO₂e/ha·ciclo)": format_num,
                                "Huella de carbono N₂O directa (kg CO₂e/ha·ciclo)": format_num,
                                "Huella de carbono N₂O ind. volatilización (kg CO₂e/ha·ciclo)": format_num,
                                "Huella de carbono N₂O ind. lixiviación (kg CO₂e/ha·ciclo)": format_num,
                                "Huella de carbono N₂O indirecta (kg CO₂e/ha·ciclo)": format_num,
                                "Huella de carbono total (kg CO₂e/ha·ciclo)": format_num,
                                "Huella de carbono total (kg CO₂e/kg fruta)": lambda x: format_num(x, 3),
                                "% contribución": format_percent
                            }),
                            hide_index=True
                        )
                        # Gráficos de barras apiladas por tipo de emisión (orgánico e inorgánico por separado)
                        for tipo_cat in ["Orgánico", "Inorgánico"]:
                            df_tipo = df_fert[df_fert["Tipo fertilizante"] == tipo_cat]
                            if not df_tipo.empty:
                                st.markdown(f"**Gráfico: Emisiones por fertilizante {tipo_cat.lower()} y tipo de emisión (kg CO₂e/ha)**")
                                labels = df_tipo["tipo"]
                                em_prod = df_tipo["emision_produccion"].values
                                em_co2_urea = df_tipo["emision_co2_urea"].values
                                em_n2o_dir = df_tipo["emision_n2o_directa"].values
                                em_n2o_ind_vol = df_tipo["emision_n2o_ind_volatilizacion"].values
                                em_n2o_ind_lix = df_tipo["emision_n2o_ind_lixiviacion"].values
                                fig_fert = go.Figure()
                                fig_fert.add_bar(x=labels, y=em_prod, name="Producción")
                                fig_fert.add_bar(x=labels, y=em_co2_urea, name="CO₂ hidrólisis urea")
                                fig_fert.add_bar(x=labels, y=em_n2o_dir, name="N₂O directa")
                                fig_fert.add_bar(x=labels, y=em_n2o_ind_vol, name="N₂O indirecta (volatilización)")
                                fig_fert.add_bar(x=labels, y=em_n2o_ind_lix, name="N₂O indirecta (lixiviación)")
                                totales = em_prod + em_co2_urea + em_n2o_dir + em_n2o_ind_vol + em_n2o_ind_lix
                                textos_tot = [format_num(v) for v in totales]
                                fig_fert.add_trace(go.Scatter(
                                    x=labels,
                                    y=totales,
                                    text=textos_tot,
                                    mode="text",
                                    textposition="top center",
                                    showlegend=False
                                ))
                                fig_fert.update_layout(
                                    barmode='stack',
                                    yaxis_title="Emisiones (kg CO₂e/ha)",
                                    title=f"Emisiones por fertilizante {tipo_cat.lower()} y tipo de emisión",
                                    height=400,
                                    separators=',.'  # Formato español
                                )
                                fig_fert.update_yaxes(range=[0, max(totales) * 1.15 if len(totales) > 0 else 1])
                                st.plotly_chart(fig_fert, use_container_width=True, key=get_unique_key())
                # --- AGROQUÍMICOS ---
                elif fuente == "Agroquímicos" and emisiones_fuente_etapa[etapa].get("desglose_agroquimicos"):
                    df_agro = pd.DataFrame(emisiones_fuente_etapa[etapa]["desglose_agroquimicos"])
                    if not df_agro.empty:
                        total_agro = df_agro["emisiones"].sum()
                        df_agro["% contribución"] = df_agro["emisiones"] / total_agro * 100
                        if prod and prod > 0:
                            df_agro["Huella de carbono (kg CO₂e/kg fruta)"] = df_agro["emisiones"] / prod
                        else:
                            df_agro["Huella de carbono (kg CO₂e/kg fruta)"] = None
                        # Renombrar columna para mostrar en tabla
                        df_agro["Huella de carbono (kg CO₂e/ha)"] = df_agro["emisiones"]
                        st.markdown("**Tabla: Desglose de agroquímicos**")
                        st.dataframe(df_agro[["nombre_comercial", "categoria", "tipo", "cantidad_ia", "Huella de carbono (kg CO₂e/ha)", "Huella de carbono (kg CO₂e/kg fruta)", "% contribución"]].style.format({
                            "cantidad_ia": format_num,
                            "Huella de carbono (kg CO₂e/ha)": format_num,
                            "Huella de carbono (kg CO₂e/kg fruta)": lambda x: format_num(x, 3),
                            "% contribución": format_percent
                        }), hide_index=True)
                        # Gráfico de barras por nombre comercial (kg CO₂e/ha)
                        st.markdown("**Gráfico: Emisiones de agroquímicos por nombre comercial (kg CO₂e/ha)**")
                        # Agrupar por categoría para crear las barras
                        categorias = df_agro["categoria"].unique()
                        fig_agro = go.Figure()
                        
                        for categoria in categorias:
                            df_cat = df_agro[df_agro["categoria"] == categoria]
                            fig_agro.add_bar(
                                x=df_cat["nombre_comercial"], 
                                y=df_cat["emisiones"], 
                                name=categoria,
                                text=[format_num(v) for v in df_cat["emisiones"]],
                                textposition="outside"
                            )
                        
                        fig_agro.update_layout(
                            barmode='group',
                            yaxis_title="Emisiones (kg CO₂e/ha)",
                            title="Emisiones de agroquímicos por nombre comercial",
                            height=400,
                            separators=',.',  # Formato español
                            xaxis_title="Nombre comercial"
                        )
                        y_max_agro = df_agro["emisiones"].max() if not df_agro.empty else 1
                        fig_agro.update_yaxes(range=[0, y_max_agro * 1.15])
                        st.plotly_chart(fig_agro, use_container_width=True, key=get_unique_key())
                        # Gráfico de torta por nombre comercial (kg CO₂e/ha)
                        st.markdown("**Gráfico: % de contribución de cada agroquímico por nombre comercial (kg CO₂e/ha)**")
                        fig_pie_agro = px.pie(
                            df_agro,
                            names="nombre_comercial",
                            values="emisiones",
                            title="Contribución de cada agroquímico por nombre comercial",
                            color_discrete_sequence=px.colors.qualitative.Set2,
                            hole=0.3
                        )
                        # Configurar formato español para nombres y porcentajes
                        fig_pie_agro.update_traces(
                            textinfo='label+percent',
                            texttemplate='%{label}<br>%{percent}',
                            hovertemplate='<b>%{label}</b><br>Categoría: %{customdata}<br>Huella de carbono: %{value:.2f} kg CO₂e/ha<br>Porcentaje: %{percent}<extra></extra>',
                            customdata=df_agro["categoria"]
                        )
                        fig_pie_agro.update_layout(
                            showlegend=True, 
                            height=400,
                            separators=',.'  # Formato español
                        )
                        st.plotly_chart(fig_pie_agro, use_container_width=True, key=get_unique_key())
                # --- MAQUINARIA ---
                elif fuente == "Maquinaria" and emisiones_fuente_etapa[etapa].get("desglose_maquinaria"):
                    df_maq = pd.DataFrame(emisiones_fuente_etapa[etapa]["desglose_maquinaria"])
                    if not df_maq.empty:
                        total_maq = df_maq["emisiones"].sum()
                        df_maq["% contribución"] = df_maq["emisiones"] / total_maq * 100
                        if prod and prod > 0:
                            df_maq["Huella de carbono (kg CO₂e/kg fruta)"] = df_maq["emisiones"] / prod
                        else:
                            df_maq["Huella de carbono (kg CO₂e/kg fruta)"] = None
                        # Renombrar columna para mostrar en tabla
                        df_maq["Huella de carbono (kg CO₂e/ha)"] = df_maq["emisiones"]
                        st.markdown("**Tabla: Desglose de maquinaria**")
                        st.dataframe(df_maq[["nombre_labor", "tipo_maquinaria", "tipo_combustible", "litros", "Huella de carbono (kg CO₂e/ha)", "Huella de carbono (kg CO₂e/kg fruta)", "% contribución"]].style.format({
                            "litros": format_num,
                            "Huella de carbono (kg CO₂e/ha)": format_num,
                            "Huella de carbono (kg CO₂e/kg fruta)": lambda x: format_num(x, 3),
                            "% contribución": format_percent
                        }), hide_index=True)
                        # Gráfico de torta: emisiones por labor (kg CO₂e/ha)
                        st.markdown("**Gráfico: % de contribución de cada labor (torta, kg CO₂e/ha)**")
                        df_labor = df_maq.groupby("nombre_labor")["emisiones"].sum().reset_index()
                        fig_pie_labor = px.pie(
                            df_labor,
                            names="nombre_labor",
                            values="emisiones",
                            title="Contribución de cada labor al total de emisiones de maquinaria",
                            color_discrete_sequence=px.colors.qualitative.Set2,
                            hole=0.3
                        )
                        # Configurar formato español para nombres y porcentajes
                        fig_pie_labor.update_traces(
                            textinfo='label+percent',
                            texttemplate='%{label}<br>%{percent}',
                            hovertemplate='<b>%{label}</b><br>Emisiones: %{value:.2f} kg CO₂e/ha<br>Porcentaje: %{percent}<extra></extra>'
                        )
                        fig_pie_labor.update_layout(
                            showlegend=True, 
                            height=400,
                            separators=',.'  # Formato español
                        )
                        st.plotly_chart(fig_pie_labor, use_container_width=True, key=get_unique_key())
                        # Gráfico de torta: emisiones por maquinaria dentro de cada labor (kg CO₂e/ha)
                        labores_unicas = df_maq["nombre_labor"].unique()
                        for labor in labores_unicas:
                            df_labor_maq = df_maq[df_maq["nombre_labor"] == labor]
                            if len(df_labor_maq) > 1:
                                st.markdown(f"**Gráfico: % de contribución de cada maquinaria en la labor '{labor}' (torta, kg CO₂e/ha)**")
                                fig_pie_maq = px.pie(
                                    df_labor_maq,
                                    names="tipo_maquinaria",
                                    values="emisiones",
                                    title=f"Contribución de cada maquinaria en la labor '{labor}'",
                                    color_discrete_sequence=px.colors.qualitative.Pastel,
                                    hole=0.3
                                )
                                # Configurar formato español para nombres y porcentajes
                                fig_pie_maq.update_traces(
                                    textinfo='label+percent',
                                    texttemplate='%{label}<br>%{percent}',
                                    hovertemplate='<b>%{label}</b><br>Emisiones: %{value:.2f} kg CO₂e/ha<br>Porcentaje: %{percent}<extra></extra>'
                                )
                                fig_pie_maq.update_layout(
                                    showlegend=True, 
                                    height=400,
                                    separators=',.'  # Formato español
                                )
                                st.plotly_chart(fig_pie_maq, use_container_width=True, key=get_unique_key())
                        # Gráfico de barras apiladas: labor (X), emisiones (Y), apilado por maquinaria (kg CO₂e/ha)
                        st.markdown("**Gráfico: Emisiones por labor y tipo de maquinaria (barras apiladas, kg CO₂e/ha)**")
                        df_maq_grouped = df_maq.groupby(["nombre_labor", "tipo_maquinaria"]).agg({"emisiones": "sum"}).reset_index()
                        labores = df_maq_grouped["nombre_labor"].unique()
                        tipos_maq = df_maq_grouped["tipo_maquinaria"].unique()
                        fig_maq = go.Figure()
                        for maq in tipos_maq:
                            vals = []
                            for l in labores:
                                row = df_maq_grouped[(df_maq_grouped["nombre_labor"] == l) & (df_maq_grouped["tipo_maquinaria"] == maq)]
                                vals.append(row["emisiones"].values[0] if not row.empty else 0)
                            fig_maq.add_bar(
                                x=labores,
                                y=vals,
                                name=maq
                            )
                        totales = df_maq_grouped.groupby("nombre_labor")["emisiones"].sum().reindex(labores).values
                        textos_tot = [format_num(v) for v in totales]
                        fig_maq.add_trace(go.Scatter(
                            x=labores,
                            y=totales,
                            text=textos_tot,
                            mode="text",
                            textposition="top center",
                            showlegend=False
                        ))
                        y_max_maq = max(totales) if len(totales) > 0 else 1
                        fig_maq.update_layout(
                            barmode='stack',
                            yaxis_title="Emisiones (kg CO₂e/ha)",
                            title="Emisiones por labor y tipo de maquinaria",
                            height=400,
                            separators=',.'  # Formato español
                        )
                        fig_maq.update_yaxes(range=[0, y_max_maq * 1.15])
                        st.plotly_chart(fig_maq, use_container_width=True, key=get_unique_key())
                # --- RIEGO ---
                elif fuente == "Riego" and emisiones_fuente_etapa[etapa].get("desglose_riego"):
                    dr = emisiones_fuente_etapa[etapa]["desglose_riego"]
                    energia_actividades = dr.get("energia_actividades", [])
                    actividades = []
                    for ea in energia_actividades:
                        actividades.append({
                            "Actividad": ea.get("actividad", ""),
                            "Tipo actividad": ea.get("tipo_actividad", ""),
                            "Consumo agua (m³)": ea.get("agua_total_m3", 0),
                            "Huella de carbono agua (kg CO₂e/ha)": ea.get("emisiones_agua", 0),
                            "Consumo energía": ea.get("consumo_energia", 0),
                            "Tipo energía": ea.get("tipo_energia", ""),
                            "Huella de carbono energía (kg CO₂e/ha)": ea.get("emisiones_energia", 0),
                        })
                    if actividades:
                        df_riego = pd.DataFrame(actividades)
                        df_riego["Huella de carbono total (kg CO₂e/ha)"] = df_riego["Huella de carbono agua (kg CO₂e/ha)"] + df_riego["Huella de carbono energía (kg CO₂e/ha)"]
                        if prod and prod > 0:
                            df_riego["Huella de carbono total (kg CO₂e/kg fruta)"] = df_riego["Huella de carbono total (kg CO₂e/ha)"] / prod
                        else:
                            df_riego["Huella de carbono total (kg CO₂e/kg fruta)"] = None
                        total_riego = df_riego["Huella de carbono total (kg CO₂e/ha)"].sum()
                        if total_riego > 0:
                            df_riego["% contribución"] = df_riego["Huella de carbono total (kg CO₂e/ha)"] / total_riego * 100
                        else:
                            df_riego["% contribución"] = 0
                        st.markdown("**Tabla: Desglose de riego por actividad (agua y energía apilados)**")
                        st.dataframe(df_riego[[
                            "Actividad", "Tipo actividad", "Consumo agua (m³)", "Huella de carbono agua (kg CO₂e/ha)",
                            "Consumo energía", "Tipo energía", "Huella de carbono energía (kg CO₂e/ha)",
                            "Huella de carbono total (kg CO₂e/ha)", "Huella de carbono total (kg CO₂e/kg fruta)", "% contribución"
                        ]].style.format({
                            "Consumo agua (m³)": format_num,
                            "Huella de carbono agua (kg CO₂e/ha)": format_num,
                            "Consumo energía": format_num,
                            "Huella de carbono energía (kg CO₂e/ha)": format_num,
                            "Huella de carbono total (kg CO₂e/ha)": format_num,
                            "Huella de carbono total (kg CO₂e/kg fruta)": lambda x: format_num(x, 3),
                            "% contribución": format_percent
                        }), hide_index=True)
                        # Gráfico de barras apiladas por actividad (agua + energía) - texto sólo en el total
                        fig_riego = go.Figure()
                        fig_riego.add_bar(
                            x=df_riego["Actividad"],
                            y=df_riego["Huella de carbono agua (kg CO₂e/ha)"],
                            name="Agua"
                        )
                        fig_riego.add_bar(
                            x=df_riego["Actividad"],
                            y=df_riego["Huella de carbono energía (kg CO₂e/ha)"],
                            name="Energía"
                        )
                        totales = df_riego["Huella de carbono total (kg CO₂e/ha)"].values
                        textos_tot = [format_num(v) for v in totales]
                        fig_riego.add_trace(go.Scatter(
                            x=df_riego["Actividad"],
                            y=totales,
                            text=textos_tot,
                            mode="text",
                            textposition="top center",
                            showlegend=False
                        ))
                        y_max_riego = max(totales) if len(totales) > 0 else 1
                        fig_riego.update_layout(
                            barmode='stack',
                            yaxis_title="Huella de carbono (kg CO₂e/ha)",
                            title="Emisiones de riego por actividad (agua + energía)",
                            height=400,
                            separators=',.'  # Formato español
                        )
                        fig_riego.update_yaxes(range=[0, y_max_riego * 1.15])
                        st.plotly_chart(fig_riego, use_container_width=True, key=get_unique_key())
                    else:
                        st.info("No se ingresaron actividades de riego para esta etapa.")
                # --- RESIDUOS ---
                elif fuente == "Residuos" and emisiones_fuente_etapa[etapa].get("desglose_residuos"):
                    dr = emisiones_fuente_etapa[etapa]["desglose_residuos"]
                    if isinstance(dr, dict) and dr:
                        df_res = pd.DataFrame([
                            {
                                "Gestión": k,
                                "Biomasa (kg/ha)": v.get("biomasa", 0),
                                "Emisiones (kg CO₂e/ha)": v.get("emisiones", 0),
                                "Emisiones (kg CO₂e/kg fruta)": v.get("emisiones", 0) / prod if prod and prod > 0 else None
                            }
                            for k, v in dr.items()
                        ])
                        total_res = df_res["Emisiones (kg CO₂e/ha)"].sum()
                        df_res["% contribución"] = df_res["Emisiones (kg CO₂e/ha)"] / total_res * 100
                        textos_res = [format_num(v) for v in df_res["Emisiones (kg CO₂e/ha)"]]
                        st.markdown("**Tabla: Desglose de gestión de residuos vegetales**")
                        st.dataframe(df_res[[
                            "Gestión", "Biomasa (kg/ha)", "Emisiones (kg CO₂e/ha)", "Emisiones (kg CO₂e/kg fruta)", "% contribución"
                        ]].style.format({
                            "Biomasa (kg/ha)": format_num,
                            "Emisiones (kg CO₂e/ha)": format_num,
                            "Emisiones (kg CO₂e/kg fruta)": lambda x: format_num(x, 3),
                            "% contribución": format_percent
                        }), hide_index=True)
                        # Gráfico de barras por gestión de residuos
                        fig_res = px.bar(
                            df_res,
                            x="Gestión",
                            y="Emisiones (kg CO₂e/ha)",
                            color="Gestión",
                            color_discrete_sequence=px.colors.qualitative.Pastel,
                            title="Emisiones por gestión de residuos"
                        )
                        fig_res.add_trace(go.Scatter(
                            x=df_res["Gestión"],
                            y=df_res["Emisiones (kg CO₂e/ha)"],
                            text=textos_res,
                            mode="text",
                            textposition="top center",
                            showlegend=False
                        ))
                        fig_res.update_layout(showlegend=False, height=400, separators=',.')
                        fig_res.update_yaxes(range=[0, max(df_res["Emisiones (kg CO₂e/ha)"]) * 1.15 if not df_res.empty else 1])
                        st.plotly_chart(fig_res, use_container_width=True, key=get_unique_key())

    st.markdown("---")

    # --- Resumen ejecutivo ---
    st.markdown("#### Resumen ejecutivo")
    st.success(
        "📝 **Resumen ejecutivo:**\n\n"
        "El resumen ejecutivo presenta los resultados clave del cálculo de huella de carbono, útiles para reportes, certificaciones o toma de decisiones.\n\n"
        "Las emisiones totales estimadas para el sistema productivo corresponden a la suma de todas las fuentes y etapas consideradas, expresadas en **kg CO₂e/ha**. "
        "Este valor representa las emisiones acumuladas a lo largo de todo el ciclo de vida del cultivo, desde la implantación hasta la última etapa productiva, según el límite 'cradle-to-farm gate'.\n\n"
        f"**Total emisiones estimadas:** {format_num(em_total, 2)} kg CO₂e/ha"
        + (
            f"\n\n**Emisiones por kg de fruta:** {format_num(em_total/prod_total, 3)} kg CO₂e/kg fruta. "
            "Este indicador permite comparar la huella de carbono entre diferentes sistemas o productos, ya que relaciona las emisiones totales con la producción obtenida."
            if prod_total > 0 else "\n\nNo se ha ingresado producción total. No es posible calcular emisiones por kg de fruta."
        )
    )

    st.markdown("---")
    st.markdown("#### Parámetros de cálculo")
    st.write(f"Potenciales de calentamiento global (GWP) usados: {GWP}")
    st.write("Factores de emisión y fórmulas según IPCC 2006 y valores configurables al inicio del código.")

    # Guardar resultados globales y desgloses en session_state para exportación futura
    st.session_state["resultados_globales"] = {
        "tipo": "perenne",
        "em_total": em_total,
        "prod_total": prod_total,
        "emisiones_etapas": emisiones_etapas.copy(),
        "produccion_etapas": produccion_etapas.copy(),
        "emisiones_fuentes": emisiones_fuentes.copy(),
        "emisiones_fuente_etapa": emisiones_fuente_etapa.copy(),
        "detalle_residuos": st.session_state.get("detalle_residuos", []),
        "emisiones_anuales": st.session_state.get("emisiones_anuales", [])
    }

# -----------------------------
# Interfaz principal
# -----------------------------
em_total = 0
prod_total = 0

if anual.strip().lower() == "perenne":
    tabs = st.tabs(["Implantación", "Crecimiento sin producción", "Producción", "Resultados"])
    with tabs[0]:
        em_imp, prod_imp = etapa_implantacion()
        st.session_state["em_imp"] = em_imp
        st.session_state["prod_imp"] = prod_imp
    with tabs[1]:
        em_csp, prod_csp = etapa_crecimiento("Crecimiento sin producción", produccion_pregunta=False)
        st.session_state["em_csp"] = em_csp
        st.session_state["prod_csp"] = prod_csp
    with tabs[2]:
        em_pc, prod_pc = etapa_produccion_segmentada()
        st.session_state["em_pc"] = em_pc
        st.session_state["prod_pc"] = prod_pc
    with tabs[3]:
        # Calcular los totales SOLO al mostrar resultados
        em_total = (
            st.session_state.get("em_imp", 0)
            + st.session_state.get("em_csp", 0)
            + st.session_state.get("em_pc", 0)
        )
        prod_total = st.session_state.get("prod_pc", 0)
        mostrar_resultados_perenne(em_total, prod_total)

elif anual.strip().lower() == "anual":
    tabs = st.tabs(["Ingreso de información", "Resultados"])
    with tabs[0]:
        em_anual, prod_anual = etapa_anual()
        st.session_state["em_anual"] = em_anual
        st.session_state["prod_anual"] = prod_anual
    with tabs[1]:
        # Calcular los totales SOLO al mostrar resultados
        em_total = st.session_state.get("em_anual", 0)
        prod_total = st.session_state.get("prod_anual", 0)
        mostrar_resultados_anual(em_total, prod_total)
else:
    st.warning("Debe seleccionar si el cultivo es anual o perenne para continuar.")