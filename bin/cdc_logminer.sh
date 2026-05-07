#!/bin/bash
set -Eeuo pipefail

# =============================================================================
# cdc_logminer.sh v2
# =============================================================================
# Proyecto:
#   CDC-Industrial ORACLE - PostgreSQL v10 (No-License)
#
# Objetivo de esta versión:
#   - Ejecutar LogMiner de forma recurrente y controlada desde XE
#   - Gestionar explícitamente el diccionario LogMiner
#   - Usar ancla de diccionario SIN reprocesar cambios de negocio ya emitidos
#   - Mantener estado persistente externo y auditable
#   - Marcar archivos procesados (*.arc.proc)
#   - Mantener identificado el diccionario activo (*.arc.dict)
#   - Aceptar promoción de diccionario por marker/sidecar explícito
#
# Fuera de alcance de esta versión:
#   - Publicación a Kafka
#   - Aplicación a PostgreSQL
#   - Paralelismo / alta disponibilidad del extractor
#
# Principios operativos reflejados en el script:
#   - El script puede arrancar desde custuser, pero SIEMPRE ejecuta SQL como oracle
#   - El diccionario activo es un concepto explícito, no heurístico
#   - El offset de negocio NO se basa solo en SEQUENCE: usa SCN persistido
#   - El ARC ancla del diccionario puede reinyectarse sin duplicar negocio
#     porque la minería arranca desde LAST_PROCESSED_SCN + 1
#   - Bootstrap inicial es MANUAL y auditado
#
# Convención física acordada:
#   - Pendiente       : 1_20699_909684448.arc
#   - Procesado       : 1_20699_909684448.arc.proc
#   - Diccionario act.: 1_20687_909684448.arc.dict
#   - Marker explícito: 1_20687_909684448.arc.marker
#
# Marker esperado (ejemplo):
#   DICTIONARY_SEQUENCE=20687
#   DICTIONARY_SCN=487737436126
#   BUILD_REASON=DAILY
#   BUILD_TS=2026-05-06T10:45:00
#
# Bootstrap manual esperado en cdc_state.env:
#   LAST_PROCESSED_SEQUENCE=20699
#   LAST_PROCESSED_SCN=487737436126
#   LAST_DICTIONARY_SEQUENCE=20687
#   LAST_DICTIONARY_SCN=487737436126
#   CURRENT_DICTIONARY_FILE=/apps/olr/incoming/1_20687_909684448.arc.dict
#   LAST_RUN_TS=2026-05-06T00:00:00
#   LAST_STATUS=BOOTSTRAP
#   LAST_ROW_COUNT=0
#   LAST_ERROR=
#
# NOTA IMPORTANTE:
#   Este script NO borra evidencia documental. Los comentarios se consideran
#   parte del rastro técnico y operativo.
# =============================================================================

# =============================================================================
# CONFIGURACIÓN BASE
# =============================================================================

ORACLE_USER="oracle"
ORACLE_SID="${ORACLE_SID:-XE}"
ORACLE_HOME="${ORACLE_HOME:-/u01/app/oracle/product/11.2.0/xe}"
PATH="$ORACLE_HOME/bin:$PATH"

BASE_DIR="/apps/olr"
BIN_DIR="$BASE_DIR/bin"
ARC_DIR="$BASE_DIR/incoming"
STATE_DIR="$BASE_DIR/state"
LOG_DIR="$BASE_DIR/logs"
EXPORT_DIR="$BASE_DIR/export"
TMP_DIR="$BASE_DIR/tmp"

STATE_FILE="$STATE_DIR/cdc_state.env"
LOCK_FILE="$STATE_DIR/cdc_logminer.lock"

RUN_TS="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="$LOG_DIR/cdc_logminer_${RUN_TS}.log"
EXPORT_FILE="$EXPORT_DIR/logminer_export_${RUN_TS}.txt"

mkdir -p "$LOG_DIR" "$EXPORT_DIR" "$TMP_DIR" "$STATE_DIR"

# =============================================================================
# LOGGING
# =============================================================================
log() {
  local level="$1"
  shift
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$level] $*"
}

fail() {
  log "ERROR" "$*"
  exit 1
}

# Redirigir stdout/stderr a log + consola
exec > >(tee -a "$LOG_FILE") 2>&1

# =============================================================================
# TRAP GENERAL
# =============================================================================
on_error() {
  local exit_code=$?
  local line_no="${1:-unknown}"
  local cmd="${2:-unknown}"

  log "ERROR" "Fallo no controlado en línea $line_no: $cmd"
  update_state_error "ERROR en línea $line_no: $cmd"
  exit "$exit_code"
}

trap 'on_error "${LINENO}" "${BASH_COMMAND}"' ERR

# =============================================================================
# CONTROL DE USUARIO DE EJECUCIÓN
# =============================================================================
# Regla acordada:
#   - El script puede ser lanzado por custuser
#   - Pero el bloque Oracle SIEMPRE se ejecuta como oracle
#   - Se exige sudo -n -u oracle (sin password)
#
# Nota:
#   Si este bloque falla, no seguimos: un CDC recurrente NO puede depender
#   de password interactiva.
# =============================================================================
SCRIPT_PATH="${BASH_SOURCE[0]}"

if [[ "${1:-}" != "--as-oracle" ]]; then
  CURRENT_USER="$(id -un)"
  if [[ "$CURRENT_USER" != "$ORACLE_USER" ]]; then
    log "INFO" "Usuario actual: $CURRENT_USER"
    log "INFO" "Re-ejecutando script como $ORACLE_USER con sudo -n"

    exec sudo -n -u "$ORACLE_USER" \
      ORACLE_SID="$ORACLE_SID" \
      ORACLE_HOME="$ORACLE_HOME" \
      PATH="$ORACLE_HOME/bin:$PATH" \
      bash "$SCRIPT_PATH" --as-oracle "$@"
  fi
else
  shift
fi

CURRENT_USER="$(id -un)"
[[ "$CURRENT_USER" == "$ORACLE_USER" ]] || fail "El script debe ejecutarse como $ORACLE_USER"

# =============================================================================
# LOCK DE CONCURRENCIA
# =============================================================================
# Evita ejecuciones simultáneas del extractor.
# =============================================================================
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  fail "Ya existe una ejecución activa (lock: $LOCK_FILE)"
fi

# =============================================================================
# FUNCIONES DE SOPORTE
# =============================================================================
require_file() {
  local f="$1"
  [[ -f "$f" ]] || fail "No existe archivo requerido: $f"
}

is_integer() {
  [[ "$1" =~ ^[0-9]+$ ]]
}

extract_seq_from_path() {
  # Soporta:
  #   *.arc
  #   *.arc.dict
  #   *.arc.proc
  local f="$1"
  local base
  base="$(basename "$f")"

  if [[ "$base" =~ ^1_([0-9]+)_[0-9]+\.arc(\.dict|\.proc)?$ ]]; then
    echo "${BASH_REMATCH[1]}"
    return 0
  fi

  return 1
}

normalize_marker_arc_path() {
  # Convierte:
  #   /apps/olr/incoming/1_20687_909684448.arc.marker
  # en:
  #   /apps/olr/incoming/1_20687_909684448.arc
  local marker="$1"
  echo "${marker%.marker}"
}

sqlplus_sysdba() {
  sqlplus -s / as sysdba
}

# =============================================================================
# ESTADO EN MEMORIA
# =============================================================================
LAST_PROCESSED_SEQUENCE=""
LAST_PROCESSED_SCN=""
LAST_DICTIONARY_SEQUENCE=""
LAST_DICTIONARY_SCN=""
CURRENT_DICTIONARY_FILE=""
LAST_RUN_TS=""
LAST_STATUS=""
LAST_ROW_COUNT=""
LAST_ERROR=""

# Variables informativas del diccionario actual
LAST_DICTIONARY_MARKER=""
LAST_DICTIONARY_BUILD_REASON=""
LAST_DICTIONARY_BUILD_TS=""

# Valores calculados en esta ejecución
NEW_LAST_PROCESSED_SEQUENCE=""
NEW_LAST_PROCESSED_SCN=""
NEW_LAST_ROW_COUNT="0"
NEW_LAST_STATUS="OK"
NEW_LAST_ERROR=""

# Arrays
declare -a CANDIDATE_FILES=()
declare -a FILES_TO_PROCESS=()

# =============================================================================
# CARGA DE ESTADO (BOOTSTRAP MANUAL OBLIGATORIO)
# =============================================================================
load_state() {
  log "INFO" "Cargando estado desde: $STATE_FILE"
  require_file "$STATE_FILE"

  # shellcheck source=/dev/null
  source "$STATE_FILE"

  # Validaciones mínimas de bootstrap manual
  : "${LAST_PROCESSED_SEQUENCE:?Falta LAST_PROCESSED_SEQUENCE}"
  : "${LAST_PROCESSED_SCN:?Falta LAST_PROCESSED_SCN}"
  : "${LAST_DICTIONARY_SEQUENCE:?Falta LAST_DICTIONARY_SEQUENCE}"
  : "${LAST_DICTIONARY_SCN:?Falta LAST_DICTIONARY_SCN}"
  : "${CURRENT_DICTIONARY_FILE:?Falta CURRENT_DICTIONARY_FILE}"
  : "${LAST_RUN_TS:?Falta LAST_RUN_TS}"
  : "${LAST_STATUS:?Falta LAST_STATUS}"
  : "${LAST_ROW_COUNT:?Falta LAST_ROW_COUNT}"
  : "${LAST_ERROR:=}"

  is_integer "$LAST_PROCESSED_SEQUENCE" || fail "LAST_PROCESSED_SEQUENCE no es numérico"
  is_integer "$LAST_PROCESSED_SCN" || fail "LAST_PROCESSED_SCN no es numérico"
  is_integer "$LAST_DICTIONARY_SEQUENCE" || fail "LAST_DICTIONARY_SEQUENCE no es numérico"
  is_integer "$LAST_DICTIONARY_SCN" || fail "LAST_DICTIONARY_SCN no es numérico"

  if [[ ! -f "$CURRENT_DICTIONARY_FILE" ]]; then
    fail "CURRENT_DICTIONARY_FILE no existe físicamente: $CURRENT_DICTIONARY_FILE"
  fi

  log "INFO" "Estado cargado:"
  log "INFO" "  LAST_PROCESSED_SEQUENCE=$LAST_PROCESSED_SEQUENCE"
  log "INFO" "  LAST_PROCESSED_SCN=$LAST_PROCESSED_SCN"
  log "INFO" "  LAST_DICTIONARY_SEQUENCE=$LAST_DICTIONARY_SEQUENCE"
  log "INFO" "  LAST_DICTIONARY_SCN=$LAST_DICTIONARY_SCN"
  log "INFO" "  CURRENT_DICTIONARY_FILE=$CURRENT_DICTIONARY_FILE"
  log "INFO" "  LAST_RUN_TS=$LAST_RUN_TS"
  log "INFO" "  LAST_STATUS=$LAST_STATUS"
  log "INFO" "  LAST_ROW_COUNT=$LAST_ROW_COUNT"
}

# =============================================================================
# PERSISTENCIA SEGURA DEL ESTADO
# =============================================================================
save_state() {
  local tmp_state
  tmp_state="$(mktemp "$TMP_DIR/cdc_state.env.XXXXXX")"

  cat > "$tmp_state" <<EOF
# =============================================================================
# cdc_state.env
# =============================================================================
# Estado persistente del extractor CDC LogMiner.
# Este archivo es la fuente de verdad operativa del extractor.
# Bootstrap inicial: MANUAL y auditado.
# Actualizaciones posteriores: EXCLUSIVAMENTE por script.
# =============================================================================

LAST_PROCESSED_SEQUENCE=${NEW_LAST_PROCESSED_SEQUENCE}
LAST_PROCESSED_SCN=${NEW_LAST_PROCESSED_SCN}
LAST_DICTIONARY_SEQUENCE=${LAST_DICTIONARY_SEQUENCE}
LAST_DICTIONARY_SCN=${LAST_DICTIONARY_SCN}
CURRENT_DICTIONARY_FILE=${CURRENT_DICTIONARY_FILE}
LAST_RUN_TS=$(date -Iseconds)
LAST_STATUS=${NEW_LAST_STATUS}
LAST_ROW_COUNT=${NEW_LAST_ROW_COUNT}
LAST_ERROR=${NEW_LAST_ERROR}
LAST_DICTIONARY_MARKER=${LAST_DICTIONARY_MARKER}
LAST_DICTIONARY_BUILD_REASON=${LAST_DICTIONARY_BUILD_REASON}
LAST_DICTIONARY_BUILD_TS=${LAST_DICTIONARY_BUILD_TS}
EOF

  mv "$tmp_state" "$STATE_FILE"
  log "INFO" "Estado persistido correctamente en $STATE_FILE"
}

update_state_error() {
  local msg="${1:-ERROR_NO_DETALLADO}"

  # Si todavía no hemos cargado estado, no intentamos persistir nada
  [[ -f "$STATE_FILE" ]] || return 0

  # Si el estado nuevo aún no existe, preservamos el anterior
  NEW_LAST_PROCESSED_SEQUENCE="${NEW_LAST_PROCESSED_SEQUENCE:-${LAST_PROCESSED_SEQUENCE:-0}}"
  NEW_LAST_PROCESSED_SCN="${NEW_LAST_PROCESSED_SCN:-${LAST_PROCESSED_SCN:-0}}"
  NEW_LAST_ROW_COUNT="${NEW_LAST_ROW_COUNT:-0}"
  NEW_LAST_STATUS="ERROR"
  NEW_LAST_ERROR="$(echo "$msg" | tr ' ' '_' | tr -cd '[:alnum:]_:-')"

  if [[ -n "${LAST_PROCESSED_SEQUENCE:-}" ]]; then
    save_state || true
  fi
}

# =============================================================================
# PROMOCIÓN EXPLÍCITA DE NUEVO DICCIONARIO MEDIANTE MARKER
# =============================================================================
# Regla acordada:
#   - Un nuevo diccionario NO se "adivina"
#   - Se promueve por señal explícita (.marker)
#   - Bootstrap inicial sigue siendo manual
#
# Marker esperado:
#   1_20687_909684448.arc.marker
#
# Contenido:
#   DICTIONARY_SEQUENCE=20687
#   DICTIONARY_SCN=487737436126
#   BUILD_REASON=DAILY|POST_DDL
#   BUILD_TS=2026-05-06T10:45:00
# =============================================================================
promote_dictionary_if_marker_exists() {
  local marker
  local selected_marker=""
  local selected_seq=0
  local selected_scn=0
  local selected_reason=""
  local selected_ts=""
  local selected_arc=""
  local marker_seq
  local marker_scn
  local marker_reason
  local marker_ts

  shopt -s nullglob
  for marker in "$ARC_DIR"/1_*_*.arc.marker; do
    marker_seq=""
    marker_scn=""
    marker_reason=""
    marker_ts=""

    # shellcheck source=/dev/null
    source "$marker"

    : "${DICTIONARY_SEQUENCE:?Marker inválido sin DICTIONARY_SEQUENCE: $marker}"
    : "${DICTIONARY_SCN:?Marker inválido sin DICTIONARY_SCN: $marker}"
    : "${BUILD_REASON:=UNSPECIFIED}"
    : "${BUILD_TS:=UNKNOWN}"

    marker_seq="$DICTIONARY_SEQUENCE"
    marker_scn="$DICTIONARY_SCN"
    marker_reason="$BUILD_REASON"
    marker_ts="$BUILD_TS"

    is_integer "$marker_seq" || fail "Marker con DICTIONARY_SEQUENCE no numérico: $marker"
    is_integer "$marker_scn" || fail "Marker con DICTIONARY_SCN no numérico: $marker"

    if (( marker_seq > LAST_DICTIONARY_SEQUENCE )) && (( marker_seq > selected_seq )); then
      selected_marker="$marker"
      selected_seq="$marker_seq"
      selected_scn="$marker_scn"
      selected_reason="$marker_reason"
      selected_ts="$marker_ts"
    fi
  done
  shopt -u nullglob

  if [[ -z "$selected_marker" ]]; then
    log "INFO" "No hay marker de diccionario nuevo a promover."
    return 0
  fi

  selected_arc="$(normalize_marker_arc_path "$selected_marker")"
  require_file "$selected_arc"

  log "INFO" "Detectado nuevo diccionario por marker explícito:"
  log "INFO" "  MARKER=$selected_marker"
  log "INFO" "  ARC=$selected_arc"
  log "INFO" "  DICTIONARY_SEQUENCE=$selected_seq"
  log "INFO" "  DICTIONARY_SCN=$selected_scn"
  log "INFO" "  BUILD_REASON=$selected_reason"
  log "INFO" "  BUILD_TS=$selected_ts"

  # Demover el diccionario activo anterior si existe y es distinto
  # Solo si está físicamente marcado como .dict
  if [[ -n "$CURRENT_DICTIONARY_FILE" && -f "$CURRENT_DICTIONARY_FILE" && "$CURRENT_DICTIONARY_FILE" != "${selected_arc}.dict" ]]; then
    if [[ "$CURRENT_DICTIONARY_FILE" == *.arc.dict ]]; then
      local old_dict_seq
      old_dict_seq="$(extract_seq_from_path "$CURRENT_DICTIONARY_FILE" || echo 0)"

      if (( old_dict_seq <= LAST_PROCESSED_SEQUENCE )); then
        log "INFO" "Demoviendo diccionario anterior a .proc: $CURRENT_DICTIONARY_FILE"
        mv "$CURRENT_DICTIONARY_FILE" "${CURRENT_DICTIONARY_FILE%.dict}.proc"
      else
        log "WARN" "Diccionario anterior aún no consta procesado por secuencia. Se preserva por seguridad: $CURRENT_DICTIONARY_FILE"
      fi
    fi
  fi

  # Promover nuevo ARC a diccionario activo
  # Si ya estaba renombrado como .dict, lo respetamos
  if [[ -f "$selected_arc" ]]; then
    mv "$selected_arc" "${selected_arc}.dict"
    selected_arc="${selected_arc}.dict"
  elif [[ -f "${selected_arc}.dict" ]]; then
    selected_arc="${selected_arc}.dict"
  else
    fail "No se encuentra el ARC físico del marker ni como .arc ni como .arc.dict: $selected_arc"
  fi

  LAST_DICTIONARY_SEQUENCE="$selected_seq"
  LAST_DICTIONARY_SCN="$selected_scn"
  CURRENT_DICTIONARY_FILE="$selected_arc"
  LAST_DICTIONARY_MARKER="$selected_marker"
  LAST_DICTIONARY_BUILD_REASON="$selected_reason"
  LAST_DICTIONARY_BUILD_TS="$selected_ts"

  log "INFO" "Nuevo diccionario activo promovido:"
  log "INFO" "  CURRENT_DICTIONARY_FILE=$CURRENT_DICTIONARY_FILE"
  log "INFO" "  LAST_DICTIONARY_SEQUENCE=$LAST_DICTIONARY_SEQUENCE"
  log "INFO" "  LAST_DICTIONARY_SCN=$LAST_DICTIONARY_SCN"
}

# =============================================================================
# DESCUBRIMIENTO DE ARCHIVOS PENDIENTES
# =============================================================================
# Se consideran pendientes:
#   - *.arc
#   - *.arc.dict   (si su sequence > LAST_PROCESSED_SEQUENCE)
#
# Se excluyen:
#   - *.arc.proc
#   - *.marker
#
# NOTA:
#   El diccionario activo puede estar ya procesado y seguir existiendo como .dict.
#   Eso es correcto: sirve como ancla.
# =============================================================================
discover_pending_files() {
  local f
  local seq

  CANDIDATE_FILES=()
  FILES_TO_PROCESS=()

  shopt -s nullglob
  for f in "$ARC_DIR"/1_*_*.arc "$ARC_DIR"/1_*_*.arc.dict; do
    [[ -f "$f" ]] || continue

    # Excluir .proc si por alguna razón el glob captura algo extraño
    [[ "$f" == *.proc ]] && continue
    [[ "$f" == *.marker ]] && continue

    seq="$(extract_seq_from_path "$f" || true)"
    [[ -n "$seq" ]] || continue

    CANDIDATE_FILES+=("${seq}|${f}")
  done
  shopt -u nullglob

  if [[ "${#CANDIDATE_FILES[@]}" -eq 0 ]]; then
    log "INFO" "No hay archivos candidatos en incoming."
    return 0
  fi

  mapfile -t FILES_TO_PROCESS < <(
    printf '%s\n' "${CANDIDATE_FILES[@]}" \
      | sort -t'|' -k1,1n \
      | awk -F'|' -v last="$LAST_PROCESSED_SEQUENCE" '$1 > last { print $2 }'
  )

  if [[ "${#FILES_TO_PROCESS[@]}" -eq 0 ]]; then
    log "INFO" "No hay nuevos ARCHIVELOGs para procesar."
    return 0
  fi

  log "INFO" "ARCHIVELOGs pendientes detectados:"
  printf '  %s\n' "${FILES_TO_PROCESS[@]}"
}

# =============================================================================
# CONSTRUCCIÓN Y EJECUCIÓN DE SESIÓN LOGMINER
# =============================================================================
# Estrategia clave:
#   - Siempre se añade primero el diccionario activo (NEW)
#   - Luego los ARC pendientes (ADDFILE)
#   - START_LOGMNR arranca con STARTSCN = LAST_PROCESSED_SCN + 1
#   - COMMITTED_DATA_ONLY para orden y semántica de transacciones comprometidas
#   - DDL_DICT_TRACKING para mantener coherencia si el diccionario en redo requiere
#     seguimiento durante la sesión
#
# Esto evita que el ARC ancla reprocesa negocio:
#   el ancla aporta diccionario; el offset real lo gobierna STARTSCN.
# =============================================================================
build_and_run_logminer() {
  local sql_script
  local start_scn
  local f

  require_file "$CURRENT_DICTIONARY_FILE"

  start_scn=$((LAST_PROCESSED_SCN + 1))
  sql_script="$(mktemp "$TMP_DIR/logminer_XXXXXX.sql")"

  log "INFO" "Construyendo SQL de LogMiner..."
  log "INFO" "  CURRENT_DICTIONARY_FILE=$CURRENT_DICTIONARY_FILE"
  log "INFO" "  STARTSCN=$start_scn"

  {
    echo "WHENEVER SQLERROR EXIT FAILURE"
    echo "SET SERVEROUTPUT ON"
    echo "SET FEEDBACK ON"
    echo "SET VERIFY OFF"
    echo "SET ECHO OFF"
    echo ""

    echo "BEGIN"
    echo "  DBMS_LOGMNR.END_LOGMNR;"
    echo "EXCEPTION"
    echo "  WHEN OTHERS THEN NULL;"
    echo "END;"
    echo "/"
    echo ""

    echo "BEGIN"
    echo "  DBMS_LOGMNR.ADD_LOGFILE("
    echo "    LOGFILENAME => '$CURRENT_DICTIONARY_FILE',"
    echo "    OPTIONS     => DBMS_LOGMNR.NEW"
    echo "  );"

    for f in "${FILES_TO_PROCESS[@]}"; do
      [[ "$f" == "$CURRENT_DICTIONARY_FILE" ]] && continue
      echo "  DBMS_LOGMNR.ADD_LOGFILE("
      echo "    LOGFILENAME => '$f',"
      echo "    OPTIONS     => DBMS_LOGMNR.ADDFILE"
      echo "  );"
    done

    echo "END;"
    echo "/"
    echo ""

    echo "BEGIN"
    echo "  DBMS_LOGMNR.START_LOGMNR("
    echo "    STARTSCN => $start_scn,"
    echo "    OPTIONS  => DBMS_LOGMNR.DICT_FROM_REDO_LOGS"
    echo "               + DBMS_LOGMNR.COMMITTED_DATA_ONLY"
    echo "               + DBMS_LOGMNR.DDL_DICT_TRACKING"
    echo "  );"
    echo "END;"
    echo "/"
  } > "$sql_script"

  log "INFO" "Ejecutando sesión LogMiner..."
  sqlplus_sysdba @"$sql_script"

  rm -f "$sql_script"
}

# =============================================================================
# EXTRACCIÓN DE RESULTADOS DE ESTA EJECUCIÓN
# =============================================================================
# En esta fase aún no publicamos a Kafka.
# Dejamos un export técnico/forense para Notebook / validación.
#
# Se exportan columnas útiles y se calcula:
#   - número de filas CDC emitibles
#   - máximo SCN observado
#
# Nota:
#   SQL_REDO puede ser largo, por eso se exporta en archivo de texto delimitado.
# =============================================================================
extract_results() {
  local metrics_file
  metrics_file="$(mktemp "$TMP_DIR/metrics_XXXXXX.out")"

  log "INFO" "Extrayendo resultados a: $EXPORT_FILE"

  sqlplus_sysdba <<EOF > "$EXPORT_FILE"
SET PAGESIZE 0
SET LINESIZE 4000
SET LONG 100000
SET LONGCHUNKSIZE 100000
SET FEEDBACK OFF
SET VERIFY OFF
SET HEADING OFF
SET TRIMSPOOL ON

SELECT
  SCN || '|' ||
  NVL(TO_CHAR(TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS'), '') || '|' ||
  NVL(OPERATION, '') || '|' ||
  NVL(SEG_OWNER, '') || '|' ||
  NVL(SEG_NAME, '') || '|' ||
  REPLACE(REPLACE(NVL(SQL_REDO, ''), CHR(10), ' '), CHR(13), ' ')
FROM V\\$LOGMNR_CONTENTS
WHERE SCN > ${LAST_PROCESSED_SCN}
ORDER BY SCN, RS_ID, SSN;
EXIT
EOF

  sqlplus_sysdba <<EOF > "$metrics_file"
SET PAGESIZE 0
SET FEEDBACK OFF
SET VERIFY OFF
SET HEADING OFF
SET TRIMSPOOL ON

SELECT NVL(COUNT(*),0) FROM V\\$LOGMNR_CONTENTS WHERE SCN > ${LAST_PROCESSED_SCN};
SELECT NVL(MAX(SCN),${LAST_PROCESSED_SCN}) FROM V\\$LOGMNR_CONTENTS WHERE SCN > ${LAST_PROCESSED_SCN};
EXIT
EOF

  mapfile -t METRICS < "$metrics_file"
  rm -f "$metrics_file"

  NEW_LAST_ROW_COUNT="$(echo "${METRICS[0]:-0}" | tr -d '[:space:]')"
  NEW_LAST_PROCESSED_SCN="$(echo "${METRICS[1]:-$LAST_PROCESSED_SCN}" | tr -d '[:space:]')"

  is_integer "$NEW_LAST_ROW_COUNT" || NEW_LAST_ROW_COUNT="0"
  is_integer "$NEW_LAST_PROCESSED_SCN" || NEW_LAST_PROCESSED_SCN="$LAST_PROCESSED_SCN"

  log "INFO" "Métricas de la sesión:"
  log "INFO" "  ROW_COUNT=$NEW_LAST_ROW_COUNT"
  log "INFO" "  MAX_SCN=$NEW_LAST_PROCESSED_SCN"
}

# =============================================================================
# CIERRE EXPLÍCITO DE LOGMINER
# =============================================================================
end_logminer_session() {
  log "INFO" "Cerrando sesión LogMiner..."
  sqlplus_sysdba <<EOF
WHENEVER SQLERROR EXIT FAILURE
BEGIN
  DBMS_LOGMNR.END_LOGMNR;
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/
EXIT
EOF
}

# =============================================================================
# ROTACIÓN FÍSICA DE ARCHIVOS PROCESADOS
# =============================================================================
# Reglas:
#   - El diccionario activo se conserva como *.arc.dict
#   - Los ARC normales procesados pasan a *.arc.proc
#   - Si por alguna razón otro .dict ya no es el activo y fue procesado,
#     se puede degradar a *.arc.proc en una promoción futura
# =============================================================================
rotate_processed_files() {
  local f
  local seq

  if [[ "${#FILES_TO_PROCESS[@]}" -eq 0 ]]; then
    log "INFO" "No hay archivos que rotar."
    return 0
  fi

  NEW_LAST_PROCESSED_SEQUENCE="$LAST_PROCESSED_SEQUENCE"

  for f in "${FILES_TO_PROCESS[@]}"; do
    seq="$(extract_seq_from_path "$f" || echo 0)"
    (( seq > NEW_LAST_PROCESSED_SEQUENCE )) && NEW_LAST_PROCESSED_SEQUENCE="$seq"

    if [[ "$f" == "$CURRENT_DICTIONARY_FILE" ]]; then
      log "INFO" "Se conserva diccionario activo sin renombrar: $f"
      continue
    fi

    if [[ "$f" == *.arc ]]; then
      log "INFO" "Marcando ARC procesado -> .proc: $f"
      mv "$f" "${f}.proc"
      continue
    fi

    if [[ "$f" == *.arc.dict ]]; then
      if [[ "$f" != "$CURRENT_DICTIONARY_FILE" ]]; then
        log "INFO" "Demoviendo .dict no activo a .proc: $f"
        mv "$f" "${f%.dict}.proc"
      fi
      continue
    fi
  done

  log "INFO" "Última sequence procesada en esta ejecución: $NEW_LAST_PROCESSED_SEQUENCE"
}

# =============================================================================
# MAIN
# =============================================================================
main() {
  log "INFO" "============================================================"
  log "INFO" "CDC LOGMINER v2 - INICIO"
  log "INFO" "Usuario efectivo: $(id -un)"
  log "INFO" "ORACLE_SID=$ORACLE_SID"
  log "INFO" "ORACLE_HOME=$ORACLE_HOME"
  log "INFO" "LOG_FILE=$LOG_FILE"
  log "INFO" "============================================================"

  load_state

  # 1) Intentar promover un nuevo diccionario si ha llegado marker explícito
  promote_dictionary_if_marker_exists

  # 2) Descubrir archivos pendientes
  discover_pending_files

  if [[ "${#FILES_TO_PROCESS[@]}" -eq 0 ]]; then
    log "INFO" "No hay nuevos ARCHIVELOGs para procesar. Fin limpio."
    NEW_LAST_PROCESSED_SEQUENCE="$LAST_PROCESSED_SEQUENCE"
    NEW_LAST_PROCESSED_SCN="$LAST_PROCESSED_SCN"
    NEW_LAST_ROW_COUNT="0"
    NEW_LAST_STATUS="OK"
    NEW_LAST_ERROR=""
    save_state
    exit 0
  fi

  # 3) Ejecutar LogMiner anclado al diccionario activo, con STARTSCN controlado
  build_and_run_logminer

  # 4) Extraer resultados y métricas de esta ventana
  extract_results

  # 5) Cerrar sesión de forma explícita
  end_logminer_session

  # 6) Rotar físicamente archivos procesados
  rotate_processed_files

  # 7) Persistir estado final
  NEW_LAST_STATUS="OK"
  NEW_LAST_ERROR=""
  save_state

  log "INFO" "============================================================"
  log "INFO" "CDC LOGMINER v2 - FIN OK"
  log "INFO" "  NEW_LAST_PROCESSED_SEQUENCE=$NEW_LAST_PROCESSED_SEQUENCE"
  log "INFO" "  NEW_LAST_PROCESSED_SCN=$NEW_LAST_PROCESSED_SCN"
  log "INFO" "  NEW_LAST_ROW_COUNT=$NEW_LAST_ROW_COUNT"
  log "INFO" "  CURRENT_DICTIONARY_FILE=$CURRENT_DICTIONARY_FILE"
  log "INFO" "  EXPORT_FILE=$EXPORT_FILE"
  log "INFO" "============================================================"
}

main "$@"