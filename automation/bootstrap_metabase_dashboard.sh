#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${PROJECT_ROOT}/.env"
QUERIES_DIR="${PROJECT_ROOT}/metabase/dashboard_queries"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/wait_helpers.sh"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/docker_helper.sh"

METABASE_PORT="${METABASE_PORT:-3001}"

load_env_file() {
  if [ -f "${ENV_FILE}" ]; then
    set -a
    # shellcheck disable=SC1090
    source "${ENV_FILE}"
    set +a
  fi
}

require_env() {
  local var_name="$1"
  local value="${!var_name:-}"
  if [ -z "${value}" ]; then
    echo "ERROR: required env var '${var_name}' is not set." >&2
    exit 1
  fi
}

require_command() {
  local cmd="$1"
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "ERROR: required command '${cmd}' not found on host (needed for Metabase dashboard provisioning)." >&2
    exit 1
  fi
}

# Generic Metabase API call with mandatory HTTP-status checking.
# Args: METHOD PATH [SESSION_TOKEN] [JSON_BODY]
# - Echoes the response body on stdout for the caller to parse with jq.
# - Aborts (non-zero) on any non-2xx, printing Metabase's JSON error body.
# - Request bodies are streamed via stdin (never on argv) and never logged, so
#   the admin/reader passwords cannot leak into process lists or output.
metabase_api() {
  local method="$1"
  local path="$2"
  local session="${3:-}"
  local body="${4:-}"
  local url="${METABASE_BASE_URL}${path}"
  local body_file http_code
  body_file="$(mktemp)"

  local -a args=(-sS -X "${method}" -H "Content-Type: application/json" \
    -o "${body_file}" -w '%{http_code}')
  if [ -n "${session}" ]; then
    args+=(-H "X-Metabase-Session: ${session}")
  fi

  if [ -n "${body}" ]; then
    http_code="$(printf '%s' "${body}" | curl "${args[@]}" --data-binary @- "${url}")"
  else
    http_code="$(curl "${args[@]}" "${url}")"
  fi

  if [ -z "${http_code}" ] || [ "${http_code}" -lt 200 ] || [ "${http_code}" -ge 300 ]; then
    echo "ERROR: Metabase API ${method} ${path} returned HTTP ${http_code:-000}." >&2
    cat "${body_file}" >&2
    echo >&2
    rm -f "${body_file}"
    return 1
  fi

  cat "${body_file}"
  rm -f "${body_file}"
}

resolve_field_id() {
  local metadata="$1"
  local table="$2"
  local field="$3"
  printf '%s' "${metadata}" \
    | jq -r --arg t "${table}" --arg f "${field}" \
      '.tables[] | select(.name == $t) | .fields[] | select(.name == $f) | .id' \
    | head -n1
}

resolve_all_fields() {
  local metadata="$1"
  FIELD_DATE_ACTUAL="$(resolve_field_id "${metadata}" "dim_time" "date_actual")"
  FIELD_IS_WEEKEND="$(resolve_field_id "${metadata}" "dim_time" "is_weekend")"
  FIELD_ROUTE_ID="$(resolve_field_id "${metadata}" "trip_facts" "route_id")"
  FIELD_DIRECTION="$(resolve_field_id "${metadata}" "trip_facts" "direction")"
  FIELD_IS_CIRCULAR_FACTS="$(resolve_field_id "${metadata}" "trip_facts" "is_circular")"
  FIELD_IS_CIRCULAR_DETAILS="$(resolve_field_id "${metadata}" "trip_details" "is_circular")"
  FIELD_LINHA_LT="$(resolve_field_id "${metadata}" "latest_positions" "linha_lt")"
  FIELD_LINHA_SENTIDO="$(resolve_field_id "${metadata}" "latest_positions" "linha_sentido")"
  FIELD_VEICULO_LAT="$(resolve_field_id "${metadata}" "latest_positions" "veiculo_lat")"
  FIELD_VEICULO_LONG="$(resolve_field_id "${metadata}" "latest_positions" "veiculo_long")"
}

create_card() {
  local name="$1"
  local display="$2"
  local sql_file="$3"
  local tags_json="$4"
  local viz_json="$5"
  local sql card_id
  sql="$(cat "${QUERIES_DIR}/${sql_file}")"
  card_id="$(metabase_api POST /api/card "${SESSION_ID}" \
    "$(jq -n \
      --arg name "${name}" \
      --arg display "${display}" \
      --argjson db "${DB_ID}" \
      --argjson col "${COLLECTION_ID}" \
      --arg query "${sql}" \
      --argjson tags "${tags_json}" \
      --argjson viz "${viz_json}" \
      '{name: $name, display: $display, database_id: $db, collection_id: $col,
        dataset_query: {type: "native", database: $db,
          native: {query: $query, "template-tags": $tags}},
        visualization_settings: $viz}')" \
    | jq -r '.id // empty')"
  if ! [[ "${card_id}" =~ ^[0-9]+$ ]]; then
    echo "ERROR: card creation failed for '${name}' (got: '${card_id}')." >&2
    exit 1
  fi
  echo "    - card '${name}' created (id=${card_id})" >&2
  printf '%s' "${card_id}"
}


authenticate() {
  echo "==> Authenticating with Metabase..."
  SESSION_ID="$(metabase_api POST /api/session "" \
    "$(jq -n \
      --arg u "${METABASE_ADMIN_EMAIL}" \
      --arg p "${METABASE_ADMIN_PASSWORD}" \
      '{username: $u, password: $p}')" \
    | jq -r '.id // empty')"
  if [ -z "${SESSION_ID}" ]; then
    echo "ERROR: failed to obtain a Metabase admin session (check METABASE_ADMIN_* credentials)." >&2
    exit 1
  fi

  echo "==> Resolving sptrans_insights datasource..."
  DB_ID="$(metabase_api GET /api/database "${SESSION_ID}" \
    | jq -r '.data[] | select(.name == "sptrans_insights") | .id' | head -n1)"
  if [ -z "${DB_ID}" ] || ! [[ "${DB_ID}" =~ ^[0-9]+$ ]]; then
    echo "ERROR: datasource 'sptrans_insights' not found in Metabase. Run bootstrap_metabase.sh first." >&2
    exit 1
  fi
  echo "[OK] Authenticated, datasource id=${DB_ID}"

  echo "==> Checking if dashboard already exists..."
  local existing
  existing="$(metabase_api GET /api/dashboard "${SESSION_ID}" \
    | jq -r --arg n "${DASHBOARD_NAME}" '.[] | select(.name == $n) | .id' | head -n1)"
  if [ -n "${existing}" ]; then
    echo "Dashboard '${DASHBOARD_NAME}' already exists (id=${existing}); skipping."
    exit 0
  fi
  echo "==> Dashboard does not exist. Proceeding with provisioning..."
}

provision_collection() {
  echo "==> Resolving collection..."
  COLLECTION_ID="$(metabase_api GET /api/collection "${SESSION_ID}" \
    | jq -r --arg n "${COLLECTION_NAME}" '.[] | select(.name == $n) | .id' | head -n1)"
  if [ -n "${COLLECTION_ID}" ]; then
    echo "    - collection '${COLLECTION_NAME}' already exists (id=${COLLECTION_ID})"
  else
    COLLECTION_ID="$(metabase_api POST /api/collection "${SESSION_ID}" \
      "$(jq -n --arg n "${COLLECTION_NAME}" '{name: $n, color: "#509EE3"}')" \
      | jq -r '.id // empty')"
    echo "    - created collection '${COLLECTION_NAME}' (id=${COLLECTION_ID})"
  fi
  if ! [[ "${COLLECTION_ID}" =~ ^[0-9]+$ ]]; then
    echo "ERROR: could not resolve a valid collection id for '${COLLECTION_NAME}' (got: '${COLLECTION_ID}')." >&2
    exit 1
  fi
  echo "[OK] Collection resolved: id=${COLLECTION_ID}"
}

cleanup_orphaned_cards() {
  echo "==> Cleaning up orphaned cards in collection..."
  local card_ids orphan_count=0
  card_ids="$(metabase_api GET "/api/collection/${COLLECTION_ID}/items?models=card&limit=100" "${SESSION_ID}" \
    | jq -r '.data[].id')"
  while IFS= read -r card_id; do
    [ -z "${card_id}" ] && continue
    metabase_api DELETE "/api/card/${card_id}" "${SESSION_ID}" >/dev/null
    echo "    - deleted card id=${card_id}"
    orphan_count=$((orphan_count + 1))
  done <<< "${card_ids}"
  echo "[OK] ${orphan_count} orphaned card(s) removed"
}

resolve_field_ids() {
  echo "==> Resolving field IDs from database metadata..."
  local metadata needs_retry
  metadata="$(metabase_api GET "/api/database/${DB_ID}/metadata" "${SESSION_ID}")"
  resolve_all_fields "${metadata}"

  needs_retry=false
  for var in FIELD_DATE_ACTUAL FIELD_IS_WEEKEND FIELD_ROUTE_ID FIELD_DIRECTION \
             FIELD_IS_CIRCULAR_FACTS FIELD_IS_CIRCULAR_DETAILS FIELD_LINHA_LT \
             FIELD_LINHA_SENTIDO FIELD_VEICULO_LAT FIELD_VEICULO_LONG; do
    if [ -z "${!var}" ]; then needs_retry=true; break; fi
  done

  if "${needs_retry}"; then
    echo "    - some field IDs not yet available; waiting 10s for schema sync..."
    sleep 10
    metadata="$(metabase_api GET "/api/database/${DB_ID}/metadata" "${SESSION_ID}")"
    resolve_all_fields "${metadata}"
  fi

  for var in FIELD_DATE_ACTUAL FIELD_IS_WEEKEND FIELD_ROUTE_ID FIELD_DIRECTION \
             FIELD_IS_CIRCULAR_FACTS FIELD_IS_CIRCULAR_DETAILS FIELD_LINHA_LT \
             FIELD_LINHA_SENTIDO FIELD_VEICULO_LAT FIELD_VEICULO_LONG; do
    if ! [[ "${!var}" =~ ^[0-9]+$ ]]; then
      echo "ERROR: could not resolve field ID for '${var}' (got: '${!var}')." >&2
      exit 1
    fi
    echo "    - ${var}=${!var}"
  done
  echo "[OK] All 10 field IDs resolved"
}

create_cards() {
  echo "==> Creating cards..."

  local TAGS_P4 TAGS_P5 TAGS_P6 TAGS_P7 TAGS_P8 TAGS_P9 TAGS_P10 TAGS_P11A TAGS_P11B

  TAGS_P4="$(jq -n \
    --arg id_dr "${UUID_P4_DATE_RANGE}" --argjson fdr "${FIELD_DATE_ACTUAL}" \
    --arg id_ro "${UUID_P4_ROUTE}"       --argjson fro "${FIELD_ROUTE_ID}" \
    --arg id_di "${UUID_P4_DIRECTION}"   --argjson fdi "${FIELD_DIRECTION}" \
    --arg id_we "${UUID_P4_IS_WEEKEND}"  --argjson fwe "${FIELD_IS_WEEKEND}" \
    --arg id_ci "${UUID_P4_IS_CIRCULAR}" --argjson fci "${FIELD_IS_CIRCULAR_FACTS}" \
    '{date_range:  {id:$id_dr,name:"date_range",  "display-name":"Date range",type:"dimension",dimension:["field",$fdr,null],"widget-type":"date/all-options",default:null},
      route:       {id:$id_ro,name:"route",       "display-name":"Route",     type:"dimension",dimension:["field",$fro,null],"widget-type":"string/=",        default:null},
      direction:   {id:$id_di,name:"direction",   "display-name":"Direction", type:"dimension",dimension:["field",$fdi,null],"widget-type":"string/=",        default:null},
      is_weekend:  {id:$id_we,name:"is_weekend",  "display-name":"Weekend",   type:"dimension",dimension:["field",$fwe,null],"widget-type":"category",        default:null},
      is_circular: {id:$id_ci,name:"is_circular", "display-name":"Circular",  type:"dimension",dimension:["field",$fci,null],"widget-type":"category",        default:null}}')"

  TAGS_P5="$(jq -n \
    --arg id_dr "${UUID_P5_DATE_RANGE}" --argjson fdr "${FIELD_DATE_ACTUAL}" \
    --arg id_ro "${UUID_P5_ROUTE}"       --argjson fro "${FIELD_ROUTE_ID}" \
    --arg id_di "${UUID_P5_DIRECTION}"   --argjson fdi "${FIELD_DIRECTION}" \
    --arg id_we "${UUID_P5_IS_WEEKEND}"  --argjson fwe "${FIELD_IS_WEEKEND}" \
    --arg id_ci "${UUID_P5_IS_CIRCULAR}" --argjson fci "${FIELD_IS_CIRCULAR_FACTS}" \
    '{date_range:  {id:$id_dr,name:"date_range",  "display-name":"Date range",type:"dimension",dimension:["field",$fdr,null],"widget-type":"date/all-options",default:null},
      route:       {id:$id_ro,name:"route",       "display-name":"Route",     type:"dimension",dimension:["field",$fro,null],"widget-type":"string/=",        default:null},
      direction:   {id:$id_di,name:"direction",   "display-name":"Direction", type:"dimension",dimension:["field",$fdi,null],"widget-type":"string/=",        default:null},
      is_weekend:  {id:$id_we,name:"is_weekend",  "display-name":"Weekend",   type:"dimension",dimension:["field",$fwe,null],"widget-type":"category",        default:null},
      is_circular: {id:$id_ci,name:"is_circular", "display-name":"Circular",  type:"dimension",dimension:["field",$fci,null],"widget-type":"category",        default:null}}')"

  TAGS_P6="$(jq -n \
    --arg id_dr "${UUID_P6_DATE_RANGE}" --argjson fdr "${FIELD_DATE_ACTUAL}" \
    --arg id_di "${UUID_P6_DIRECTION}"   --argjson fdi "${FIELD_DIRECTION}" \
    --arg id_we "${UUID_P6_IS_WEEKEND}"  --argjson fwe "${FIELD_IS_WEEKEND}" \
    --arg id_ro "${UUID_P6_ROUTE}"       --argjson fro "${FIELD_ROUTE_ID}" \
    --arg id_ci "${UUID_P6_IS_CIRCULAR}" --argjson fci "${FIELD_IS_CIRCULAR_FACTS}" \
    --arg id_mt "${UUID_P6_MIN_TRIPS}" \
    '{date_range:  {id:$id_dr,name:"date_range",  "display-name":"Date range",type:"dimension",dimension:["field",$fdr,null],"widget-type":"date/all-options",default:null},
      direction:   {id:$id_di,name:"direction",   "display-name":"Direction", type:"dimension",dimension:["field",$fdi,null],"widget-type":"string/=",        default:null},
      is_weekend:  {id:$id_we,name:"is_weekend",  "display-name":"Weekend",   type:"dimension",dimension:["field",$fwe,null],"widget-type":"category",        default:null},
      route:       {id:$id_ro,name:"route",       "display-name":"Route",     type:"dimension",dimension:["field",$fro,null],"widget-type":"string/=",        default:null},
      is_circular: {id:$id_ci,name:"is_circular", "display-name":"Circular",  type:"dimension",dimension:["field",$fci,null],"widget-type":"category",        default:null},
      min_trips:   {id:$id_mt,name:"min_trips",   "display-name":"Min trips", type:"number",   default:"5"}}')"

  TAGS_P7="$(jq -n \
    --arg id_ro "${UUID_P7_ROUTE}" --argjson fro "${FIELD_ROUTE_ID}" \
    '{route: {id:$id_ro,name:"route","display-name":"Route",type:"dimension",dimension:["field",$fro,null],"widget-type":"string/=",default:null}}')"

  TAGS_P8="$(jq -n \
    --arg id_dr "${UUID_P8_DATE_RANGE}" --argjson fdr "${FIELD_DATE_ACTUAL}" \
    --arg id_ro "${UUID_P8_ROUTE}"       --argjson fro "${FIELD_ROUTE_ID}" \
    --arg id_ci "${UUID_P8_IS_CIRCULAR}" --argjson fci "${FIELD_IS_CIRCULAR_FACTS}" \
    --arg id_mt "${UUID_P8_MIN_TRIPS}" \
    '{date_range:  {id:$id_dr,name:"date_range",  "display-name":"Date range",type:"dimension",dimension:["field",$fdr,null],"widget-type":"date/all-options",default:null},
      route:       {id:$id_ro,name:"route",       "display-name":"Route",     type:"dimension",dimension:["field",$fro,null],"widget-type":"string/=",        default:null},
      is_circular: {id:$id_ci,name:"is_circular", "display-name":"Circular",  type:"dimension",dimension:["field",$fci,null],"widget-type":"category",        default:null},
      min_trips:   {id:$id_mt,name:"min_trips",   "display-name":"Min trips", type:"number",   default:"5"}}')"

  TAGS_P9="$(jq -n \
    --arg id_dr "${UUID_P9_DATE_RANGE}" --argjson fdr "${FIELD_DATE_ACTUAL}" \
    --arg id_ro "${UUID_P9_ROUTE}"       --argjson fro "${FIELD_ROUTE_ID}" \
    --arg id_ci "${UUID_P9_IS_CIRCULAR}" --argjson fci "${FIELD_IS_CIRCULAR_FACTS}" \
    '{date_range:  {id:$id_dr,name:"date_range",  "display-name":"Date range",type:"dimension",dimension:["field",$fdr,null],"widget-type":"date/all-options",default:null},
      route:       {id:$id_ro,name:"route",       "display-name":"Route",     type:"dimension",dimension:["field",$fro,null],"widget-type":"string/=",        default:null},
      is_circular: {id:$id_ci,name:"is_circular", "display-name":"Circular",  type:"dimension",dimension:["field",$fci,null],"widget-type":"category",        default:null}}')"

  TAGS_P10="$(jq -n \
    --arg id_ro "${UUID_P10_ROUTE}"       --argjson fro "${FIELD_ROUTE_ID}" \
    --arg id_dr "${UUID_P10_DATE_RANGE}"  --argjson fdr "${FIELD_DATE_ACTUAL}" \
    --arg id_ci "${UUID_P10_IS_CIRCULAR}" --argjson fci "${FIELD_IS_CIRCULAR_DETAILS}" \
    '{route:       {id:$id_ro,name:"route",       "display-name":"Route",     type:"dimension",dimension:["field",$fro,null],"widget-type":"string/=",        default:null},
      date_range:  {id:$id_dr,name:"date_range",  "display-name":"Date range",type:"dimension",dimension:["field",$fdr,null],"widget-type":"date/all-options",default:null},
      is_circular: {id:$id_ci,name:"is_circular", "display-name":"Circular",  type:"dimension",dimension:["field",$fci,null],"widget-type":"category",        default:null}}')"

  TAGS_P11A="$(jq -n \
    --arg id_lt "${UUID_P11A_LINHA_LT}"      --argjson flt "${FIELD_LINHA_LT}" \
    --arg id_se "${UUID_P11A_LINHA_SENTIDO}" --argjson fse "${FIELD_LINHA_SENTIDO}" \
    '{linha_lt:      {id:$id_lt,name:"linha_lt",      "display-name":"Linha LT",      type:"dimension",dimension:["field",$flt,null],"widget-type":"string/=",default:null},
      linha_sentido: {id:$id_se,name:"linha_sentido", "display-name":"Linha sentido", type:"dimension",dimension:["field",$fse,null],"widget-type":"string/=",default:null}}')"

  TAGS_P11B="$(jq -n \
    --arg id_lt "${UUID_P11B_LINHA_LT}"      --argjson flt "${FIELD_LINHA_LT}" \
    --arg id_se "${UUID_P11B_LINHA_SENTIDO}" --argjson fse "${FIELD_LINHA_SENTIDO}" \
    '{linha_lt:      {id:$id_lt,name:"linha_lt",      "display-name":"Linha LT",      type:"dimension",dimension:["field",$flt,null],"widget-type":"string/=",default:null},
      linha_sentido: {id:$id_se,name:"linha_sentido", "display-name":"Linha sentido", type:"dimension",dimension:["field",$fse,null],"widget-type":"string/=",default:null}}')"

  CARD_P0="$(create_card   "Pipeline freshness"             "table"  "latest_batch_freshness.sql"                  '{}'           '{}')"
  CARD_P1="$(create_card   "Trips completed today"          "scalar" "today_kpis.sql"                              '{}'           '{"scalar.field":"trips_completed_today"}')"
  CARD_P2="$(create_card   "Active routes today"            "scalar" "today_kpis.sql"                              '{}'           '{"scalar.field":"active_routes_today"}')"
  CARD_P3="$(create_card   "Active vehicles today"          "scalar" "today_kpis.sql"                              '{}'           '{"scalar.field":"active_vehicles_today"}')"
  CARD_P4="$(create_card   "Trips per route per hour"       "table"  "frequency_by_route_hour_direction.sql"       "${TAGS_P4}"   '{}')"
  CARD_P5="$(create_card   "Frequency by direction"         "bar"    "frequency_by_route_hour_direction.sql"       "${TAGS_P5}"   '{"graph.dimensions":["direction"],"graph.metrics":["trip_count"]}')"
  CARD_P6="$(create_card   "Median and P95 duration"        "bar"    "median_and_p95_duration_by_route.sql"        "${TAGS_P6}"   '{"graph.dimensions":["route_id"],"graph.metrics":["median_duration_seconds","p95_duration_seconds"]}')"
  CARD_P7="$(create_card   "Today vs historical baseline"   "table"  "duration_today_vs_same_weekday_baseline.sql" "${TAGS_P7}"   '{}')"
  CARD_P8="$(create_card   "Reliability ranking"            "bar"    "reliability_by_route.sql"                    "${TAGS_P8}"   '{"graph.dimensions":["route_id"],"graph.metrics":["reliability_index"],"graph.x_axis.scale":"ordinal"}')"
  CARD_P9="$(create_card   "Avg speed by route and hour"    "line"   "avg_speed_by_route_and_hour.sql"             "${TAGS_P9}"   '{"graph.dimensions":["hour_of_day"],"graph.metrics":["avg_speed_kmh"]}')"
  CARD_P10="$(create_card  "Route summary"                  "table"  "route_summary_with_trip_details.sql"         "${TAGS_P10}"  '{}')"
  CARD_P11A="$(create_card "Live fleet positions"           "map"    "live_fleet_positions.sql"                    "${TAGS_P11A}" '{"map.type":"pin","map.latitude_column":"veiculo_lat","map.longitude_column":"veiculo_long"}')"
  CARD_P11B="$(create_card "Live fleet count + freshness"   "scalar" "live_fleet_positions_freshness.sql"          "${TAGS_P11B}" '{"scalar.field":"active_vehicle_count"}')"

  for var in CARD_P0 CARD_P1 CARD_P2 CARD_P3 CARD_P4 CARD_P5 CARD_P6 CARD_P7 \
             CARD_P8 CARD_P9 CARD_P10 CARD_P11A CARD_P11B; do
    if ! [[ "${!var}" =~ ^[0-9]+$ ]]; then
      echo "ERROR: card ID not set for '${var}'." >&2
      exit 1
    fi
  done
  echo "[OK] All 13 cards created"
}

create_dashboard() {
  echo "==> Creating dashboard..."
  DASHBOARD_ID="$(metabase_api POST /api/dashboard "${SESSION_ID}" \
    "$(jq -n --arg n "${DASHBOARD_NAME}" --argjson col "${COLLECTION_ID}" \
      '{name: $n, collection_id: $col}')" \
    | jq -r '.id // empty')"
  if ! [[ "${DASHBOARD_ID}" =~ ^[0-9]+$ ]]; then
    echo "ERROR: dashboard creation failed (got: '${DASHBOARD_ID}')." >&2
    exit 1
  fi
  echo "    - dashboard '${DASHBOARD_NAME}' created (id=${DASHBOARD_ID})"

  echo "==> Setting dashboard parameters..."
  metabase_api PUT "/api/dashboard/${DASHBOARD_ID}" "${SESSION_ID}" \
    "$(jq -n \
      --arg dr "${UUID_PARAM_DATE_RANGE}" \
      --arg ro "${UUID_PARAM_ROUTE}" \
      --arg di "${UUID_PARAM_DIRECTION}" \
      --arg we "${UUID_PARAM_IS_WEEKEND}" \
      --arg ci "${UUID_PARAM_IS_CIRCULAR}" \
      --arg lt "${UUID_PARAM_LINHA_LT}" \
      --arg se "${UUID_PARAM_LINHA_SENTIDO}" \
      '{parameters: [
        {id:$dr, type:"date/all-options", name:"Date range",    slug:"date_range",    default:"past30days", sectionId:"date"},
        {id:$ro, type:"string/=",         name:"Route",         slug:"route"},
        {id:$di, type:"string/=",         name:"Direction",     slug:"direction"},
        {id:$we, type:"category",         name:"Weekend",       slug:"is_weekend"},
        {id:$ci, type:"category",         name:"Circular",      slug:"is_circular"},
        {id:$lt, type:"string/=",         name:"Linha LT",      slug:"linha_lt"},
        {id:$se, type:"string/=",         name:"Linha Sentido", slug:"linha_sentido"}
      ]}')" >/dev/null

  local param_count
  param_count="$(metabase_api GET "/api/dashboard/${DASHBOARD_ID}" "${SESSION_ID}" \
    | jq '.parameters | length')"
  if [ "${param_count}" != "7" ]; then
    echo "ERROR: dashboard parameters not persisted (expected 7, got ${param_count})." >&2
    exit 1
  fi
  echo "[OK] 7 dashboard parameters set"

  echo "==> Placing dashcards..."

  local cards_json dashcards_count
  cards_json="$(jq -n \
    --argjson c0  "${CARD_P0}"   --argjson c1  "${CARD_P1}"  \
    --argjson c2  "${CARD_P2}"   --argjson c3  "${CARD_P3}"  \
    --argjson c4  "${CARD_P4}"   --argjson c5  "${CARD_P5}"  \
    --argjson c6  "${CARD_P6}"   --argjson c7  "${CARD_P7}"  \
    --argjson c8  "${CARD_P8}"   --argjson c9  "${CARD_P9}"  \
    --argjson c10 "${CARD_P10}"  \
    --argjson c11 "${CARD_P11A}" --argjson c12 "${CARD_P11B}" \
    --arg dr "${UUID_PARAM_DATE_RANGE}" --arg ro "${UUID_PARAM_ROUTE}" \
    --arg di "${UUID_PARAM_DIRECTION}"  --arg we "${UUID_PARAM_IS_WEEKEND}" \
    --arg ci "${UUID_PARAM_IS_CIRCULAR}" \
    --arg lt "${UUID_PARAM_LINHA_LT}"   --arg se "${UUID_PARAM_LINHA_SENTIDO}" \
    '[
      {id:-1, card_id:$c0, col:0, row:0,  size_x:24,size_y:4,  parameter_mappings:[]},
      {id:-2, card_id:$c1, col:0, row:4,  size_x:8, size_y:4,  parameter_mappings:[]},
      {id:-3, card_id:$c2, col:8, row:4,  size_x:8, size_y:4,  parameter_mappings:[]},
      {id:-4, card_id:$c3, col:16,row:4,  size_x:8, size_y:4,  parameter_mappings:[]},
      {id:-5, card_id:$c4, col:0, row:8,  size_x:12,size_y:8,  parameter_mappings:[
        {card_id:$c4,parameter_id:$dr,target:["dimension",["template-tag","date_range"]]},
        {card_id:$c4,parameter_id:$ro,target:["dimension",["template-tag","route"]]},
        {card_id:$c4,parameter_id:$di,target:["dimension",["template-tag","direction"]]},
        {card_id:$c4,parameter_id:$we,target:["dimension",["template-tag","is_weekend"]]},
        {card_id:$c4,parameter_id:$ci,target:["dimension",["template-tag","is_circular"]]}]},
      {id:-6, card_id:$c5, col:12,row:8,  size_x:12,size_y:8,  parameter_mappings:[
        {card_id:$c5,parameter_id:$dr,target:["dimension",["template-tag","date_range"]]},
        {card_id:$c5,parameter_id:$ro,target:["dimension",["template-tag","route"]]},
        {card_id:$c5,parameter_id:$di,target:["dimension",["template-tag","direction"]]},
        {card_id:$c5,parameter_id:$we,target:["dimension",["template-tag","is_weekend"]]},
        {card_id:$c5,parameter_id:$ci,target:["dimension",["template-tag","is_circular"]]}]},
      {id:-7, card_id:$c6, col:0, row:16, size_x:12,size_y:8,  parameter_mappings:[
        {card_id:$c6,parameter_id:$dr,target:["dimension",["template-tag","date_range"]]},
        {card_id:$c6,parameter_id:$ro,target:["dimension",["template-tag","route"]]},
        {card_id:$c6,parameter_id:$di,target:["dimension",["template-tag","direction"]]},
        {card_id:$c6,parameter_id:$we,target:["dimension",["template-tag","is_weekend"]]},
        {card_id:$c6,parameter_id:$ci,target:["dimension",["template-tag","is_circular"]]}]},
      {id:-8, card_id:$c7, col:12,row:16, size_x:12,size_y:8,  parameter_mappings:[
        {card_id:$c7,parameter_id:$ro,target:["dimension",["template-tag","route"]]}]},
      {id:-9, card_id:$c8, col:0, row:24, size_x:12,size_y:8,  parameter_mappings:[
        {card_id:$c8,parameter_id:$dr,target:["dimension",["template-tag","date_range"]]},
        {card_id:$c8,parameter_id:$ro,target:["dimension",["template-tag","route"]]},
        {card_id:$c8,parameter_id:$ci,target:["dimension",["template-tag","is_circular"]]}]},
      {id:-10,card_id:$c9, col:12,row:24, size_x:12,size_y:8,  parameter_mappings:[
        {card_id:$c9,parameter_id:$dr,target:["dimension",["template-tag","date_range"]]},
        {card_id:$c9,parameter_id:$ro,target:["dimension",["template-tag","route"]]},
        {card_id:$c9,parameter_id:$ci,target:["dimension",["template-tag","is_circular"]]}]},
      {id:-11,card_id:$c10,col:0, row:32, size_x:24,size_y:8,  parameter_mappings:[
        {card_id:$c10,parameter_id:$dr,target:["dimension",["template-tag","date_range"]]},
        {card_id:$c10,parameter_id:$ro,target:["dimension",["template-tag","route"]]},
        {card_id:$c10,parameter_id:$ci,target:["dimension",["template-tag","is_circular"]]}]},
      {id:-12,card_id:$c11,col:0, row:40, size_x:16,size_y:12, parameter_mappings:[
        {card_id:$c11,parameter_id:$lt,target:["dimension",["template-tag","linha_lt"]]},
        {card_id:$c11,parameter_id:$se,target:["dimension",["template-tag","linha_sentido"]]}]},
      {id:-13,card_id:$c12,col:16,row:40, size_x:8, size_y:12, parameter_mappings:[]}
    ]')"

  dashcards_count="$(metabase_api PUT "/api/dashboard/${DASHBOARD_ID}/cards" "${SESSION_ID}" \
    "$(jq -n --argjson cards "${cards_json}" '{cards: $cards}')" \
    | jq '.cards | length')"
  if [ "${dashcards_count}" != "13" ]; then
    echo "ERROR: dashcard placement incomplete (expected 13, got ${dashcards_count})." >&2
    exit 1
  fi
  echo "[OK] Dashboard ready: id=${DASHBOARD_ID}, 13 dashcards placed"
}

set_field_metadata() {
  echo "==> Setting semantic types for lat/long fields..."

  metabase_api PUT "/api/field/${FIELD_VEICULO_LAT}" "${SESSION_ID}" \
    '{"semantic_type":"type/Latitude"}' >/dev/null
  local lat_type
  lat_type="$(metabase_api GET "/api/field/${FIELD_VEICULO_LAT}" "${SESSION_ID}" \
    | jq -r '.semantic_type // empty')"
  if [ "${lat_type}" != "type/Latitude" ]; then
    echo "ERROR: semantic type not persisted for veiculo_lat (got: '${lat_type}')." >&2
    exit 1
  fi
  echo "    - veiculo_lat: semantic_type=${lat_type}"

  metabase_api PUT "/api/field/${FIELD_VEICULO_LONG}" "${SESSION_ID}" \
    '{"semantic_type":"type/Longitude"}' >/dev/null
  local long_type
  long_type="$(metabase_api GET "/api/field/${FIELD_VEICULO_LONG}" "${SESSION_ID}" \
    | jq -r '.semantic_type // empty')"
  if [ "${long_type}" != "type/Longitude" ]; then
    echo "ERROR: semantic type not persisted for veiculo_long (got: '${long_type}')." >&2
    exit 1
  fi
  echo "    - veiculo_long: semantic_type=${long_type}"

  echo "[OK] Semantic types set for veiculo_lat and veiculo_long"
}

print_auto_refresh_reminder() {
  echo "[INFO] Auto-refresh cannot be set via API — set it once manually:"
  echo "       open the '${DASHBOARD_NAME}' dashboard → clock icon (top-right) → select 1 minute."
}

cd "${PROJECT_ROOT}"
load_env_file

METABASE_BASE_URL="${METABASE_BASE_URL:-http://localhost:${METABASE_PORT}}"

readonly DASHBOARD_NAME="SPTrans Insights"
readonly COLLECTION_NAME="SPTrans Insights Pro"

# Template-tag UUIDs — stable constants; never regenerate (invalidates saved filter state)
readonly UUID_P4_DATE_RANGE="2ece6096-7fbf-4d55-b181-79b35c60abec"
readonly UUID_P4_ROUTE="3dda24be-1edc-4411-a372-ca065099b79e"
readonly UUID_P4_DIRECTION="c893f050-1c3f-4a74-ba10-14fdc6cf6eb4"
readonly UUID_P4_IS_WEEKEND="98e5826e-4774-4448-bc6e-7a884d841bca"
readonly UUID_P4_IS_CIRCULAR="783e0aab-3c79-4ae7-b41d-8c25ef71f8fe"
readonly UUID_P5_DATE_RANGE="0de8219f-7ef1-429c-a86d-0289d4dd1577"
readonly UUID_P5_ROUTE="54b675dd-5fe6-4164-bae5-ef0a20a675d3"
readonly UUID_P5_DIRECTION="95f9f25c-8eb0-4da4-8bf7-890ac6f552f5"
readonly UUID_P5_IS_WEEKEND="524d3782-5d02-4ebd-a15d-cf1a3ff248aa"
readonly UUID_P5_IS_CIRCULAR="bfc0c4f6-dc02-482b-b137-c2fa7d0f89e5"
readonly UUID_P6_DATE_RANGE="b4485740-c511-471d-a2b6-cf8dd7a0f23f"
readonly UUID_P6_DIRECTION="51d68733-d075-416d-ab96-8beeabbfff94"
readonly UUID_P6_IS_WEEKEND="d950adb5-7844-48e6-9e75-26ae49162c3a"
readonly UUID_P6_ROUTE="bc9c821d-90e7-4089-a1da-b30526362cb6"
readonly UUID_P6_IS_CIRCULAR="8cd0ab48-a58c-48e4-b823-add9c07a0c78"
readonly UUID_P6_MIN_TRIPS="d9c5d5c3-3aee-472d-ab82-9a328d26d993"
readonly UUID_P7_ROUTE="f1c8e9c0-88f2-477d-ad08-4a5d5f3405f6"
readonly UUID_P8_DATE_RANGE="3785a4cc-8bbe-42b6-88bb-0c301fa9d3ac"
readonly UUID_P8_ROUTE="b60a0d67-83f7-4d85-a970-7275a291c592"
readonly UUID_P8_IS_CIRCULAR="d2f4096c-a4a7-4222-a02e-e02190cae361"
readonly UUID_P8_MIN_TRIPS="1e5b3360-2b03-4fb7-9feb-44cf286c933f"
readonly UUID_P9_DATE_RANGE="b7956da6-89d2-4e73-9834-7a2ef06183d4"
readonly UUID_P9_ROUTE="09135d46-c216-4c8c-8bfb-34056f55ee34"
readonly UUID_P9_IS_CIRCULAR="61164632-2098-4774-a715-134e9d2c68bd"
readonly UUID_P10_ROUTE="42297453-04d8-4c27-8741-9b8c9f43fb0b"
readonly UUID_P10_DATE_RANGE="fa145b5e-89a9-4b76-b74b-05e838cde72f"
readonly UUID_P10_IS_CIRCULAR="f006b489-afda-429a-b193-95aacacbfe8c"
readonly UUID_P11A_LINHA_LT="fecd97fc-d225-414f-8e57-0ed3c70bb6be"
readonly UUID_P11A_LINHA_SENTIDO="d209ef46-a7d0-437e-bcae-dfb0e146b72b"
readonly UUID_P11B_LINHA_LT="c200f0e4-444e-4e6d-bef7-7f0d42afa3b9"
readonly UUID_P11B_LINHA_SENTIDO="00f13c76-465a-4d34-8890-1d94c0b613a4"

# Dashboard parameter UUIDs — stable constants; never regenerate (invalidates saved filter state)
readonly UUID_PARAM_DATE_RANGE="8f57f82c-0dd5-4dfa-b205-11246ee95cc8"
readonly UUID_PARAM_ROUTE="2d8ffbc3-aec2-44ea-9371-cf14f9511da3"
readonly UUID_PARAM_DIRECTION="87249ffb-cb91-4848-b29b-bae5974aaaa0"
readonly UUID_PARAM_IS_WEEKEND="6c9021c6-f943-4b4a-9885-b3253d4c45a7"
readonly UUID_PARAM_IS_CIRCULAR="45474b4f-ccba-4abb-a8d8-02ba2656137f"
readonly UUID_PARAM_LINHA_LT="9018a7c8-0eaa-4c72-b48c-a849539cd64e"
readonly UUID_PARAM_LINHA_SENTIDO="a1573872-8ae7-4424-a383-37e1ed599986"

require_env "METABASE_ADMIN_EMAIL"
require_env "METABASE_ADMIN_PASSWORD"

require_command "curl"
require_command "jq"

authenticate
provision_collection
cleanup_orphaned_cards
resolve_field_ids
create_cards
create_dashboard
set_field_metadata
print_auto_refresh_reminder
