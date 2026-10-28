#!/usr/bin/env bash
# The single project entry point. Extend all() whenever another milestone is integrated.
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

CLUSTER="streamscale"
LOCAL_PROJECT="streamscale-local"
KUBECTL="$ROOT/.tools/bin/kubectl"
KUBECONFIG_FILE="$ROOT/artifacts/kubeconfig"

usage() {
    cat <<'HELP'
StreamScale launcher — integrated milestones M0–M16

Usage: ./streamscale.sh [command] [options]

  all [--no-ui]  Setup, checks, ML tests and offline evaluation, scaling, the M16
                measured matrix, plots/report, console acceptance, then the console
                with dashboards. This is the default.
                --no-ui finishes without opening the console or port forwards.
  local         Setup and run a fresh local Kafka/PostgreSQL fixture demo.
  check         Start local dependencies and run unit, lint and live integration checks.
  cluster       Setup and deploy/verify a fresh kind cluster execution.
  scaling       Exercise fixed/CPU-HPA/lag-KEDA/ML-reactive/ML-predictive independently;
                retain replica and controller-decision evidence.
  ml            ML forecaster tests and simulated offline evaluation (no cluster).
  ml-train      Refit the simulator to M7, retune and retrain the packaged forecaster.
  experiments-v3 [--fresh]
                M16: pilot and 75 measured runs (5 modes × 5 scenarios × 3); resume or
                reuse retained evidence. Results go to docs/results/m16.
  analysis-v3   Audit the M16 campaign and regenerate its plots, report and PDF.
  experiments [--fresh]  M7: pilot and 27 measured runs (protocol v2); not part of all.
  analysis      Audit the M7 campaign and regenerate docs/results/current.
  verify        Verify the current fresh cluster execution without republishing.
  inspect-local Reinspect the latest local demo made by this launcher.
  ui [--grafana-port PORT] [--prometheus-port PORT]
                Open dashboard forwards; Ctrl+C closes this command's forwards.
                all and console accept these port options too. Use 0 for any port.
  console [--port PORT]
                Build/deploy the interactive console and serve it on loopback with
                Grafana/Prometheus links. Prints a login URL; Ctrl+C closes it.
                Pages: Overview, Sale, Scaling, Failure lab, Sessions and Guide.
  console-check Console unit, web, live-cluster and browser acceptance (M9–M15),
                including real sales, scaling modes, faults, replay and the full journey.
  status        Show this project's containers and, when running, Kubernetes resources.
  stop          Stop local services and kind node containers; keep all stored data.
  stop-local    Stop only the local Compose services; keep their data.
  stop-cluster  Stop only the kind node containers (all cluster pods); keep cluster data.
  resume        Start retained local services and kind nodes; do not create a new run.
  help          Show this help.

Prerequisites: Bash, uv, make, Node.js 20.19+ with npm, and a running Docker Engine
with Compose. Browser checks use the installed Google Chrome.
Preferred ports: console 18080, Grafana 13000, Prometheus 19090.
Busy default ports automatically fall back; open the URLs printed by ui.
Grafana login: artifacts/k8s/credentials.json

Close the dashboard terminal with Ctrl+C before running stop in another terminal.
all/local/cluster create fresh executions and preserve earlier runs.
experiments uses a frozen pilot-calibrated protocol and may take over an hour;
experiments-v3 takes about 3.5 hours. M7's committed results are never rewritten by all.
--fresh creates a new measured campaign while retaining earlier results.
After resume, use all or cluster for a fresh counter-based acceptance check.
Cluster verification waits up to 60 seconds for metrics to reflect SQL reconciliation.
HELP
}

require_command() {
    if ! command -v "$1" >/dev/null 2>&1; then
        printf 'Missing command: %s. See docs/run-project.md.\n' "$1" >&2
        exit 1
    fi
}

docker_ready() {
    require_command docker
    if ! docker info >/dev/null 2>&1; then
        printf 'Start Docker Engine/Desktop and make sure your user can access Docker.\n' >&2
        exit 1
    fi
}

setup() {
    require_command uv
    require_command make
    docker_ready
    docker compose version >/dev/null
    if [[ ! -f .env ]]; then
        (umask 077; cp .env.example .env)
        printf 'Created .env from the local example.\n'
    fi
    uv sync --frozen
}

containers() {
    docker ps -a --filter "label=$1" --format '{{.Names}}'
}

container_action() {
    local operation="$1" selector="$2" listing name
    local names=()
    listing="$(containers "$selector")"
    while IFS= read -r name; do
        if [[ -n "$name" ]]; then
            names+=("$name")
        fi
    done <<< "$listing"
    if [[ ${#names[@]} -gt 0 ]]; then
        if [[ "$operation" == stop ]]; then
            docker stop --timeout 45 "${names[@]}"
        else
            docker start "${names[@]}"
        fi
    else
        printf 'No containers found for %s.\n' "$selector"
    fi
}

start_cluster_nodes() {
    container_action start "io.x-k8s.kind.cluster=$CLUSTER"
    if [[ -x "$KUBECTL" && -f "$KUBECONFIG_FILE" &&
          -n "$(containers "io.x-k8s.kind.cluster=$CLUSTER")" ]]; then
        wait_for_api
    fi
}

local_start() {
    make up
    make bootstrap
}

local_demo() {
    local base directory
    mkdir -p artifacts/launcher
    base="$(mktemp -d "$ROOT/artifacts/launcher/demo-XXXXXXXX")"
    directory="$base/local-run"
    make demo "RUN_DIR=$directory"
    printf '%s\n' "$directory" > artifacts/launcher/last-local-run.path
    printf '\nLocal run artifacts: %s\n' "$directory"
}

checks() {
    make test
    make check
    make integration
}

cluster_demo() {
    # kind lists stopped nodes too; start retained nodes before Helm talks to the API.
    start_cluster_nodes
    make k8s-up
    make k8s-verify
}

kube() {
    "$KUBECTL" --kubeconfig "$KUBECONFIG_FILE" --context "kind-$CLUSTER" \
        --namespace streamscale "$@"
}

wait_for_api() {
    local deadline=$((SECONDS + 180))
    printf 'Waiting for the retained cluster API...\n'
    until kube --request-timeout=5s get --raw=/readyz >/dev/null 2>&1; do
        if [[ $SECONDS -ge $deadline ]]; then
            printf 'Cluster API did not become ready. Inspect with ./streamscale.sh status.\n' >&2
            return 1
        fi
        sleep 2
    done
}

status() {
    docker_ready
    printf '\nLocal services:\n'
    docker ps -a --filter "label=com.docker.compose.project=$LOCAL_PROJECT" \
        --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
    printf '\nCluster nodes:\n'
    docker ps -a --filter "label=io.x-k8s.kind.cluster=$CLUSTER" \
        --format 'table {{.Names}}\t{{.Status}}'
    local running
    running="$(docker ps --filter "label=io.x-k8s.kind.cluster=$CLUSTER" --format '{{.Names}}')"
    if [[ -n "$running" && -x "$KUBECTL" && -f "$KUBECONFIG_FILE" ]]; then
        kube get pods,jobs,svc,pvc
    else
        printf 'Cluster is absent or stopped. Use cluster for a new run, or resume for retained data.\n'
    fi
}

ui() {
    require_command uv
    # Replace the launcher so Ctrl+C goes to the foreground dashboard command.
    exec uv run --frozen python deploy/ui.py "${ui_options[@]}"
}

console_prerequisites() {
    require_command node
    require_command npm
}

console_serve() {
    # Replace the launcher so Ctrl+C reaches the console server and closes its forwards.
    exec uv run --project console --frozen python -m streamscale_console serve --dashboards \
        "${console_options[@]}" "${ui_options[@]}"
}

console() {
    setup
    console_prerequisites
    start_cluster_nodes
    make console-web console-deploy
    console_serve
}

resume() {
    setup
    local_start
    local nodes
    nodes="$(containers "io.x-k8s.kind.cluster=$CLUSTER")"
    if [[ -n "$nodes" ]]; then
        start_cluster_nodes
        if [[ ! -x "$KUBECTL" || ! -f "$KUBECONFIG_FILE" ]]; then
            printf 'Cluster tools/config are missing. Run ./streamscale.sh cluster.\n' >&2
            exit 1
        fi
        kube wait --for=condition=Ready nodes --all --timeout=180s
        kube rollout status statefulset/streamscale-kafka --timeout=180s
        kube rollout status statefulset/streamscale-postgres --timeout=180s
        kube rollout status deployment/streamscale-consumer --timeout=180s
        kube rollout status deployment/streamscale-exporter --timeout=180s
        kube rollout status deployment/streamscale-prometheus --timeout=180s
        kube rollout status deployment/streamscale-grafana --timeout=180s
        if kube get deployment streamscale-console-agent >/dev/null 2>&1; then
            kube rollout status deployment/streamscale-console-agent --timeout=180s
            kube rollout status deployment/streamscale-console-consumer --timeout=180s
        fi
    fi
    printf 'Retained services resumed. No new workload was published.\n'
    status
}

command="${1:-all}"
if [[ $# -gt 0 ]]; then shift; fi
no_ui=false
fresh=false
ui_options=()
console_options=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --no-ui)
            if [[ "$command" != all ]]; then
                printf '%s is only supported with all.\n' "$1" >&2
                exit 2
            fi
            no_ui=true
            shift
            ;;
        --fresh)
            if [[ "$command" != experiments && "$command" != experiments-v3 ]]; then
                printf '%s is only supported with experiments or experiments-v3.\n' "$1" >&2
                exit 2
            fi
            fresh=true
            shift
            ;;
        --port)
            if [[ "$command" != console ]]; then
                printf '%s is only supported with console.\n' "$1" >&2
                exit 2
            fi
            if [[ $# -lt 2 || ! "$2" =~ ^[0-9]{1,5}$ ]] || ((10#$2 > 65535)); then
                printf '%s requires a port from 0 to 65535 (0 chooses an available port).\n' "$1" >&2
                exit 2
            fi
            console_options+=("$1" "$2")
            shift 2
            ;;
        --grafana-port|--prometheus-port)
            if [[ "$command" != ui && "$command" != all && "$command" != console ]]; then
                printf '%s is only supported with ui, all or console.\n' "$1" >&2
                exit 2
            fi
            if [[ $# -lt 2 || ! "$2" =~ ^[0-9]{1,5}$ ]] || ((10#$2 > 65535)); then
                printf '%s requires a port from 0 to 65535 (0 chooses an available port).\n' "$1" >&2
                exit 2
            fi
            ui_options+=("$1" "$2")
            shift 2
            ;;
        *)
            printf 'Unexpected argument: %s\n' "$1" >&2
            usage >&2
            exit 2
            ;;
    esac
done
if [[ "$no_ui" == true && ${#ui_options[@]} -gt 0 ]]; then
    printf 'Dashboard port options cannot be combined with --no-ui.\n' >&2
    exit 2
fi

case "$command" in
    all)
        setup
        console_prerequisites
        printf '\n[M0–M4] Starting the local pipeline, smoke test and fresh fixture demo.\n'
        local_start
        make smoke
        local_demo
        checks
        printf '\n[M16] Testing the ML forecaster and evaluating every policy in simulation.\n'
        make ml-test
        make ml-eval
        printf '\n[M5] Deploying a fresh cluster execution and checking SQL/metrics/Grafana.\n'
        cluster_demo
        printf '\n[M6/M16] Exercising the five isolated scaling modes and replica bounds.\n'
        make scaling
        # M7/M8 (protocol v2) stay available as experiments/analysis; their committed
        # results in docs/results/current are never rewritten here.
        printf '\n[M16] Running or verifying the pilot and frozen 75-run experiment matrix.\n'
        make experiments-v3
        printf '\n[M16] Auditing measurements and generating the M16 plots, report and PDF.\n'
        make analysis-v3
        printf '\n[M9–M15] Verifying the interactive console against the live cluster and browser.\n'
        make console-check
        status
        printf '\nM0–M16 passed. Reports: docs/results/m16/report.md and docs/results/current/report.md; console evidence: artifacts/console/.\n'
        if [[ "$no_ui" == false ]]; then console_serve; fi
        ;;
    local) setup; local_start; make smoke; local_demo ;;
    check) setup; local_start; checks ;;
    cluster) setup; cluster_demo ;;
    scaling) setup; start_cluster_nodes; make scaling ;;
    experiments)
        setup
        start_cluster_nodes
        if [[ "$fresh" == true ]]; then make experiments-fresh; else make experiments; fi
        ;;
    experiments-v3)
        setup
        start_cluster_nodes
        if [[ "$fresh" == true ]]; then make experiments-v3-fresh; else make experiments-v3; fi
        ;;
    analysis) require_command uv; require_command make; make analysis ;;
    analysis-v3) require_command uv; require_command make; make analysis-v3 ;;
    ml) setup; make ml-test ml-eval ;;
    ml-train) setup; make ml-fit ml-train ml-eval ;;
    verify) require_command make; make k8s-verify ;;
    inspect-local)
        require_command uv
        if [[ ! -f artifacts/launcher/last-local-run.path ]]; then
            printf 'No launcher demo found. Run ./streamscale.sh local first.\n' >&2
            exit 1
        fi
        IFS= read -r directory < artifacts/launcher/last-local-run.path
        uv run --frozen --env-file .env streamscale inspect --run-dir "$directory"
        ;;
    ui) ui ;;
    console) console ;;
    console-check) setup; console_prerequisites; start_cluster_nodes; make console-check ;;
    status) status ;;
    stop|stop-local|stop-cluster)
        docker_ready
        if [[ "$command" != stop-cluster ]]; then
            container_action stop "com.docker.compose.project=$LOCAL_PROJECT"
        fi
        if [[ "$command" != stop-local ]]; then
            container_action stop "io.x-k8s.kind.cluster=$CLUSTER"
        fi
        printf 'Stopped selected StreamScale services. Data is retained; use resume to restart.\n'
        ;;
    resume) resume ;;
    help|--help|-h) usage ;;
    *) printf 'Unknown command: %s\n' "$command" >&2; usage >&2; exit 2 ;;
esac
