COMPOSE = docker compose --env-file .env -f deploy/local/compose.yaml
RUN = uv run --frozen --env-file .env
RUN_DIR ?= artifacts/demo

.PHONY: install up bootstrap smoke demo test integration faults check down logs k8s-tools k8s-up k8s-verify k8s-ui k8s-status k8s-logs k8s-addons scaling experiments experiments-fresh experiment-audit analysis

install:
	uv sync --frozen

up:
	$(COMPOSE) up -d --wait --wait-timeout 180

bootstrap:
	$(RUN) streamscale topics
	$(RUN) streamscale migrate

smoke:
	$(RUN) streamscale smoke

demo:
	$(RUN) streamscale generate --output "$(RUN_DIR)" --seed 42 --orders 2 --profile fixtures
	$(RUN) streamscale publish --run-dir "$(RUN_DIR)" --no-wait
	$(RUN) streamscale consume --run-dir "$(RUN_DIR)"
	$(RUN) streamscale inspect --run-dir "$(RUN_DIR)"

test:
	uv run --frozen pytest

integration:
	$(RUN) pytest -m integration

faults:
	$(RUN) pytest -m integration tests/integration/test_faults.py

check:
	uv run --frozen ruff check .
	uv run --frozen ruff format --check .

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs --tail 80

k8s-tools:
	uv run --frozen python deploy/k8s/install_tools.py

.tools/bin/kind .tools/bin/kubectl .tools/bin/helm: deploy/k8s/install_tools.py
	uv run --frozen python deploy/k8s/install_tools.py

k8s-up: .tools/bin/kind .tools/bin/kubectl .tools/bin/helm
	uv run --frozen python deploy/k8s/manage.py up

k8s-verify:
	uv run --frozen python deploy/k8s/verify.py

k8s-ui:
	./streamscale.sh ui

k8s-status:
	uv run --frozen python deploy/k8s/manage.py status

k8s-logs:
	uv run --frozen python deploy/k8s/manage.py logs

k8s-addons: .tools/bin/kind .tools/bin/kubectl .tools/bin/helm
	uv run --frozen python deploy/k8s/addons.py

scaling: .tools/bin/kind .tools/bin/kubectl .tools/bin/helm
	uv run --frozen python deploy/k8s/scaling.py

experiments: .tools/bin/kind .tools/bin/kubectl .tools/bin/helm
	uv run --frozen python -m experiments.run --resume
	uv run --frozen python -m experiments.evidence

experiments-fresh: .tools/bin/kind .tools/bin/kubectl .tools/bin/helm
	uv run --frozen python -m experiments.run --fresh
	uv run --frozen python -m experiments.evidence

experiment-audit:
	uv run --frozen python -m experiments.evidence

analysis:
	uv run --project experiments/analysis --frozen python -m experiments.analyze

# M16 protocol v3: five modes × five scenarios × three repetitions, beside M7's results.
.PHONY: experiments-v3 experiments-v3-fresh experiment-audit-v3 analysis-v3
experiments-v3: .tools/bin/kind .tools/bin/kubectl .tools/bin/helm
	uv run --frozen python -m experiments.run --protocol v3 --resume
	uv run --frozen python -m experiments.evidence --protocol v3

experiments-v3-fresh: .tools/bin/kind .tools/bin/kubectl .tools/bin/helm
	uv run --frozen python -m experiments.run --protocol v3 --fresh
	uv run --frozen python -m experiments.evidence --protocol v3

experiment-audit-v3:
	uv run --frozen python -m experiments.evidence --protocol v3

analysis-v3:
	uv run --project experiments/analysis --frozen python -m experiments.analyze --protocol v3

# Interactive console (M9+): separately locked Python and Node dependencies.
CONSOLE = uv run --project console --frozen
CONSOLE_ARGS ?=

.PHONY: console-install console-web console-test console-deploy console-live console-check console-serve

console-install:
	uv sync --project console --frozen
	cd console/web && PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1 npm ci

console-web: console-install
	cd console/web && npm run build

console-test: console-install
	$(CONSOLE) pytest console/tests
	cd console/web && npm run typecheck && npm test

console-deploy: console-install .tools/bin/kind .tools/bin/kubectl .tools/bin/helm
	$(CONSOLE) python -m streamscale_console deploy

console-live:
	$(CONSOLE) pytest console/tests/live -m live

console-check: console-install .tools/bin/kind .tools/bin/kubectl .tools/bin/helm
	$(CONSOLE) python -m streamscale_console check

console-serve:
	$(CONSOLE) python -m streamscale_console serve $(CONSOLE_ARGS)

# ML predictive autoscaling (M16): separately locked offline tooling. The cluster image only
# carries the plain-Python controller and the exported forecaster JSON.
ML = uv run --project ml --frozen
REPLAY ?= flash-sale
.PHONY: ml-install ml-test ml-fit ml-sim ml-train ml-eval ml-calibrate autoscale-replay
ml-install:
	uv sync --project ml --frozen
ml-test: ml-install
	$(ML) pytest ml/tests
ml-fit: ml-install
	$(ML) python -m streamscale_ml.calibrate
ml-sim: ml-install
	$(ML) python -m streamscale_ml.compare
ml-train: ml-install
	$(ML) python -m streamscale_ml.tune
ml-eval: ml-install
	$(ML) python -m streamscale_ml.evaluate

# Short cluster runs on train: shapes refine the simulator before retraining.
CALIBRATION_CAMPAIGN = artifacts/experiments/v3/calibration/latest.path
ml-calibrate: ml-install .tools/bin/kind .tools/bin/kubectl .tools/bin/helm
	uv run --frozen python -m experiments.run --protocol v3 --fresh --calibration \
		--modes fixed,lag-keda,ml-reactive --scenarios flash-sale,ramp,repeated-sales \
		--repetitions 1
	$(ML) python -m streamscale_ml.calibrate --campaign "$$(cat $(CALIBRATION_CAMPAIGN))"
	$(ML) python -m streamscale_ml.tune
	$(ML) python -m streamscale_ml.evaluate
autoscale-replay:
	uv run --frozen streamscale autoscale-replay --synthetic "$(REPLAY)"
