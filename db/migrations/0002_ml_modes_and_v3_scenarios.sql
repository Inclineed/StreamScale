-- M16: allow the ML scaling modes and the protocol v3 scenarios. 0001 stays unchanged.
ALTER TABLE experiment_run DROP CONSTRAINT experiment_run_mode_check;
ALTER TABLE experiment_run ADD CONSTRAINT experiment_run_mode_check CHECK (
    mode IN ('fixed', 'cpu-hpa', 'lag-keda', 'ml-reactive', 'ml-predictive')
);
ALTER TABLE experiment_run DROP CONSTRAINT experiment_run_scenario_check;
ALTER TABLE experiment_run ADD CONSTRAINT experiment_run_scenario_check CHECK (
    scenario IN ('steady', 'flash-sale', 'overload-recovery', 'ramp', 'repeated-sales')
);
