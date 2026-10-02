# Week 3 Console Service + Operator Controls

Date: March 26, 2026  
Status: Delivered

## Objective

Close M1 operator-control gaps by surfacing local service lifecycle actions and first-run operator workflows directly in the desktop console.

## Delivered Changes

1. Added Tauri command wiring for:
   - `config_console_service_action`
   - `operator_run_action`
2. Added console API/store/types support for:
   - service lifecycle actions: `start`, `stop`, `restart`, `status`
   - operator actions: `bootstrap_local`, `generate_admin_token`, `verify_first_run`, `export_support_bundle`
3. Added UI panels in Config Console:
   - `Service Lifecycle` panel with per-service controls and action history
   - `Operator Workflow` panel with guided first-run flow and per-action output history
4. Added backend unit tests for service/operator action normalization and command-target mapping.

## Operator Behavior

1. Operators can run broker/runtime lifecycle actions without leaving the console.
2. Operators can execute common first-run actions from a guided flow in recommended order.
3. Last-run status, timestamp, exit code, and captured command output are visible for each action.

## Follow-Up (Week 4+)

1. Add explicit cancellation/interrupt control for long-running operator actions.
2. Add role-based guardrails for destructive lifecycle actions in shared desktop environments.
