#!/usr/bin/env python3
"""
Validates rendered Kubernetes manifests from Helm templates.
Verifies YAML syntax, Kubernetes object schema integrity, security contexts,
resource limits, and health probes without requiring connection to an external cluster.
"""

import subprocess
import sys
import yaml
from pathlib import Path

def main():
    repo_root = Path(__file__).resolve().parent.parent
    chart_path = repo_root / "deploy" / "helm" / "streamforge"
    helm_bin = repo_root / ".tools" / "helm.exe"
    
    if not helm_bin.exists():
        helm_bin = "helm"

    print(f"[*] Rendering Helm templates for chart: {chart_path}")
    proc = subprocess.run(
        [str(helm_bin), "template", "streamforge", str(chart_path)],
        capture_output=True,
        text=True,
        check=False
    )
    if proc.returncode != 0:
        print(f"[!] Helm template failed:\n{proc.stderr}", file=sys.stderr)
        sys.exit(1)

    manifest_docs = list(yaml.safe_load_all(proc.stdout))
    print(f"[*] Successfully parsed {len(manifest_docs)} Kubernetes manifests from template.")

    expected_components = {"core", "analytics", "alerts", "dashboard", "postgres", "kafka"}
    found_components = set()
    errors = []

    for i, doc in enumerate(manifest_docs):
        if not doc:
            continue
        api_version = doc.get("apiVersion")
        kind = doc.get("kind")
        name = doc.get("metadata", {}).get("name")
        component = doc.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/component")

        if not api_version or not kind or not name:
            errors.append(f"Doc #{i} missing basic metadata: {doc}")
            continue

        if component:
            found_components.add(component)

        # Validate SecurityContexts on Deployments and StatefulSets
        if kind in ("Deployment", "StatefulSet"):
            pod_spec = doc.get("spec", {}).get("template", {}).get("spec", {})
            pod_sec = pod_spec.get("securityContext", {})
            containers = pod_spec.get("containers", [])

            if not pod_sec.get("runAsNonRoot"):
                errors.append(f"{kind}/{name}: missing pod-level runAsNonRoot=true")

            if not pod_spec.get("serviceAccountName"):
                errors.append(f"{kind}/{name}: missing serviceAccountName")

            for c in containers:
                c_name = c.get("name")
                c_sec = c.get("securityContext", {})
                resources = c.get("resources", {})

                # Container security hardening
                if c_sec.get("allowPrivilegeEscalation") is not False:
                    errors.append(f"{kind}/{name} container {c_name}: allowPrivilegeEscalation must be False")
                
                caps = c_sec.get("capabilities", {}).get("drop", [])
                if "ALL" not in caps:
                    errors.append(f"{kind}/{name} container {c_name}: capabilities must drop ALL")

                # Application containers must have read-only root filesystems
                if c_name in ("core", "analytics", "alerts", "dashboard"):
                    if not c_sec.get("readOnlyRootFilesystem"):
                        errors.append(f"{kind}/{name} container {c_name}: readOnlyRootFilesystem must be True")

                # Resource limits
                if not resources.get("limits") or not resources.get("requests"):
                    errors.append(f"{kind}/{name} container {c_name}: missing resource limits or requests")

                # Probes on servers (core, alerts, dashboard)
                if c_name in ("core", "alerts", "dashboard"):
                    if not c.get("livenessProbe"):
                        errors.append(f"{kind}/{name} container {c_name}: missing livenessProbe")
                    if not c.get("readinessProbe"):
                        errors.append(f"{kind}/{name} container {c_name}: missing readinessProbe")

    missing = expected_components - found_components
    if missing:
        errors.append(f"Missing expected components in rendered manifests: {missing}")

    if errors:
        print("[!] Validation failed with errors:", file=sys.stderr)
        for err in errors:
            print(f"  - {err}", file=sys.stderr)
        sys.exit(1)

    print("[+] All Kubernetes manifests passed security, resource, probe, and schema checks!")

if __name__ == "__main__":
    main()
