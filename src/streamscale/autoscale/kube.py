"""Minimal in-cluster Kubernetes client: read one Deployment and set its scale.

The controller's ServiceAccount Role grants only ``get`` on the Deployment and
``get``/``patch`` on its ``scale`` subresource, scoped by resource name. This client also
refuses replica counts outside the 1–3 contract before sending anything.
"""

import json
import os
import ssl
from pathlib import Path
from urllib.parse import quote
from urllib.request import Request, urlopen

SERVICE_ACCOUNT = Path("/var/run/secrets/kubernetes.io/serviceaccount")
MIN_REPLICAS, MAX_REPLICAS = 1, 3


class DeploymentScaler:
    def __init__(self, deployment: str, namespace: str | None = None, account=SERVICE_ACCOUNT):
        self.account = Path(account)
        self.namespace = namespace or (self.account / "namespace").read_text().strip()
        self.deployment = deployment
        host = os.environ["KUBERNETES_SERVICE_HOST"]
        port = os.environ.get("KUBERNETES_SERVICE_PORT", "443")
        if ":" in host:
            host = f"[{host}]"
        self.base = (
            f"https://{host}:{port}/apis/apps/v1/namespaces/{quote(self.namespace)}"
            f"/deployments/{quote(deployment)}"
        )
        self.context = ssl.create_default_context(cafile=str(self.account / "ca.crt"))

    def _request(self, path="", method="GET", body=None, content="application/json"):
        # Projected tokens rotate; read the current one for every request.
        token = (self.account / "token").read_text().strip()
        request = Request(
            self.base + path,
            method=method,
            data=None if body is None else json.dumps(body).encode(),
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "Content-Type": content,
            },
        )
        with urlopen(request, timeout=5, context=self.context) as response:
            return json.loads(response.read())

    def status(self):
        """(desired spec replicas, ready replicas) of the target Deployment."""
        deployment = self._request()
        spec = deployment.get("spec", {})
        status = deployment.get("status", {})
        return int(spec.get("replicas", 1)), int(status.get("readyReplicas", 0) or 0)

    def scale(self, replicas: int):
        if type(replicas) is not int or not MIN_REPLICAS <= replicas <= MAX_REPLICAS:
            raise ValueError("Consumer replicas must be an integer from 1 to 3")
        return self._request(
            "/scale",
            "PATCH",
            {"spec": {"replicas": replicas}},
            "application/merge-patch+json",
        )
