"""Ephemeral Kubernetes manifests for the chaos operator fixture.

Provides resources() for the 3-voter StatefulSet, MinIO, and operator,
and learner_resources() for standalone learner pods with immutable credentials.
"""

from __future__ import annotations

import json

# Pinned images from existing repo manifests.
MINIO_IMAGE = (
    "minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"
)
MC_IMAGE = (
    "minio/mc@sha256:a7fe349ef4bd8521fb8497f55c6042871b2ae640607cf99d9bede5e9bdf11727"
)

_MINIO_ACCESS = "chaos-minio"
_MINIO_SECRET = "chaos-minio-secret"

_MEMBERS = [
    {
        "node_id": f"rhiza-{i}",
        "url": f"http://rhiza-{i}.rhiza-headless:8080",
        "peer_url": f"quic://rhiza-{i}.rhiza-headless:9090",
        "token": f"chaos-peer-{i}",
    }
    for i in range(3)
]


def resources(namespace: str, db_image: str, operator_image: str) -> list[dict]:
    """Return Kubernetes manifest dicts for the chaos operator fixture.

    Excludes CRD, RBAC, and Namespace resources -- the parent applies those
    from repository manifests with namespace rewriting.
    """

    config_map = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "rhiza-config", "namespace": namespace},
        "data": {
            "RHIZA_CLUSTER_ID": "chaos-source",
            "RHIZA_BIND_ADDR": "0.0.0.0:8080",
            "RHIZA_PEER_ADDR": "0.0.0.0:9090",
            "RHIZA_DATA_DIR": "/data",
            "RHIZA_ENABLE_RECONFIGURATION": "true",
            "RHIZA_CHECKPOINT_INTERVAL": "0",
            "RHIZA_OBJSTORE_SYNC_INTERVAL": "1s",
            "RHIZA_OBJSTORE_GC_INTERVAL": "0",
            "RHIZA_MAX_WAL_BYTES": "0",
            "RHIZA_OBJSTORE_PROVIDER": "s3",
            "RHIZA_OBJSTORE_PREFIX": "rhiza",
            "RHIZA_OBJSTORE_DURABILITY": "before-ack",
            "RHIZA_CLUSTER_MEMBERS": json.dumps(
                _MEMBERS, separators=(",", ":")
            ),
        },
    }

    peer_credentials = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": "rhiza-peer-credentials",
            "namespace": namespace,
        },
        "type": "Opaque",
        "stringData": {
            "RHIZA_ADMIN_TOKEN": "chaos-admin",
        },
    }

    object_store = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": "rhiza-object-store",
            "namespace": namespace,
        },
        "type": "Opaque",
        "stringData": {
            "RHIZA_OBJSTORE_ENDPOINT": "rhiza-minio:9000",
            "RHIZA_OBJSTORE_BUCKET": "rhiza",
            "RHIZA_OBJSTORE_ACCESS_KEY": _MINIO_ACCESS,
            "RHIZA_OBJSTORE_SECRET_KEY": _MINIO_SECRET,
            "RHIZA_OBJSTORE_INSECURE": "true",
        },
    }

    headless_service = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": "rhiza-headless",
            "namespace": namespace,
            "labels": {"app.kubernetes.io/name": "rhiza"},
        },
        "spec": {
            "type": "ClusterIP",
            "clusterIP": "None",
            "publishNotReadyAddresses": True,
            "ports": [
                {
                    "name": "http",
                    "port": 8080,
                    "targetPort": "http",
                    "protocol": "TCP",
                },
                {
                    "name": "peer-quic",
                    "port": 9090,
                    "targetPort": "peer-quic",
                    "protocol": "UDP",
                },
            ],
            "selector": {"app.kubernetes.io/name": "rhiza"},
        },
    }

    stateful_set = {
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {
            "name": "rhiza",
            "namespace": namespace,
            "labels": {"app.kubernetes.io/name": "rhiza"},
        },
        "spec": {
            "serviceName": "rhiza-headless",
            "podManagementPolicy": "Parallel",
            "updateStrategy": {"type": "OnDelete", "rollingUpdate": None},
            "replicas": 3,
            "selector": {
                "matchLabels": {"app.kubernetes.io/name": "rhiza"}
            },
            "template": {
                "metadata": {
                    "labels": {"app.kubernetes.io/name": "rhiza"}
                },
                "spec": {
                    "securityContext": {
                        "fsGroup": 65532,
                        "runAsNonRoot": True,
                    },
                    "containers": [
                        {
                            "name": "rhiza",
                            "image": db_image,
                            "ports": [
                                {
                                    "name": "http",
                                    "containerPort": 8080,
                                },
                                {
                                    "name": "peer-quic",
                                    "containerPort": 9090,
                                    "protocol": "UDP",
                                },
                            ],
                            "envFrom": [
                                {"configMapRef": {"name": "rhiza-config"}},
                                {
                                    "secretRef": {
                                        "name": "rhiza-peer-credentials"
                                    }
                                },
                                {
                                    "secretRef": {
                                        "name": "rhiza-object-store"
                                    }
                                },
                            ],
                            "env": [
                                {
                                    "name": "RHIZA_NODE_ID",
                                    "valueFrom": {
                                        "fieldRef": {
                                            "fieldPath": "metadata.name"
                                        }
                                    },
                                },
                            ],
                            "volumeMounts": [
                                {"name": "data", "mountPath": "/data"}
                            ],
                            "startupProbe": {
                                "httpGet": {
                                    "path": "/healthz",
                                    "port": "http",
                                },
                                "periodSeconds": 5,
                                "failureThreshold": 60,
                            },
                            "livenessProbe": {
                                "httpGet": {
                                    "path": "/healthz",
                                    "port": "http",
                                },
                                "initialDelaySeconds": 10,
                                "periodSeconds": 10,
                            },
                            "readinessProbe": {
                                "httpGet": {
                                    "path": "/ready",
                                    "port": "http",
                                },
                                "initialDelaySeconds": 5,
                                "periodSeconds": 5,
                            },
                            "resources": {
                                "requests": {
                                    "cpu": "500m",
                                    "memory": "512Mi",
                                },
                                "limits": {"memory": "2Gi"},
                            },
                        }
                    ],
                    "volumes": [
                        {"name": "data", "emptyDir": {}}
                    ],
                },
            },
        },
    }

    minio_secret = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": "rhiza-minio",
            "namespace": namespace,
        },
        "type": "Opaque",
        "stringData": {
            "access-key": _MINIO_ACCESS,
            "secret-key": _MINIO_SECRET,
        },
    }

    minio_service = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": "rhiza-minio",
            "namespace": namespace,
        },
        "spec": {
            "selector": {"app": "rhiza-minio"},
            "ports": [{"name": "s3", "port": 9000}],
        },
    }

    minio_deployment = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "rhiza-minio",
            "namespace": namespace,
        },
        "spec": {
            "replicas": 1,
            "selector": {
                "matchLabels": {"app": "rhiza-minio"}
            },
            "template": {
                "metadata": {
                    "labels": {"app": "rhiza-minio"}
                },
                "spec": {
                    "containers": [
                        {
                            "name": "minio",
                            "image": MINIO_IMAGE,
                            "command": ["/bin/sh", "-c"],
                            "args": [
                                "exec minio server /data"
                            ],
                            "env": [
                                {
                                    "name": "MINIO_ROOT_USER",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                            "name": "rhiza-minio",
                                            "key": "access-key",
                                        }
                                    },
                                },
                                {
                                    "name": "MINIO_ROOT_PASSWORD",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                            "name": "rhiza-minio",
                                            "key": "secret-key",
                                        }
                                    },
                                },
                            ],
                            "ports": [
                                {"name": "s3", "containerPort": 9000}
                            ],
                            "readinessProbe": {
                                "httpGet": {
                                    "path": "/minio/health/ready",
                                    "port": "s3",
                                }
                            },
                            "volumeMounts": [
                                {"name": "data", "mountPath": "/data"}
                            ],
                        }
                    ],
                    "volumes": [
                        {"name": "data", "emptyDir": {}}
                    ],
                },
            },
        },
    }

    create_bucket_job = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": "rhiza-create-bucket",
            "namespace": namespace,
        },
        "spec": {
            "template": {
                "spec": {
                    "restartPolicy": "OnFailure",
                    "containers": [
                        {
                            "name": "mc",
                            "image": MC_IMAGE,
                            "command": ["/bin/sh", "-c"],
                            "args": [
                                'until mc alias set local http://rhiza-minio:9000 "$ACCESS_KEY" "$SECRET_KEY"; do sleep 1; done; '
                                "mc mb --ignore-existing local/rhiza"
                            ],
                            "env": [
                                {
                                    "name": "ACCESS_KEY",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                            "name": "rhiza-minio",
                                            "key": "access-key",
                                        }
                                    },
                                },
                                {
                                    "name": "SECRET_KEY",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                            "name": "rhiza-minio",
                                            "key": "secret-key",
                                        }
                                    },
                                },
                            ],
                        }
                    ],
                },
            },
        },
    }

    operator_deployment = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "rhiza-operator",
            "namespace": namespace,
            "labels": {"app.kubernetes.io/name": "rhiza-operator"},
        },
        "spec": {
            "replicas": 1,
            "selector": {
                "matchLabels": {
                    "app.kubernetes.io/name": "rhiza-operator"
                }
            },
            "template": {
                "metadata": {
                    "labels": {
                        "app.kubernetes.io/name": "rhiza-operator"
                    }
                },
                "spec": {
                    "serviceAccountName": "rhiza-operator",
                    "securityContext": {
                        "runAsNonRoot": True,
                        "fsGroup": 65532,
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                    "containers": [
                        {
                            "name": "operator",
                            "image": operator_image,
                            "imagePullPolicy": "IfNotPresent",
                            "args": [
                                "--namespace=$(POD_NAMESPACE)",
                                "--poll-interval=2s",
                            ],
                            "env": [
                                {
                                    "name": "POD_NAMESPACE",
                                    "valueFrom": {
                                        "fieldRef": {
                                            "fieldPath": "metadata.namespace"
                                        }
                                    },
                                },
                            ],
                            "envFrom": [
                                {"configMapRef": {"name": "rhiza-config"}},
                                {
                                    "secretRef": {
                                        "name": "rhiza-object-store"
                                    }
                                },
                            ],
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "capabilities": {"drop": ["ALL"]},
                                "readOnlyRootFilesystem": True,
                            },
                            "volumeMounts": [
                                {"name": "tmp", "mountPath": "/tmp"}
                            ],
                            "resources": {
                                "requests": {
                                    "cpu": "100m",
                                    "memory": "128Mi",
                                },
                                "limits": {"memory": "256Mi"},
                            },
                        }
                    ],
                    "volumes": [
                        {"name": "tmp", "emptyDir": {}}
                    ],
                },
            },
        },
    }

    return [
        config_map,
        peer_credentials,
        object_store,
        headless_service,
        stateful_set,
        minio_secret,
        minio_service,
        minio_deployment,
        create_bucket_job,
        operator_deployment,
    ]


def learner_resources(
    namespace: str,
    name: str,
    db_image: str,
    bootstrap_members: list[dict],
) -> list[dict]:
    """Return Service + standalone Pod + immutable Secret for a learner.

    The learner uses the same ConfigMap and object-store Secret as the
    StatefulSet voters.  Its identity comes from an immutable per-pod
    credential Secret and the RHIZA_LEARNER env var.
    """
    token = f"chaos-peer-{name}"
    peer_url = f"quic://{name}:9090"
    learner_json = json.dumps(
        {"node_id": name, "peer_url": peer_url, "token": token},
        separators=(",", ":"),
    )

    credentials_secret = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": f"{name}-credentials",
            "namespace": namespace,
        },
        "type": "Opaque",
        "immutable": True,
        "stringData": {
            "member": learner_json,
        },
    }

    service = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {
            "clusterIP": "None",
            "publishNotReadyAddresses": True,
            "selector": {"app": name},
            "ports": [
                {
                    "name": "http",
                    "port": 8080,
                    "targetPort": "http",
                },
                {
                    "name": "peer-quic",
                    "port": 9090,
                    "targetPort": "peer-quic",
                    "protocol": "UDP",
                },
            ],
        },
    }

    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {"app": name},
        },
        "spec": {
            "securityContext": {
                "fsGroup": 65532,
                "runAsNonRoot": True,
            },
            "containers": [
                {
                    "name": "rhiza",
                    "image": db_image,
                    "ports": [
                        {"name": "http", "containerPort": 8080},
                        {
                            "name": "peer-quic",
                            "containerPort": 9090,
                            "protocol": "UDP",
                        },
                    ],
                    "envFrom": [
                        {"configMapRef": {"name": "rhiza-config"}},
                        {
                            "secretRef": {
                                "name": "rhiza-peer-credentials"
                            }
                        },
                        {
                            "secretRef": {
                                "name": "rhiza-object-store"
                            }
                        },
                    ],
                    "env": [
                        {"name": "RHIZA_NODE_ID", "value": name},
                        {
                            "name": "RHIZA_LEARNER",
                            "value": learner_json,
                        },
                    ],
                    "volumeMounts": [
                        {"name": "data", "mountPath": "/data"}
                    ],
                    "startupProbe": {
                        "httpGet": {
                            "path": "/healthz",
                            "port": "http",
                        },
                        "periodSeconds": 5,
                        "failureThreshold": 60,
                    },
                    "livenessProbe": {
                        "httpGet": {
                            "path": "/healthz",
                            "port": "http",
                        },
                        "initialDelaySeconds": 10,
                        "periodSeconds": 10,
                    },
                    "readinessProbe": {
                        "httpGet": {
                            "path": "/ready",
                            "port": "http",
                        },
                        "initialDelaySeconds": 5,
                        "periodSeconds": 5,
                    },
                }
            ],
            "volumes": [
                {"name": "data", "emptyDir": {}}
            ],
        },
    }

    return [credentials_secret, service, pod]
