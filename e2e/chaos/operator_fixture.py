"""Ephemeral Kubernetes manifests for the chaos operator fixture.

Provides resources() for the 3-voter StatefulSet, S3 test gateway, and operator,
and learner_resources() for standalone learner pods with immutable credentials.
"""

from __future__ import annotations

import json

S3_IMAGE = (
    "ghcr.io/versity/versitygw@sha256:30292fc2eeacc67a36993b01f7a7a5e3361a19cced0e80c1d71cfa2a4b0a2499"
)
AWS_CLI_IMAGE = (
    "amazon/aws-cli@sha256:83f8ffe939569070c5b66d22231862ab78718766d9d8e4c44ca84dd0be5569a5"
)

_MINIO_ACCESS = "chaos-minio"
_MINIO_SECRET = "chaos-minio-secret"

# Peer identity tokens stay in the Secret; the replicated membership publishes
# only the public key each token derives for cluster "chaos-source". The values
# below are the Go KDF output (rhiza.PeerPublicKey) for those exact tokens.
_PEER_TOKENS = {f"rhiza-{i}": f"chaos-peer-{i}" for i in range(3)}

_PEER_PUBLIC_KEYS = {
    "rhiza-0": "w/9aNKUMViLY3om/rhV1cdEi0Ze/RRHoWVVASKtxEZ0=",
    "rhiza-1": "20cqK3U/vCHM97okdruQevaHJQTLwFQwt3JSBRyU4I4=",
    "rhiza-2": "duahgYsvhEXer27GZRWtAaQKXxTUxgPjP/Zo4ZSy7ZQ=",
    "learner-a": "KG0UGao23Awl505BjApO1UQuPHgh3N7530FNo7VoXyY=",
    "learner-b": "81VijYuEB04X3clLPkoSGs+gun03Drf/PfsPjA2Ed9E=",
    "learner-c": "jGmOr56lfg2S+6TiI44aUzEoJ/eHSrOpKdggldq6+XY=",
}

_MEMBERS = [
    {
        "node_id": f"rhiza-{i}",
        "url": f"http://rhiza-{i}.rhiza-headless:8080",
        "peer_url": f"quic://rhiza-{i}.rhiza-headless:9090",
        "public_key": _PEER_PUBLIC_KEYS[f"rhiza-{i}"],
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
            "RHIZA_PEER_TOKENS": json.dumps(
                _PEER_TOKENS, separators=(",", ":")
            ),
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
                            "image": S3_IMAGE,
                            "args": ["--port", ":9000", "--health", "/_/health", "posix", "/data"],
                            "env": [
                                {
                                    "name": "ROOT_ACCESS_KEY",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                            "name": "rhiza-minio",
                                            "key": "access-key",
                                        }
                                    },
                                },
                                {
                                    "name": "ROOT_SECRET_KEY",
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
                                    "path": "/_/health",
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
                            "name": "aws-cli",
                            "image": AWS_CLI_IMAGE,
                            "command": ["/bin/sh", "-c"],
                            "args": [
                                "i=0; while [ \"$i\" -lt 120 ]; do "
                                "aws --endpoint-url http://rhiza-minio:9000 s3api head-bucket --bucket rhiza && exit 0; "
                                "aws --endpoint-url http://rhiza-minio:9000 s3 mb s3://rhiza && exit 0; "
                                "i=$((i+1)); sleep 1; done; exit 1"
                            ],
                            "env": [
                                {
                                    "name": "AWS_ACCESS_KEY_ID",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                            "name": "rhiza-minio",
                                            "key": "access-key",
                                        }
                                    },
                                },
                                {
                                    "name": "AWS_SECRET_ACCESS_KEY",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                            "name": "rhiza-minio",
                                            "key": "secret-key",
                                        }
                                    },
                                },
                                {"name": "AWS_DEFAULT_REGION", "value": "us-east-1"},
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
        {
            "node_id": name,
            "peer_url": peer_url,
            "public_key": _PEER_PUBLIC_KEYS[name],
        },
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
            "RHIZA_PEER_TOKEN": token,
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
                            "valueFrom": {
                                "secretKeyRef": {
                                    "name": f"{name}-credentials",
                                    "key": "member",
                                }
                            },
                        },
                        {
                            "name": "RHIZA_PEER_TOKEN",
                            "valueFrom": {
                                "secretKeyRef": {
                                    "name": f"{name}-credentials",
                                    "key": "RHIZA_PEER_TOKEN",
                                }
                            },
                        },
                        # The voter token map is shared through envFrom; a
                        # learner uses its single RHIZA_PEER_TOKEN instead.
                        {"name": "RHIZA_PEER_TOKENS", "value": ""},
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
