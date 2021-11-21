from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client.models.v1_env_var import V1EnvVar
from kubernetes.client.models.v1_resource_attributes import V1ResourceAttributes


def kubernetes_pod_operator_factory(
    env_vars: Optional[List[V1EnvVar]] = None,
    resources: Optional[V1ResourceAttributes] = None,
    labels: Optional[Dict[AnyStr, AnyStr]] = None,
    *keys: List[Any],
    **kwargs: Dict[AnyStr, Any],
) -> KubernetesPodOperator:
    """
    Factory method for KubernetesPodOperator.
    Been written to avoid repeating ourselves in the common patrs.

    """
    if resources is None:
        resources = DEFAULT_POD_RESOURCES
    if env_vars == None:
        env_vars = DEFAULT_ENV_VARS

    # Support backward compatibility
    if isinstance(env_vars, dict):
        env_vars = [V1EnvVar(name=k, value=v) for k, v in env_vars.items()]

    return KubernetesPodOperator(
        *keys,
        env_vars=env_vars,
        resources=resources,
        is_delete_operator_pod=kwargs.pop("is_delete_operator_pod", True),
        termination_grace_period=kwargs.pop("termination_grace_period", 300),
        image_pull_policy="IfNotPresent",
        labels=labels,
        **kwargs,
    )
